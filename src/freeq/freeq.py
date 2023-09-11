from base64 import b85encode, b64encode, b85decode
from functools import partial
import json
import os
import time
import typing

from cryptography.fernet import Fernet
from Crypto.Protocol.KDF import scrypt
import flask
from flask import request
from redis import Redis
import requests
import zstandard as zstd


REDIS_BLOCKING_TIMEOUT = int(os.getenv("REDIS_BLOCKING_TIMEOUT", 60))
REDIS_EXPIRY = int(os.getenv("REDIS_EXPIRY", 60 * 60 * 24 * 2))
REDIS_MAX_QLEN = int(os.getenv("REDIS_MAX_QLEN", 2048))
REDIS_PWD = os.getenv("REDIS_PWD", None)


app = flask.Flask(__name__)
r = Redis(password=REDIS_PWD)
dumps = partial(json.dumps, separators=(",", ":"), indent=None)


@app.route("/<string:queue>/<string:key>", methods=["GET"])
def get_event(queue: str, key: str):
    redis_key = f"{queue}-{key}"
    ack = request.args.get("ack", default=True, type=lambda v: v.lower() == "true")
    block = request.args.get("block", default=False, type=lambda v: v.lower() == "true")

    if block:
        data = r.bzpopmin(redis_key, timeout=REDIS_BLOCKING_TIMEOUT)
        if data is not None:
            _, payload, tstamp = data
        else:
            # Timeout - queue is empty, poll again
            return {"success": True, "message": f'Queue "{queue}" is empty'}, 205
        if not ack:
            # Redis has no blocking zrangebyscore, so we need to re-add the event
            r.zadd(redis_key, {payload: tstamp})
    else:
        if ack:
            payload = r.zpopmin(redis_key)
            if payload:
                payload = payload[0][0]
        else:
            payload = r.zrange(redis_key, 0, 0)
            if payload:
                payload = payload[0]
        if not payload:
            return {"success": True, "message": f'Queue "{queue}" is empty'}, 204

    data = json.loads(payload.decode())
    return data, 200


@app.route("/<string:queue>/<string:key>/<string:tstamp>", methods=["POST"])
def ack_event(queue: str, key: str, tstamp: str):
    redis_key = f"{queue}-{key}"

    r.zremrangebyscore(redis_key, tstamp, tstamp)
    return {
        "success": True,
        "message": f'Removed event {tstamp} from queue "{queue}"',
    }, 200


@app.route("/<string:queue>/<string:key>", methods=["DELETE"])
def clear_queue(queue: str, key: str):
    redis_key = f"{queue}-{key}"

    r.delete(redis_key)
    return {"success": True, "message": f'Cleared queue "{queue}"'}, 200


@app.route("/<string:queue>/<string:key>", methods=["POST"])
def post_event(queue: str, key: str):
    redis_key = f"{queue}-{key}"
    data = request.get_json()
    tstamp = data["_tstamp"]
    payload = dumps(data)

    if r.zcard(redis_key) > REDIS_MAX_QLEN:
        return {
            "success": False,
            "message": f'Queue "{queue}" is full. Consume existing events or clear the queue',
        }, 400

    r.zadd(redis_key, {payload: tstamp})
    r.expire(redis_key, REDIS_EXPIRY)
    return {"success": True, "message": f'Added event {tstamp} to queue "{queue}"'}, 201


class Event(dict):
    def __init__(self, _queue, _tstamp, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = _queue
        self.tstamp = _tstamp

    def ack(self):
        self.queue.ack(self.tstamp)


class Queue:
    def __init__(self, name, access_key, secret_key, *, server_addr="qu.0x.no"):
        self.name = name
        self.access_key = access_key
        self.secret_key = secret_key
        self.addr = server_addr
        self.compressor = zstd.ZstdCompressor()
        self.decompressor = zstd.ZstdDecompressor()
        self.fernet_key = b64encode(scrypt(secret_key, b"", 32, N=2**14, r=8, p=1))
        self.fernet = Fernet(self.fernet_key)

    def get(self, ack: bool = True, block: bool = False) -> typing.Union[Event, None]:
        try:
            while True:
                resp = requests.get(
                    f"https://{self.addr}/{self.name}/{self.access_key}",
                    params={"ack": ack, "block": block},
                )
                resp.raise_for_status()
                # Poll until we get an event, or return None if we're not blocking
                if not block:
                    break
                if resp.status_code == 200:
                    break
            if resp.status_code == 204:
                return None

        except requests.exceptions.HTTPError:
            # TODO log
            return None
        data = resp.json()

        event = Event(
            self,
            data["_tstamp"],
            **json.loads(
                self.decompressor.decompress(
                    self.fernet.decrypt(b85decode(data["_data"]))
                )
            ),
        )
        return event

    def put(self, data) -> typing.Union[str, None]:
        try:
            data = dumps(data).encode()
        except (TypeError, OverflowError):
            raise ValueError(
                "Event must be JSON-serializable. Encode binary data with base64 / base85."
            )
        event_data = self.compressor.compress(data)
        encrypted_data = self.fernet.encrypt(event_data)
        b85data = b85encode(encrypted_data).decode()
        tstamp = time.time_ns()
        payload = {
            "_tstamp": tstamp,
            "_data": b85data,
        }
        try:
            resp = requests.post(
                f"https://{self.addr}/{self.name}/{self.access_key}",
                json=payload,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            # TODO log
            return None
        return str(tstamp)

    def ack(self, tstamp) -> bool:
        try:
            resp = requests.post(
                f"https://{self.addr}/{self.name}/{self.access_key}/{tstamp}",
            )
            resp.raise_for_status()
            return True
        except requests.exceptions.HTTPError:
            return False


if __name__ == "__main__":
    app.run(host="localhost", port=5000, debug=True)
