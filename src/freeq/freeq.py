import time

from base64 import b64decode, b64encode
from functools import partial
import json
import os
import random
import typing

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
import requests
import zstandard as zstd


FREEQ_SERVER_ADDRS = os.getenv(
    "FREEQ_SERVER_ADDRS",
    "https://yutani.enterprises,https://weyland.enterprises",
).split(",")

dumps = partial(json.dumps, separators=(",", ":"), indent=None)


class RandomServer:
    def __str__(self):
        return random.choice(FREEQ_SERVER_ADDRS)


server = RandomServer()


class Event(dict):
    def __init__(self, _queue, _tstamp, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = _queue
        self.tstamp = _tstamp

    def ack(self):
        self.queue.ack(self.tstamp)


class Queue:
    def __init__(self, name, access_key, secret_key):
        self.name = name
        self.access_key = access_key
        self.compressor = zstd.ZstdCompressor()
        self.decompressor = zstd.ZstdDecompressor()
        kdf = Scrypt(salt=b"", length=32, n=2**14, r=8, p=1)
        key = kdf.derive(secret_key.encode())
        self.aesgcm = AESGCM(key)

    def get(self, ack: bool = True, block: bool = False) -> typing.Union[Event, None]:
        while True:
            resp = requests.get(
                f"{server}/{self.name}/{self.access_key}",
                params={"ack": ack},
            )
            resp.raise_for_status()
            # Poll until we get an event, or return None if we're not blocking
            if not block:
                break
            if resp.status_code == 200:
                break
            time.sleep(5)
        # Non-blocking and empty queue
        if resp.status_code == 204:
            return None

        payload = resp.json()
        nonce, data = payload["event"]["data"].split(":")
        event = Event(
            self,
            payload["tstamp"],
            **json.loads(
                self.decompressor.decompress(
                    self.aesgcm.decrypt(b64decode(nonce), b64decode(data), None)
                )
            ),
        )
        return event

    def put(self, data: dict) -> typing.Union[str, None]:
        if not isinstance(data, dict):
            raise ValueError(
                "Event must be JSON-serializable dict. Encode binary data with base64 / base85."
            )
        try:
            data = dumps(data).encode()
        except (TypeError, OverflowError):
            raise ValueError(
                "Event must be JSON-serializable dict. Encode binary data with base64 / base85."
            )
        event_data = self.compressor.compress(data)
        nonce = os.urandom(12)
        encrypted_data = self.aesgcm.encrypt(nonce, event_data, None)
        b64data = b64encode(encrypted_data).decode()
        b64nonce = b64encode(nonce).decode()
        payload = {
            "data": f"{b64nonce}:{b64data}",
        }
        resp = requests.post(
            f"{server}/{self.name}/{self.access_key}",
            json=payload,
        )
        resp.raise_for_status()
        tstamp = resp.json()["tstamp"]
        return str(tstamp)

    def ack(self, tstamp) -> bool:
        resp = requests.post(
            f"{server}/{self.name}/{self.access_key}/{tstamp}",
        )
        resp.raise_for_status()
        return True

    def clear(self) -> bool:
        resp = requests.delete(
            f"{server}/{self.name}/{self.access_key}",
        )
        resp.raise_for_status()
        return True
