from base64 import b85encode, b64encode, b85decode
from functools import partial
import json
import time
import typing

from cryptography.fernet import Fernet
from Crypto.Protocol.KDF import scrypt
import requests
import zstandard as zstd


dumps = partial(json.dumps, separators=(",", ":"), indent=None)


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
