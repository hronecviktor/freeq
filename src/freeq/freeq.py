from base64 import b64decode, b64encode
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
        self.addr = server_addr
        self.compressor = zstd.ZstdCompressor()
        self.decompressor = zstd.ZstdDecompressor()
        self.fernet_key = b64encode(scrypt(secret_key, b"", 32, N=2**14, r=8, p=1))
        self.fernet = Fernet(self.fernet_key)

    def get(self, ack: bool = True, block: bool = False) -> typing.Union[Event, None]:
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
        # Non-blocking and empty queue
        if resp.status_code == 204:
            return None

        payload = resp.json()
        event = Event(
            self,
            payload["tstamp"],
            **json.loads(
                self.decompressor.decompress(
                    self.fernet.decrypt(b64decode(payload["event"]["data"]))
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
        encrypted_data = self.fernet.encrypt(event_data)
        b64data = b64encode(encrypted_data).decode()
        tstamp = time.time_ns()
        payload = {
            "data": b64data,
        }
        resp = requests.post(
            f"https://{self.addr}/{self.name}/{self.access_key}",
            json=payload,
        )
        resp.raise_for_status()
        return str(tstamp)

    def ack(self, tstamp) -> bool:
        resp = requests.post(
            f"https://{self.addr}/{self.name}/{self.access_key}/{tstamp}",
        )
        resp.raise_for_status()
        return True

    def clear(self) -> bool:
        resp = requests.delete(
            f"https://{self.addr}/{self.name}/{self.access_key}",
        )
        resp.raise_for_status()
        return True
