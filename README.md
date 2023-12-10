# FreeQ

---

## Zero-setup end-to-end encrypted message queue for free in 1 line of python

---

## TL;DR;

No need to set up a server, a DB, redis, dedicated queue, hosted pub/sub and worrying about free tier limits, or anything.
`pip install freeq` and go

#### Install:
```shell
pip install -U freeq
```
#### Publisher:
```python
> from freeq import Queue
> q = Queue(name='thisismyqueuename',                       # made up, super unique
            access_key='so_noone_can_write_to_my_queue',    # made up, identifies the queue along with name
            secret_key='i_shit_when_i_sneeze')              # made up, never leaves the client
> q.put({"pool_temperature": "too low"})
```
#### Consumer:
```python
> from freeq import Queue
> q = Queue(name='thisismyqueuename',                       # made up, super unique
            access_key='so_noone_can_write_to_my_queue',    # made up, identifies the queue along with name
            secret_key='i_shit_when_i_sneeze')              # made up, never leaves the client
> event = q.get()
```
## Limitations

The server runs on free-tier infra (for now), so each queue is limited to:
- 2048 messages per queue
- 2048 bytes per message (after encryption and compression)
- messages expire after 48 hours

You can [run your own server](https://github.com/hronecviktor/freeq-server) to get around this.
Configure the client to use it with env vars:
```FREEQ_SERVER_ADDRS=https://server1.com,https://server2.com```

## Contributing

PRs and issues are welcome, there is barely any code so should be easy to get a grasp of it. 

OpenAPI / swagger schema available @ https://weyland.yutani.enterprises/docs so feel free to generate a client for [insert language] using e.g. [openapi-generator](https://openapi-generator.tech/).

Please make sure your client **encrypts everything** and doesn't spam the API too much.
