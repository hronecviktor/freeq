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
