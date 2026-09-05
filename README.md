# mini-redis

A Redis clone in Python: a single-threaded event loop speaking the RESP2 wire protocol
over TCP, with **zero runtime dependencies**.

## Status

Early. The server accepts connections, parses RESP2 correctly — multibulk arrays and
inline commands, well-formed or not — and its command set is complete: twenty-six
commands — `PING`, `ECHO`, `HELLO`, `SET`, `GET`, `DEL`, `EXISTS`, `TYPE`, `EXPIRE`,
`PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `INCR`, `DECR`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`,
`LRANGE`, `LLEN`, `DBSIZE`, `KEYS`, `FLUSHALL`, `INFO`, `CONFIG`. Keys expire lazily, on
lookup — there is no active sweep yet, so an expired key nobody has asked about stays in
memory until something does.

One consequence of that command list is worth knowing before you reach for a client
library. `INCRBY` and `DECRBY` are not among them, and `redis-py` defines `.incr()` and
`.decr()` as aliases for them — so `r.incr("k")` puts `INCRBY` on the wire and comes back
`ERR unknown command 'INCRBY'`, even though this server implements `INCR` and answers it
correctly. `r.execute_command("INCR", "k")` reaches it. `redis-py`'s `.lpop(name, count)`
has the same shape: it sends `LPOP name count` on the wire, and this server's exact
two-argument arity for `LPOP` answers a wrong-number-of-arguments error rather than the
two-element reply real Redis would give. `redis-benchmark`'s default run gets further
than it used to — `PING`, `SET`, `GET`, `INCR`, `LPUSH`, `RPUSH`, `LPOP` and `RPOP` all
complete now — and exits at `SADD`, the first command in its sequence this server does
not implement at all.

**Nothing bounds how much memory a client can use.** There is no cap on key count or
value size, no cap on total keyspace size, and no eviction policy to fall back on if
there were — a client with nothing but `SET` can grow the process until the host runs
out of memory. The read buffer is uncapped too: an unterminated command grows it for as
long as a client keeps sending, which at least costs that client a byte per byte. Queued
replies are the cheap one — one 64 KiB write holds a few thousand `GET`s — about three thousand at a
short sixteen-byte value, and between one and six thousand across ordinary sizes — every
reply is buffered whole, and the value they all name was stored once, so a few kilobytes
of request can commit gigabytes. `--output-buffer-limit BYTES` closes a connection whose
queued replies exceed it; it defaults to 0, off, which is what real Redis defaults to for
an ordinary client. `KEYS *` has the same shape: it materialises its whole reply as one
array holding every key in the keyspace, with no cap of its own, so the reply is bounded
only by however large the keyspace has already grown — real Redis has the same property
and documents it. `INFO`'s `used_memory` is peak resident memory rather than current on
any platform without `/proc`: it reads `/proc/self/statm` where that exists and falls
back to `resource.getrusage(...).ru_maxrss`, a high-water mark, so the two figures agree
right after a bulk load and diverge for a server that has since freed memory back to the
allocator. Three modules are still declared and empty: `persistence.py`, `ratelimit.py`,
`replication.py`.

## Quickstart

Python 3.11+.

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/python server.py --port 7000
```

```
$ redis-cli -p 7000 PING
PONG
$ redis-cli -p 7000 SET foo bar
OK
$ redis-cli -p 7000 GET foo
bar
```

If you run on the default 6379 and a real Redis is already there, the bind fails with
`Address already in use` — or worse, you get a healthy-looking `PONG` from *that* server.
The `listening on 127.0.0.1:<port>` line at startup is what tells you this one answered.

```bash
.venv/bin/python -m pytest
```

## What's interesting here

**Incremental RESP2 parsing.** `resp.py` parses a multibulk array a step at a time — header,
then one element per call — so a command arriving in fragments is never re-scanned from the
front. Each step reports what it consumed and the buffer length at which another attempt
could make progress. A header line can report no such length — its own is declared nowhere —
so it carries the position its last terminator search reached instead, and the next attempt
resumes there. Without that, locating a header walks the whole buffer on every readable
event, which is quadratic in what one connection has sent and, on a single-threaded loop, is
every other client's problem too. A malformed byte is still refused the moment it arrives
rather than waited out.

**Partial writes cost nothing.** A reply that can't leave in one `send()` sits in a
per-connection write buffer until a later writable event drains it. Write interest is
cleared the moment the buffer empties, so an idle connection isn't spinning the selector.

**One connection's failure is one connection's problem.** A single exception boundary wraps
the whole per-connection path; anything other than a retryable error closes that connection
and leaves the server and every other connection running. An unknown command answers with
an error rather than a disconnect, so a client's own capability probes fail harmlessly.

**One lookup answers every existence question, with one exception.** `SET`'s `NX`/`XX`
conditions, `INCR` and `DECR`'s read-modify-write, every type check, and a plain `GET`
all go through `Store.lookup()` — never a raw dict test. A key past its deadline is
deleted the moment that lookup finds it, so no handler can see a key its own logic
already considers gone. `DBSIZE`, `KEYS` and `INFO`'s keyspace line are the deliberate
exception: counting, listing or reporting through `lookup()` would delete every expired
key any of the three scanned past, so all three read the keyspace directly instead and
filter what they report.

## Layout

```
server.py       entry point, listener, signal handling, dispatch
event_loop.py   selectors readiness dispatch (on_readable / on_writable / on_accept)
connection.py   per-connection socket, read/write buffers, lifecycle
resp.py         RESP2 parser and serializer
store.py        keyspace, expiry index, pending-effects queue
commands/       registry.py, server.py, string.py and list.py have code; twenty-six commands total
tests/          pytest suite, run in CI against Python 3.11 and 3.13
```

[Design notes](docs/DESIGN.md) cover why each of these is shaped the way it is, and twenty
deliberate differences from real Redis.
