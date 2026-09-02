# mini-redis

A Redis clone in Python: a single-threaded event loop speaking the RESP2 wire protocol
over TCP, with **zero runtime dependencies**.

## Status

Early. The server accepts connections, parses RESP2 correctly — multibulk arrays and
inline commands, well-formed or not — and answers fifteen commands: `PING`, `ECHO`,
`HELLO`, `SET`, `GET`, `DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`,
`PTTL`, `INCR`, `DECR`. Keys expire lazily, on lookup — there is no active sweep yet, so
an expired key nobody has asked about stays in memory until something does.

**Nothing bounds how much memory a client can use.** There is no cap on key count or
value size, no cap on total keyspace size, and no eviction policy to fall back on if
there were — a client with nothing but `SET` can grow the process until the host runs
out of memory. The read buffer is uncapped too: an unterminated command grows it for as
long as a client keeps sending, which at least costs that client a byte per byte. Queued
replies are the cheap one — one 64 KiB write holds about three thousand `GET`s, every
reply is buffered whole, and the value they all name was stored once, so a few kilobytes
of request can commit gigabytes. `--output-buffer-limit BYTES` closes a connection whose
queued replies exceed it; it defaults to 0, off, which is what real Redis defaults to for
an ordinary client. Four modules are still declared and empty: `persistence.py`,
`ratelimit.py`, `replication.py`, `commands/list.py`.

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

**One lookup answers every existence question.** `SET`'s `NX`/`XX` conditions, `INCR` and
`DECR`'s read-modify-write, every type check, and a plain `GET` all go through
`Store.lookup()` — never a raw dict test. A key past its deadline is deleted the moment
that lookup finds it, so no handler can see a key its own logic already considers gone.

## Layout

```
server.py       entry point, listener, signal handling, dispatch
event_loop.py   selectors readiness dispatch (on_readable / on_writable / on_accept)
connection.py   per-connection socket, read/write buffers, lifecycle
resp.py         RESP2 parser and serializer
store.py        keyspace, expiry index, pending-effects queue
commands/       registry.py, server.py and string.py have code; list.py does not
tests/          pytest suite, run in CI against Python 3.11 and 3.13
```

[Design notes](docs/DESIGN.md) cover why each of these is shaped the way it is, and fifteen
deliberate differences from real Redis.
