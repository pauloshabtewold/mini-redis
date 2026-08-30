# mini-redis

A Redis clone in Python: a single-threaded event loop speaking the RESP2 wire protocol
over TCP, with **zero runtime dependencies**.

## Status

Early. The server accepts connections, parses RESP2 correctly — multibulk arrays and
inline commands, well-formed or not — and answers three commands: `PING`, `ECHO`, `HELLO`.

**There is no keyspace yet, so this is not usable as a key-value store.** `SET` returns the
same unknown-command error a typo would. Six modules are declared and empty: `store.py`,
`persistence.py`, `ratelimit.py`, `replication.py`, and `commands/string.py`,
`commands/list.py`.

What does work is the part underneath: framing, buffering, and connection lifecycle.

## Quickstart

Python 3.11+.

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/python server.py --port 7000
```

```
$ redis-cli -p 7000 PING
PONG
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
could make progress. A malformed byte is still refused the moment it arrives rather than
waited out.

**Partial writes cost nothing.** A reply that can't leave in one `send()` sits in a
per-connection write buffer until a later writable event drains it. Write interest is
cleared the moment the buffer empties, so an idle connection isn't spinning the selector.

**One connection's failure is one connection's problem.** A single exception boundary wraps
the whole per-connection path; anything other than a retryable error closes that connection
and leaves the server and every other connection running. An unknown command answers with
an error rather than a disconnect, so a client's own capability probes fail harmlessly.

## Layout

```
server.py       entry point, listener, signal handling, dispatch
event_loop.py   selectors readiness dispatch (on_readable / on_writable / on_accept)
connection.py   per-connection socket, read/write buffers, lifecycle
resp.py         RESP2 parser and serializer
commands/       registry.py + server.py have code; string.py and list.py do not
tests/          pytest suite, run in CI against Python 3.11 and 3.13
```

[Design notes](docs/DESIGN.md) cover why each of these is shaped the way it is, and twelve
deliberate differences from real Redis.
