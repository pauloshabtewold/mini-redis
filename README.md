# mini-redis

mini-redis is a Redis clone built from scratch in Python: a single-threaded event loop speaking the RESP2 wire protocol over TCP, with string and list data types, TTL expiry, snapshot persistence, per-connection rate limiting, and leader-follower replication.

## Status

The server accepts TCP connections, reads the bytes each one sends, and parses the RESP2 commands in that stream -- multibulk arrays and inline commands such as a bare `PING\r\n` alike, well-formed or not. It sends nothing back. No command has a handler, and nothing carries a reply out to the socket even if one existed: the dispatcher that would route a parsed command to a handler, and the write-buffer drain that would carry the handler's answer back out, arrive together.

A client speaking to it with `redis-cli` gets no answer to anything it sends, `PING` included: the connection opens, the request goes out, and nothing comes back, because the command layer on the other end doesn't exist yet. A command that fails to parse closes only the connection that sent it -- the server, and every other connection on it, keeps running.

One limitation is worth knowing before pointing anything at it, because nothing in the transcript reveals it: no size or count cap is applied to a request. A command that never completes -- a bulk string whose declared length runs to a gigabyte, whose body never arrives -- holds every byte sent so far in that connection's read buffer for as long as the connection stays open, and one client may open as many connections as the process has descriptors. The caps that refuse such a request, and the connection limit that bounds how many may be open at once, ship with the flags that configure them. Binding `127.0.0.1` and nothing else is what keeps that a local concern in the meantime.

## Requirements

- Python >=3.11
- No runtime dependencies beyond the standard library (`socket`, `selectors`)

## Setup

```
pip install -e '.[dev]'
```

The single quotes around `.[dev]` are required: unquoted, `[dev]` is a glob pattern in zsh (the default macOS shell), so the shell expands it before pip ever sees it and the install silently does nothing.

This installs the `dev` extra: `pytest` and the `redis` client (`>=5,<9`).

## Running the server

```
python3 server.py [--port PORT]
```

`--port` defaults to `6379`, the same default `redis-cli` uses, so `redis-cli` run with no arguments reaches a locally running instance. What it gets past the connection is silence: nothing sent to it gets a reply, `PING` included, for the reason the Status section above gives.

## License

MIT. Copyright (c) 2026 Paulos Habtewold. See `LICENSE`.

## Module layout

Root-level modules, plus the `commands` package:

- `server.py` -- entry point: the `--port` flag (default `6379`), a non-blocking listener bound to `127.0.0.1`, `SIGINT`/`SIGTERM` handling, and the event loop's three callbacks. Parses every command it receives and answers none of them. The four periodic tasks the design calls for -- expiry sweep, snapshot interval, follower reconnect, slow-follower check -- are not wired into the run loop.
- `event_loop.py` -- `selectors` readiness dispatch: `on_readable`, `on_writable`, `on_accept`, driven by a `select()` bounded to a 100 ms timeout.
- `connection.py` -- the `Connection` class: private socket, read and write buffers, a `closed` flag, `role`, and two state slots read and written only by the rate limiter and the replication link. `queue(bytes)` is the only outbound API; nothing drains the buffer it fills yet, so nothing calling it puts a byte on the wire.
- `resp.py` -- RESP2 parser and serializer, `bytes` in and `bytes` out. Parses multibulk arrays and inline commands and reports an incomplete command without consuming it; its response encoders exist and have no caller yet.
- `store.py` -- the keyspace, the expiry index, and the pending-effects queue.
- `persistence.py` -- versioned snapshot format: save and load.
- `ratelimit.py` -- per-connection sliding-window rate limiter.
- `replication.py` -- leader-side follower manager and follower mode.
- `commands/` -- the command layer, split by type: `registry.py` (name, arity, read/write/other tag, dispatch), `string.py`, `list.py`, and `server.py` (connection and server commands).
- `tests/` -- the test suite.

### Why the protocol layer works in bytes

A parser built on `str` has to guess an encoding the moment it reads off the socket, and a value that isn't in that encoding comes out corrupted on the other side. `resp.py` never makes that guess: every value in this project -- keys, values, command names, error bodies -- is `bytes`, starting from the first line of the parser and continuing through the store and the snapshot format.

### Why a bulk string's body is delimited by its declared length

A bulk string is binary-safe, which means its body is allowed to contain the exact byte pair, `\r\n`, that ends every header line in the protocol. A parser that finds the end of a body by searching for `\r\n` truncates a body that happens to contain that pair and leaves the rest of it to be re-parsed as a bogus follow-on command. `resp.py` avoids the search: a body is read as a slice at a computed offset, `buf[start:start+length]`, and the only check left is that the two bytes right after it are `\r\n`. Header lines (`*N\r\n`, `$N\r\n`) are the opposite case -- their own length isn't declared anywhere, so searching for `\r\n` is exactly the right tool for them, and only for them.

### Why Connection is a class

Keeping a raw socket in a dict keyed by its file descriptor works right up until something else needs to be tracked per connection. Four more things attach state to a connection later in this project -- rate-limit tracking, a follower link, buffer water marks, an incomplete-command deadline -- and each one would need its own parallel fd-keyed structure if `Connection` didn't already exist to hold it. It ships now, with fields nothing reads yet, so that structure exists once instead of getting rebuilt piecemeal.

### Why the two extra state slots are untyped

`rate_limit_state` and `replication_state` hold `None` here and are read and written only by the module that owns each one. Giving either of them a real type would make `connection.py` -- the most foundational module in the project -- import the module that defines that type, and that module needs `Connection` back: an import cycle, in the one file nearly every other module in this project eventually imports.

### Why select() has a bounded timeout

The tasks that need to run on a schedule -- sweeping expired keys, writing a snapshot -- run right after `select()` returns, never on a clock of their own. An unbounded `select()` on a server with no client traffic would simply never return, so once those tasks exist, an idle server would never run them. The timeout is bounded to keep that from happening, and it doubles as the longest a stop signal has to wait before the run loop notices it.

### Why inline commands are supported

A bare `PING\r\n`, with no multibulk framing at all, parses as a command here. That's what makes `telnet localhost 6379` and `nc` work as a demo with nothing installed beyond what a shell already has -- point either one at the port and type a command by hand.

It isn't free. Real Redis ships a companion load-generation CLI alongside `redis-cli`, and its default run leans on that same unframed `PING` to get started. Rejecting inline input would have stopped that tool cold on its first request with a clean protocol error -- a legible failure. Accepting it instead means that same first request gets no reply at all: this server answers nothing, to anyone, so the tool hangs waiting for a response that never comes -- a far less legible failure, and a caveat worth knowing before trusting its output at face value.

### Why the periodic tasks belong in server.py

The four periodic tasks driven off the `select()` timeout -- expiry sweep, snapshot interval, follower reconnect, slow-follower check -- belong in `server.py` rather than in `event_loop.py`. `event_loop.py` is kept to readiness dispatch alone: it knows the `Connection` class and three callbacks (`on_readable`, `on_writable`, `on_accept`) and nothing else. Putting the periodic tasks there would pull the store, persistence, the command layer, and eventually replication into the thinnest module in the project. None of the four is wired into the run loop yet.

### Why commands is a package

The command layer covers twenty-six commands across the string, list, and server/connection groups, each with its own arity check, type check, option parsing, and effect return. A single `commands.py` holding all of that would be a god object, so the command layer is split by type into its own package instead, with `registry.py` handling registration and dispatch.
