# mini-redis

mini-redis is a Redis clone built from scratch in Python: a single-threaded event loop speaking the RESP2 wire protocol over TCP, with string and list data types, TTL expiry, snapshot persistence, per-connection rate limiting, and leader-follower replication.

## Status

This repository is a scaffold. Every module listed below exists as a single file holding one docstring line naming its responsibility; none contains application logic yet.

## Requirements

- Python >=3.11
- No runtime dependencies beyond the standard library (`socket`, `selectors`)

## Setup

```
pip install -e '.[dev]'
```

The single quotes around `.[dev]` are required: unquoted, `[dev]` is a glob pattern in zsh (the default macOS shell), so the shell expands it before pip ever sees it and the install silently does nothing.

This installs the `dev` extra: `pytest` and the `redis` client (`>=5,<9`), used by the test suite.

## License

MIT. Copyright (c) 2026 Paulos Habtewold. See `LICENSE`.

## Module layout

Root-level modules, plus the `commands` package:

- `server.py` -- entry point: CLI flags, signal handling, and the four periodic tasks that run after each `select()` return (expiry sweep, snapshot interval, follower reconnect, slow-follower check).
- `event_loop.py` -- `selectors` readiness dispatch: `on_readable`, `on_writable`, `on_accept`.
- `connection.py` -- the `Connection` class: socket, read/write buffers, role, and the outbound API.
- `resp.py` -- RESP2 parser and serializer.
- `store.py` -- the keyspace, the expiry index, and the pending-effects queue.
- `persistence.py` -- versioned snapshot format: save and load.
- `ratelimit.py` -- per-connection sliding-window rate limiter.
- `replication.py` -- leader-side follower manager and follower mode.
- `commands/` -- the command layer, split by type: `registry.py` (name, arity, read/write/other tag, dispatch), `string.py`, `list.py`, and `server.py` (connection and server commands).
- `tests/` -- the test suite.

### Why the periodic tasks live in server.py

The four periodic tasks driven off the `select()` timeout -- expiry sweep, snapshot interval, follower reconnect, slow-follower check -- live in `server.py` rather than in `event_loop.py`. `event_loop.py` is kept to readiness dispatch alone: it knows the `Connection` class and three callbacks (`on_readable`, `on_writable`, `on_accept`) and nothing else. Putting the periodic tasks there would pull the store, persistence, the command layer, and eventually replication into the thinnest module in the project.

### Why commands is a package

The command layer covers twenty-six commands across the string, list, and server/connection groups, each with its own arity check, type check, option parsing, and effect return. A single `commands.py` holding all of that would be a god object, so the command layer is split by type into its own package instead, with `registry.py` handling registration and dispatch.
