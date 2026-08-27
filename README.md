# mini-redis

mini-redis is a Redis clone built from scratch in Python, around a single-threaded event loop speaking the RESP2 wire protocol over TCP. It is being built in stages toward string and list data types, TTL expiry, snapshot persistence, per-connection rate limiting, and leader-follower replication; Status below is what answers today, and it is three commands.

## Status

The server accepts TCP connections, reads the bytes each one sends, and parses the RESP2 commands in that stream -- multibulk arrays and inline commands such as a bare `PING\r\n` alike, well-formed or not. Three commands answer: `PING`, `ECHO` and `HELLO`. Everything else gets `-ERR unknown command '<name>'`, and the connection stays open either way -- an unrecognized command is a mistake in one request, not a reason to drop the connection, so a real client's own probes fail harmlessly instead of hanging it. A reply that can't go out in one `send()` -- the kernel's send buffer is full -- sits in a per-connection write buffer until a later writable event drains what's left, so a partial write costs nothing for as long as the connection lives. Closing it is where that guarantee stops: every close path, an ordinary shutdown included, makes one last non-blocking attempt to hand the remainder to the kernel and then drops whatever is still queued, so a client that has stopped reading can still lose the tail of a large reply. There is still no keyspace: nothing stores a value, so this is not yet usable as a key-value store, and a command like `SET` gets the same unknown-command error a typo would. A command that fails to parse closes only the connection that sent it -- the server, and every other connection on it, keeps running.

One limitation is worth knowing before pointing anything at it, because nothing in a transcript reveals it: nothing caps the memory one request may take, and nothing caps a queued reply either. A command that never completes -- a bulk string whose declared length runs to a gigabyte, whose body never arrives -- holds every byte sent so far in that connection's read buffer for as long as the connection stays open, and a large enough `ECHO` queues a reply just as large on the way out, with nothing yet capping either direction. One client may also open as many connections as the process has descriptors. The caps that refuse an oversized request, the ceiling on a queued reply, and the connection limit that bounds how many may be open at once all ship with the flags that configure them. Binding `127.0.0.1` and nothing else is what keeps that a local concern in the meantime.

## Requirements

- Python >=3.11
- No runtime dependencies beyond the standard library (`socket`, `selectors`)

## Setup

```
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

The virtual environment is not optional on a current Python: `pip install` run against a system interpreter either writes into it or refuses outright with `externally-managed-environment`. Activating it is also what puts `python` and `pip` on `PATH`.

The single quotes around `.[dev]` are required: unquoted, `[dev]` is a glob pattern in zsh (the default macOS shell). No file matches it, so zsh abandons the line with `no matches found: .[dev]`, pip never runs at all, and nothing is installed -- the error names the glob rather than the install, which is what makes it easy to read past.

This installs the `dev` extra: `pytest` and the `redis` client (`>=5,<9`).

Construct that client with `protocol=2`, or it will not talk to this server at all:

```
r = redis.Redis(host="127.0.0.1", port=6379, protocol=2)
```

`redis-py` defaults to RESP3 and opens a connection by sending `HELLO 3`. This server speaks RESP2 only and answers `-NOPROTO`, so with the default constructor every call raises `ResponseError: NOPROTO unsupported protocol version` -- including `ping()`, which makes a refused handshake look like a broken command.

Constructed with `protocol=2` the client sends no `HELLO` at all, and the first two commands it puts on every new connection are `CLIENT SETINFO LIB-NAME redis-py` and `CLIENT SETINFO LIB-VER <version>`, which announce the library to a server that keeps that in `CLIENT INFO`. Neither is implemented here, so both come back `ERR unknown command 'CLIENT'` and the client carries on without them, `ping()` included. That is the probe-fails-harmlessly case described under Status, and it is the reason an unknown command answers rather than closing the connection: two refused announcements are not a reason to lose a connection that works.

## Running the server

```
python3 server.py [--port PORT]
```

`--port` defaults to `6379`, the same default `redis-cli` uses, so `redis-cli` run with no arguments reaches a locally running instance:

```
$ redis-cli PING
PONG
```

Anything other than `PING`, `ECHO` or `HELLO` gets `-ERR unknown command`, rendered by `redis-cli` without the leading `-`; the connection stays open either way, so a client can keep sending after one request gets rejected.

## Running the tests

```
python -m pytest
```

Run from the repository root, with the virtual environment from Setup active.

## License

MIT. Copyright (c) 2026 Paulos Habtewold. See `LICENSE`.

## Module layout

Root-level modules, plus the `commands` package. Each entry names the boundary that module owns; where the module has code, the entry also says how far it goes. Four of them -- `store.py`, `persistence.py`, `ratelimit.py`, `replication.py` -- and two modules under `commands/` -- `string.py`, `list.py` -- have none yet, and hold only that one line.

- `server.py` -- entry point: the `--port` flag (default `6379`), a non-blocking listener bound to `127.0.0.1`, `SIGINT`/`SIGTERM` handling, and the event loop's three callbacks. Every command parsed out of a connection's buffer is dispatched and its reply queued, one flush runs against the whole batch after the last one, and write interest is updated from what that flush leaves behind. A single per-connection exception boundary wraps every step of that path; anything other than a retryable error closes only the connection that raised it. The four periodic tasks the design calls for -- expiry sweep, snapshot interval, follower reconnect, slow-follower check -- are not wired into the run loop.
- `event_loop.py` -- `selectors` readiness dispatch: `on_readable`, `on_writable`, `on_accept`, driven by a `select()` under a timeout the loop is handed rather than one it picks -- `SELECT_TIMEOUT_SECONDS`, 100 ms, is defined in `server.py` and passed to the constructor -- plus `set_write_interest()` to register or clear `EVENT_WRITE` against the selector's own mask.
- `connection.py` -- the `Connection` class: private socket, read and write buffers, a `closed` flag, `role`, a per-connection `id`, and two state slots read and written only by the rate limiter and the replication link. `queue(bytes)` appends to the write buffer and is the only outbound API; `flush()` is the only thing that writes to the socket, looping until the buffer empties or the kernel refuses more, and it reports whether the peer is still connected rather than whether the buffer is empty.
- `resp.py` -- RESP2 parser and serializer, `bytes` in and `bytes` out. Parses multibulk arrays and inline commands, and reports an incomplete command without consuming it, along with the buffer length at which another attempt could make progress; its response encoders are what every handler under `commands/` calls to build a reply.
- `store.py` -- the keyspace, the expiry index, and the pending-effects queue.
- `persistence.py` -- versioned snapshot format: save and load.
- `ratelimit.py` -- per-connection sliding-window rate limiter.
- `replication.py` -- leader-side follower manager and follower mode.
- `commands/` -- the command layer, split by type. `registry.py` (name, arity, read/write/other tag, dispatch) and `server.py` (`PING`, `ECHO`, `HELLO` today) hold code; `string.py` and `list.py` are still one docstring line each.
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

A bare `PING\r\n`, with no multibulk framing at all, parses as a command here, and now gets a real `+PONG` back like any other reply. That's what makes `nc localhost 6379` work as a demo with nothing installed beyond what a shell already has -- point it at the port, type a command, and get an answer in the same session. `telnet localhost 6379` does the same thing on a machine that has telnet, but macOS has not shipped one since 10.13, so there it is a package to install rather than something already on the box. Quoting works the way someone typing at a terminal expects, because that's the grammar a real server applies to the same line: `ECHO "a b"` is one argument with a space in it, `'` quotes as well as `"`, `\xHH` and the usual character escapes are understood inside double quotes, and a quote left open is a protocol error rather than a literal character.

Real Redis ships a companion load-generation CLI alongside `redis-cli`, and the first workload of its default run is exactly that unframed `PING` -- 50 connections all sending `PING\r\n` with no `*`/`$` around it. Accepting inline input is what lets that workload run here at all. The tool opens by asking for two configuration values in ordinary multibulk framing, both of them in a single write, which this server answers with two unknown-command errors in a single write back, and it carries on regardless, printing one warning; the multibulk `PING` workload that follows works too.

### Why the periodic tasks belong in server.py

The four periodic tasks driven off the `select()` timeout -- expiry sweep, snapshot interval, follower reconnect, slow-follower check -- belong in `server.py` rather than in `event_loop.py`. `event_loop.py` is kept to readiness dispatch alone: it knows the `Connection` class and three callbacks (`on_readable`, `on_writable`, `on_accept`) and nothing else. Putting the periodic tasks there would pull the store, persistence, the command layer, and eventually replication into the thinnest module in the project. None of the four is wired into the run loop yet.

### Why commands is a package

The command layer covers twenty-six commands across the string, list, and server/connection groups, each with its own arity check, type check, option parsing, and effect return. A single `commands.py` holding all of that would be a god object, so the command layer is split by type into its own package instead, with `registry.py` handling registration and dispatch.

### Why the kind tag is declared at the handler

A command's `read`/`write`/`other` tag is set through the same decorator that registers it, in `commands/registry.py`, rather than listed in a separate table somewhere. A table is a second place that can disagree with the handler it describes -- a rate limiter or a replication decision reads whichever one it was pointed at, and a command mistagged in the table looks correct at every other call site. Declaring the tag where the command is registered means the declaration and the handler are the same statement, so the two can't drift apart; an enum on top of that turns a typo in the tag into an error at the line that has it, instead of a value that silently matches nothing.

### Why a reply and its effect are different facts

A handler here returns reply bytes and nothing else. What a client sees is not always the whole of what a later feature needs to know happened -- a read that finds an expired key still has to produce a deletion somewhere, with no write command anywhere in sight, and a replica needs that deletion even though no client asked for one. Building a second return value now for a fact that has no consumer yet -- nothing here stores anything, and nothing here replicates -- would be code with no test able to exercise it honestly. The reply and the effect stay one value until the feature that introduces the store needs both.

### Why the write path exists before any reply is large

Three commands exist and none of them touches a keyspace, but a reply can already be as large as `ECHO`'s own argument, and a socket can already refuse to take a large write in one call. Measured against a copy of this server whose only write path was a bare `socket.send()` with nothing behind it to catch what the kernel wouldn't take: 200,000 pipelined `ECHO` commands down one connection, with the client writing every request before it read a byte, which is what fills the kernel's send buffer the way a client that has stopped draining does. This server delivers all 200,000 replies whole on every run. The bare-`send()` copy delivered between 80,000 and 115,000 of them, the count moving run to run on the same workload, and the shape of the loss matters more than the count: one reply -- the one being written when the buffer filled -- arrives cut in half, and almost every reply after it is never transmitted at all, because a `send()` against a full socket raises `BlockingIOError` and writes nothing, so the discarded remainder is a whole reply rather than the tail of one. A write path that survives a partial write isn't an optimization added later -- it's the difference between a reply and a reply the client never sees, and it ships in the same commit as the first reply rather than waiting for a request large enough to make the gap obvious.

### Why write interest turns off the moment the buffer empties

`event_loop.py` asks the selector whether a connection is writable only while that connection's write buffer is non-empty, and clears the request the instant a flush empties it. An idle, healthy socket is writable on nearly every pass, so a write request left registered against an empty buffer would have the selector reporting that connection ready again and again with nothing queued to send -- the loop would spend its time asking the kernel about a socket that has nothing to say. The selector's own mask is also the only place this fact is kept: a separate flag on `Connection` would be a second copy that can disagree with the selector, in either direction, with nothing around to notice when it does.

### Why an unknown command gets an error, not silence

`-ERR unknown command '<name>'` costs one write and leaves the connection open. Silence costs nothing to produce and is worse to receive: a client that gets nothing back cannot tell a slow server from one that will never answer, and has no way to distinguish "still working on it" from "doesn't understand this at all" -- it can only wait, or guess. An error reply lets a client's own retry and timeout logic run the way it was built to, on a connection that never had to be dropped over a single bad request.

### Why one exception boundary covers a whole connection

Every step of handling a connection's traffic -- reading, parsing, dispatching, writing -- runs inside the same `try`/`except` in `server.py`, reached from both the readable and the writable callback, rather than one boundary per callback. Two boundaries are two places to write a slightly different close path, and the failure that produces is invisible until a client happens to hit the one that's wrong: a bug that closes its connection cleanly when it surfaces on a read, and takes the whole process down when the same bug surfaces on a write. A single-threaded server has exactly one thread to lose, which is what makes this boundary mandatory rather than a defensive habit -- every other connection's traffic stops the moment an unhandled exception reaches the top of that thread.

### Twelve deliberate differences from real Redis

- An unknown command answers `ERR unknown command '<name>'`. Real Redis appends the first few arguments it received to that message; this server never does, and the argument list plays no part in it. That does not make this server's message the shorter of the two, which is the easy thing to assume: real Redis renders the name through a `%.128s` conversion and its whole reply tops out at 181 bytes however long the name was, while this server echoes the name whole and caps nothing. A 200,000-byte command name answers 181 bytes there and 200,025 bytes here. It is the only reply this server can be made to produce at an arbitrary size while it still has no keyspace to put anything in, and what bounds it is the missing request cap described under Status rather than anything in the message.
- `HELLO` with an argument other than `2` answers `NOPROTO unsupported protocol version`, whatever that argument is. Real Redis has two rules here that this server collapses into one: a non-numeric argument gets a different error naming a parse failure specifically, and `3` isn't an error there at all -- it's RESP3, a second protocol version a real server accepts, switching that connection to a different reply grammar. This server speaks RESP2 and nothing else, so `HELLO 3` is refused with the same message as `HELLO banana`, and there is one rule to remember rather than one rule plus two exceptions.
- `HELLO` takes the protocol version and nothing else. Real Redis also accepts `AUTH <username> <password>` and `SETNAME <name>` after it, so `HELLO 2 AUTH u p` answers `WRONGPASS` there and `ERR wrong number of arguments for 'hello' command` here. Neither option has anything to act on yet -- there is no authentication in this server, and no connection name to set -- and a client library that opens with `HELLO 2 AUTH ...` gets an arity error rather than an authentication one.
- `HELLO` counts its arguments before it looks at any of them. `HELLO x y` answers `ERR wrong number of arguments for 'hello' command` here and `ERR Protocol version is not an integer or out of range` there; `HELLO 2 BADOPT` answers that same arity error here and `ERR Syntax error in HELLO option 'BADOPT'` there. Real Redis parses the version first and then walks the options, so it names whichever one is actually wrong; this server takes a version and nothing after it, which makes every one of those shapes a wrong argument count before it is anything else. The more specific complaint is the one that never gets made.
- Commands that parsed successfully ahead of a malformed one in the same write lose their replies. `PING`, `PING`, then `ECHO "unterminated` sent as one write answers only `ERR Protocol error: unbalanced quotes in request` here, where real Redis answers both `PONG`s first and then the error. The parser reports the whole buffer's first failure rather than returning what it had already read, and the connection closes immediately either way, so the loss is bounded by whatever that one connection had in flight. Sent as separate writes, every command is answered normally on both servers. Changing this belongs with the pipelining work rather than here.
- A negative multibulk count is a protocol error. `*-1\r\n` gets `ERR Protocol error: invalid multibulk length` here and the connection closes; real Redis consumes any count of zero or less without replying and goes straight on to the next command in the same buffer. A negative *bulk* length inside a multibulk -- `$-1` -- is rejected by both, which is what makes this one easy to miss.
- A bulk body must be followed by `\r\n`. `*1\r\n$4\r\nPINGXX\r\n` declares four bytes and then supplies six, which this server refuses with `ERR Protocol error: unterminated bulk string`; real Redis advances past the body by its declared length and never inspects the two bytes after it, so it answers `PONG` and treats the stray bytes as an empty inline command. That error string has no real-Redis equivalent -- this server is the stricter of the two here.
- Every `\r` that ends a header must be followed by `\n`. Real Redis finds the `\r` and steps two bytes past it without ever looking at what the second byte is, so `*1\rX$1\r\na\r\n` and `*1\r\n$1\rXa\r\n` both parse there and answer `ERR unknown command 'a'`; this server refuses them with `invalid multibulk length` and `invalid bulk length` and closes. The same reading is what makes the entry above refuse `*1\r\n$1\r\na\rX`. Three positions, one rule: a lone `\r` is a terminator there and a framing error here, and this server is again the stricter of the two.
- A NUL byte ends an inline command line. `PING\x00X\r\n` runs `PING` here and everything after the NUL is dropped, which is what real Redis's own argument splitter would do with that line -- but real Redis never gets there, because it locates an inline command's end with a C string scan that stops at the NUL, so the line never looks complete and that connection stops answering permanently. The difference is invisible to any client that does not send a NUL.
- A NUL byte inside a *multibulk* command name is part of the name. `*1\r\n$4\r\nPI\x00G\r\n` answers `ERR unknown command 'PI\x00G'` here, the NUL sitting raw in the error body; real Redis builds the same message with a C string conversion that stops at the NUL, so it reports the command as `PI` and never sees the rest of it. Both servers answer and both keep the connection open, so the difference is only in what the message says was asked for.
- A NUL anywhere in multibulk *framing* is a protocol error here and silence there. `*1\r\n\x00\r\n`, `*\x001\r\n` and `*1\r\n$\x004\r\n` each get an error and an immediate close from this server -- `expected '$', got '\x00'`, `invalid multibulk length` and `invalid bulk length` respectively. Real Redis answers none of the three: it stops answering on that connection permanently, so a client sits waiting on a reply that will never come instead of reading an error and reconnecting. This is the same reasoning as the unknown-command entry above, applied to a connection that has to be dropped: something that says why beats nothing at all.
- A declared *bulk* length is never refused for being too large. Real Redis rejects a bulk length above `proto-max-bulk-len`, 512 MB by default, the moment it reads the header; this server accepts any length it can convert and then waits for a body that may never arrive. Nor is a line refused for running on without a newline, in any of the three places one can: real Redis gives up at 64 KiB on an inline command with `ERR Protocol error: too big inline request`, on a multibulk count with `ERR Protocol error: too big mbulk count string`, and on a bulk length with `ERR Protocol error: too big bulk count string`, and this server keeps buffering in all three. Both are the wire-facing half of the missing request cap described under Status, and they are the one entry on this list that is a deferred feature rather than a preference -- the cap ships with the flag that configures it.
