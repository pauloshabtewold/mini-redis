# mini-redis

A Redis clone in Python: a single-threaded event loop speaking the RESP2 wire protocol
over TCP, with **zero runtime dependencies**.

## Status

Early. The server accepts connections, parses RESP2 correctly — multibulk arrays and
inline commands, well-formed or not — and its command set is complete: twenty-six
commands — `PING`, `ECHO`, `HELLO`, `SET`, `GET`, `DEL`, `EXISTS`, `TYPE`, `EXPIRE`,
`PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `INCR`, `DECR`, `LPUSH`, `RPUSH`, `LPOP`, `RPOP`,
`LRANGE`, `LLEN`, `DBSIZE`, `KEYS`, `FLUSHALL`, `INFO`, `CONFIG`. Keys expire lazily, on
lookup, and on a sampled active sweep: `--expiry-sweep-interval` bounds how often that
sweep runs, not how long any one expired key stays resident. Each pass samples a few of
the keys that carry a TTL and deletes the expired ones, so nothing bounds when a
particular key is reclaimed — which is why expiry on lookup is still the load-bearing
half: a key you ask for is never served stale, whatever the sweep has reached.

One consequence of that command list is worth knowing before you reach for a client
library. `INCRBY` and `DECRBY` are not among them, and `redis-py` defines `.incr()` and
`.decr()` as aliases for them — so `r.incr("k")` puts `INCRBY` on the wire and comes back
`ERR unknown command 'INCRBY'`, even though this server implements `INCR` and answers it
correctly. `r.execute_command("INCR", "k")` reaches it. `redis-py`'s `.lpop(name, count)`
has the same shape: it sends `LPOP name count` on the wire, and this server's exact
two-argument arity for `LPOP` answers a wrong-number-of-arguments error rather than the
two-element reply real Redis would give. The one that costs the most is `pipeline()`,
whose `transaction` argument defaults to true: the default call wraps the batch in
`MULTI`/`EXEC`, neither of which this server implements, so it fails on `EXEC` where
`r.pipeline(transaction=False)` sends the same commands and works. `redis-benchmark`'s default run gets further
than it used to — `PING`, `SET`, `GET`, `INCR`, `LPUSH`, `RPUSH`, `LPOP` and `RPOP` all
complete now — and exits at `SADD`, the first command in its sequence this server does
not implement at all.

A `redis-py` client that asks for RESP3 by default, as the one this suite runs against
does, sends `HELLO 3` as it connects, and this server answers `NOPROTO unsupported
protocol version`, so every call fails before it reaches any of the commands above
unless the client is built with `protocol=2`, which everything above about `redis-py`
assumes.

**Nothing bounds how much memory a client can use.** There is no cap on key count, no
cap on total keyspace size, and no eviction policy to fall back on if there were — a
client with nothing but `SET` can grow the process until the host runs out of memory.
The read buffer is uncapped too: an unterminated command grows it for as long as a
client keeps sending, which at least costs that client a byte per byte — that is the one
case the two caps below do not reach, because a line with no terminator yet has no
declared length to compare against anything. A line that *has* ended is bounded even
with no flag of its own: an inline command is refused past 64 KiB, the reference's own
ceiling, because it is complete the moment its newline arrives and nothing later can
reach it. `--max-value-size BYTES` refuses a single declared bulk element — including
the command name and any key, not only what a human would call the value — that
declares more than BYTES, before a body byte of it is read; it defaults to 64 MiB, and 0
turns the check off. `--max-multibulk COUNT` refuses a command declaring more than COUNT
elements; it defaults to 1,048,576, and 0 turns it off too. Neither bounds anything in
aggregate — each is a ceiling on one element, or on one command's element count, never on
a connection's traffic as a whole.
Queued replies are the cheap one — one 64 KiB write holds a few thousand `GET`s — about three thousand at a
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
allocator. `ratelimit.py` and `replication.py` are declared and empty: neither feature
is built.

**Four more flags cover persistence and the active sweep.** Persistence is on by
default. `--snapshot-path` names the file a snapshot is written to and read back from
and defaults to `./dump.mrdb`, resolved against the directory the server was started in;
a file that is present but will not decode refuses startup rather than starting empty
over it, and so does anything else at that path that is not a regular file -- a
directory, a named pipe. Whenever `--snapshot-interval` is non-zero, the startup check
tries the write it guards against rather than only inspecting the path: it creates and
removes a temporary file shaped like the one a save writes, in the snapshot's own
directory, so a directory that cannot take that file, a name the filesystem will not
accept, and a directory that will not let the file be removed are all refused before the
server starts, and an existing snapshot whose file flags would block a rename over it is
refused too. What it judges at that path is the entry a save's rename would replace, not
whatever that entry points at, so a `--snapshot-path` that is a symlink is accepted and
replaced, exactly as the rename replaces it. It writes nothing else there and removes
nothing it did not create, so what it cannot see it says nothing about: a disk that
fills later, a directory changed after startup, and a permission that blocks the removal
of the existing snapshot's own entry, which is what the rename needs and what no file
created beside it can answer for. A path like that starts, and then every save fails,
leaving the snapshot it could not replace intact. At `--snapshot-interval 0` the check
does not run, so a path no save could write -- a directory that does not exist, say --
starts without complaint and fails at the first save there is, which is the one a clean
stop makes on the way out: the failure is logged, and the exit status stays `0`. What
such a failure reports is bounded: the first one is logged with its traceback, and after
that a single line says how many times in a row it has now failed, once per hundred.
That bound is not cosmetic. Logging writes to standard error with a blocking write, and
this server has one thread, so a reader that stops — a stalled collector, a pipeline
whose far end died — can fill its buffer and park the loop inside the tick, after which
nothing is served and nothing more is logged. A failure that repeats on a timer is the
one thing that can fill that buffer on its own, and it no longer does. The residual
stands: a buffer filled from elsewhere still parks the next write, and closing that
needs either a descriptor this process does not own or a second thread. A save writes a
temporary file beside the snapshot, named after it, and renames it into place; a file
left under that name, as a process killed mid-save leaves one, is never read or removed,
and every start over the same path names it in a warning, so long as the directory can
be listed. `--snapshot-interval` (seconds, default `60`) and `--expiry-sweep-interval`
(milliseconds, default `100`, the same span as the run loop's own `select()` timeout of
`0.1` seconds) both follow the same rule as the caps above: `0` turns the periodic task
off, and a negative value is refused at the CLI and again in `Server.__init__`. So is a
value above `2**63 - 1`, the largest number anything in this project is allowed to hold:
it is refused at both doors. That ceiling is a convention and not the arithmetic's own
limit, which is far higher -- the sum that schedules an interval does not fail until the
number is past what a float can hold, around `10**308` -- and it is there so that an
absurd value is a one-line refusal at startup and not an `OverflowError` one tick later.
`--ignore-snapshot` takes no value of its own: passed, the file at `--snapshot-path` is
never read, whether or not it is readable, so a good snapshot is discarded as readily as
a corrupt one. The server starts with an empty keyspace, and the file itself stays on
disk untouched until the next save writes the keyspace as it then stands over it: the
next periodic one, or the one a stop signal triggers on the way out, whichever comes
first. Together with `--snapshot-interval 0` neither comes: no save runs at all, not
even on the way out, and the file is left as it was. That pairing is the only one that
leaves the file alone. `--snapshot-interval 0` by itself still saves on a clean stop,
and this flag with a non-zero interval still saves on a clean stop and at the interval.
The flag is the escape hatch for a file that refuses to load, and the pairing is the
form of it that keeps that file in place. Left set permanently -- in a unit file, say --
it starts every restart with an empty keyspace, not just the first, because it never
reads the file.

**A clean stop saves, and a crash can still lose recent writes.** On `SIGINT` or
`SIGTERM` the server writes a snapshot of the keyspace as it stood when the loop
stopped, and then spends up to `--shutdown-drain-timeout` seconds sending the replies
already queued for its clients before it exits; a save that fails is logged and the stop
carries on, and the one pairing of flags that skips the save is the one described with
the persistence flags above. What a crash leaves is the window between two saves: a
`SIGKILL`, a power cut or a process that dies mid-run loses whatever the last save did
not hold. The interval is counted from the moment a save finishes rather than from the
moment it starts — the rule `CONFIG GET save` publishes, and the reference's own — so
the window between two saves is the interval plus the duration of the one before it. At
the default and the snapshot size measured below that is a little over sixty seconds; on
a keyspace large enough for a save to outlast its own interval it is the interval plus
the save. Lowering `--snapshot-interval` bounds how much a crash can lose, down to that
floor. The save also blocks: it runs on the one thread that answers commands, so while a
snapshot is being serialized and written the server answers nobody. Real Redis forks and
lets a copy-on-write child pay that cost, which is the right answer at scale and the one
given up here — every non-tearing alternative needs a point-in-time view of the
keyspace, and a pure-Python copy, cheaper in memory than it sounds (`docs/DESIGN.md`
measures it), still leaves every list to copy element by element and the encode on this
one thread, so the pause is priced and published rather than hidden. A snapshot of
100,000 keys — 16-byte keys and 100-byte values — costs about 62.3 ms to serialize and
about 80.4 ms to deserialize, a 12.7 MiB payload, and about 117.2 MiB of peak resident
memory in a process that builds the keyspace, serializes it and then decodes the payload
back into a second copy (`resource.getrusage(RUSAGE_CHILDREN).ru_maxrss`). The fixture
is stated because the payload is a function of it, and the figures come from measuring
the tree that ships.

**Two more flags cover the way out and the way in.** `--shutdown-drain-timeout SECONDS`
(default `5`) bounds the drain, which is the part of the way out that waits on clients,
and not the whole of it: the wait for the loop to notice the signal, up to one
`select()` timeout, comes first, and so does the snapshot save, which costs what the
keyspace costs and which this flag does not limit. On `SIGINT` or
`SIGTERM` the loop stops, and then, before anything is torn down: the listening socket
leaves the select set, so nothing further is accepted, though the socket itself stays
open until the teardown and a replacement server cannot bind the port while this process
is still running; a snapshot is saved; and the server spends up to SECONDS sending the
replies already queued for clients, then exits whether or not the kernel took all of
them. The save comes before the drain on purpose. A client that never reads holds the
drain to its whole timeout, and an operator who gives up and sends `SIGKILL` should not
find that the snapshot was the thing waiting behind it. It runs whether or not
`--snapshot-interval` is `0`: that interval schedules periodic saves, and a save on the
way out is a different thing, which is also why `CONFIG GET save` can answer an empty
string from a server that will still write a snapshot when it is stopped. The one
pairing of flags that skips it is the one described with the persistence flags above,
where the file is being left alone on purpose. During the drain the server keeps reading
its clients and dispatches nothing. Stopping reading looks like the way to keep the
drain finite, and it is the wrong one: a request left unread in a socket's receive queue
turns the close that follows into a reset, and a reset discards the closing socket's own
send queue, which is the replies this process handed to its kernel and the kernel had
not yet put on the wire. Replies the peer had already received are not taken back; the
tail that never left is what goes. What keeps the drain finite is that no command runs
and so no new reply is queued, while inbound bytes are read and thrown away: the receive
queue is empty at the close, so the close is orderly and the kernel goes on sending what
it holds. The deadline is checked between passes of the loop, so the real bound is
SECONDS plus one `select()` timeout, and the drain ends in one log line however it ends,
even when a pass raises. The line carries two counts, connections closed while they
still owed bytes and connections still owing bytes when the drain ended, and it is a
warning if either is non-zero and informational if both are zero. The informational one
is not shown in a default run: the server sets up no logging, so Python's fallback
prints warnings and above only, and a drain that handed everything to the kernel prints
nothing while one that lost replies prints its warning.

`--shutdown-drain-timeout 0` reads the opposite way from the limits it sits beside. For
`--output-buffer-limit`, `--max-value-size`, `--max-multibulk` and `--max-connections`,
`0` removes the limit, so a check that closes or refuses something stops doing it and
turning it off is permissive. This value is a time allowance and what it allows is the
drain, so `0` is no time at all, and means no drain: turning it off is restrictive.
(`--port 0` and the two intervals read in neither way: the kernel chooses a port, or a
periodic task is switched off, and none of the three is a limit.) What is lost is the
part of the replies the kernel will not take in the one best-effort flush each
connection gets as it is closed, which still runs, and nothing after it does. No read
pass runs either, so a request that arrives at any time after the loop's last pass, the
whole of the snapshot save included, is still unread when its connection closes, and the
close then resets it and discards what the kernel was holding unsent as well. An
operator who reads the limits correctly and generalises will set this to `0` expecting
an unbounded wait, and lose queued replies on every restart. A negative value is
refused, and so is one above the same `2**63 - 1` ceiling the two intervals have.

A client that finishes sending and keeps reading, a socket half-close, is not a dead
peer during the drain. End of input on a connection that still owes bytes does not close
it: reading from that connection stops, because end of input stays readable and would
otherwise wake every pass of the loop, and the drain keeps sending until the connection
is drained or the deadline arrives. End of input on a connection that owes nothing still
closes it, because there is nothing for the close to discard. Outside the drain nothing
changed: end of input with replies queued closes the connection and discards them after
the one best-effort flush, as the reference does.

The drain has one limit worth knowing: a client that is still sending when it ends can
still lose the tail of its replies. The drain finishes as soon as the last byte reaches
the kernel, which on loopback is long before a slow client has read it, and handed to
the kernel is not on the wire: the kernel can still be holding the tail unsent. A
request arriving after the final read pass leaves unread bytes at the close, which
resets the connection and discards what the kernel was holding. What the peer had
already received stays received. A client that sends and then stops is unaffected. At
`--shutdown-drain-timeout 0` the limit begins earlier, because no pass of the drain
reads at all: the last read is the main loop's, so a request arriving at any point after
it, throughout the save, is unread at the close. This is a limitation of the design, and
a half-close from the server's side does not remove it: with a request unread and
replies still held by the kernel, a close after `shutdown(SHUT_WR)` lost as much as a
plain close, and both ended in a reset at the client. What does work is a
close that waits, either for the kernel's own send queue to empty or for the peer's end
of input. The first is a question put to the kernel that this server does not ask, and
it ties the exit to the pace of the slowest reader, up to the deadline; the second needs
the client's cooperation and has no bound of its own against one that never gives it.
This server does neither. `docs/DESIGN.md` has the reasoning.

`--max-connections COUNT` (default `1024`) closes, without a reply, a connection that
arrives while COUNT clients are already connected; `0` means no limit, and nothing is
refused. The count is the connection set's own length, and a refused connection never
enters it, so `INFO`'s `connected_clients` reports only admitted clients. A refusal is a
close with nothing written, because an error reply would be a `send()` from the accept
callback, on a socket nothing tracks, on the one thread there is, for a client that by
construction is not being kept. The server says why on its own side instead, and bounds
how much it says: the first refusal of an episode is logged as a warning, the rest at
debug level, and a short line is written once per a fixed number of refusals so that a
run that never ends still leaves a trace. A new episode begins only after a gap with no
refusals at all. A close does not begin one, because a freed slot is what a retrying
client is waiting for: with a saturated cap whose slots turn over, a close comes between
every pair of refusals, and every refusal would warn. The volume has to be bounded
because a client retrying in a loop is refused as fast as it can connect, and logging
writes to a standard error whose write blocks the one thread this server has. A cap
under 50 refuses some of the clients of `redis-benchmark -c 50`, the standard
invocation, and turns it into a test of the cap, which is why the default is well clear
of that; the flag itself takes any non-negative value. A cap of exactly 50 is enough,
because a count equal to the cap is full: it admits 50 clients and refuses the 51st,
since the question is whether there is room for one more — the opposite of every byte
limit above, which compares a quantity already held against a ceiling it may legally
reach exactly, and why this looks like an off-by-one until that is said. The cap bounds
how many clients there are, not what any one of them can hold: every limit above is
still per element, per command or per connection, and the cap does not turn any of them
into a bound on memory.

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
and leaves the server and every other connection running. When that error surfaces partway
through a pipelined batch, whatever already parsed out of the same read is answered first,
the error is queued after, and the connection closes — in that order, every time. An unknown
command answers with an error rather than a disconnect, so a client's own capability probes
fail harmlessly.

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
server.py       entry point, listener, signals, shutdown drain, connection cap, dispatch
event_loop.py   selectors readiness dispatch (on_readable / on_writable / on_accept)
connection.py   per-connection socket, read/write buffers, lifecycle
resp.py         RESP2 parser and serializer
store.py        keyspace, expiry index, pending-effects queue
persistence.py  versioned snapshot format, atomic save/load
ratelimit.py    docstring only; rate limiting is not built
replication.py  docstring only; replication is not built
commands/       __init__.py, registry.py, server.py, string.py and list.py; twenty-six commands total
tests/          pytest suite, run in CI against Python 3.11 and 3.13
```

[Design notes](docs/DESIGN.md) cover why each of these is shaped the way it is, and the
deliberate differences from real Redis.
