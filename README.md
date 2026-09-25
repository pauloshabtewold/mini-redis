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
allocator. `ratelimit.py` and `replication.py` are still declared and empty.

**Four more flags cover persistence and the active sweep.** Persistence is on by
default. `--snapshot-path` names the file a snapshot is written to and read back from
and defaults to `./dump.mrdb`, resolved against the directory the server was started in;
a file that is present but will not decode refuses startup rather than starting empty
over it, and so does anything else at that path that is not a regular file -- a
directory, a named pipe. Whenever saving is on, the startup check tries the write it
guards against rather than only inspecting the path: it creates and removes a temporary
file shaped like the one a save writes, in the snapshot's own directory, so a directory
that cannot take that file, a name the filesystem will not accept, and a directory that
will not let the file be removed are all refused before the server starts, and an
existing snapshot whose file flags would block a rename over it is refused too. What it
judges at that path is the entry a save's rename would replace, not whatever that entry
points at, so a `--snapshot-path` that is a symlink is accepted and replaced, exactly as
the rename replaces it. It still cannot see a disk that fills later or a directory
changed after startup. A save writes a temporary file beside the snapshot, named after
it, and renames it into place; a file left under that name, as a process killed mid-save
leaves one, is never read or removed, and every start over the same path names it in a
warning, so long as the directory can be listed. `--snapshot-interval` (seconds, default
`60`) and `--expiry-sweep-interval` (milliseconds, default `100`, the same span as the
run loop's own `select()` timeout of `0.1` seconds) both follow the same rule as the
caps above: `0` turns the periodic task off, and a negative value is refused at the CLI
and again in `Server.__init__`. So is a value too large to put on a clock: above `2**63
- 1` the arithmetic that schedules an interval cannot convert it, and it is refused at
both doors rather than left to surface as an `OverflowError` one tick later.
`--ignore-snapshot` takes no value of its own: passed, the file at `--snapshot-path` is
never read, whether or not it is readable, so a good snapshot is discarded as readily as
a corrupt one. The server starts with an empty keyspace, and the file itself stays on
disk untouched until the next save writes the keyspace as it then stands over it --
unless `--snapshot-interval` is 0, in which case nothing ever overwrites it. It is the
escape hatch for a file that refuses to load. Left set permanently -- in a unit file,
say -- it starts every restart with an empty keyspace, not just the first, because it
never reads the file.

**A clean stop can still lose recent writes.** Snapshots save on `--snapshot-interval`
and not on the way out, so a `SIGINT` or `SIGTERM` can lose one interval's worth of
writes plus however long the last save took. The interval is counted from the moment a
save finishes rather than from the moment it starts — the rule `CONFIG GET save`
publishes, and the reference's own — so the window between two saves is the interval
plus the duration of the one before it. At the default and the snapshot size measured
below that is a little over sixty seconds; on a keyspace large enough for a save to
outlast its own interval it is the interval plus the save. Lowering
`--snapshot-interval` bounds how much a restart can lose, down to that floor. The save
also blocks: it runs on the one thread that answers commands, so while a snapshot is
being serialized and written the server answers nobody. Real Redis forks and lets a
copy-on-write child pay that cost, which is the right answer at scale and the one given
up here — every non-tearing alternative needs a point-in-time view of the keyspace, and
a pure-Python copy, cheaper in memory than it sounds (`docs/DESIGN.md` measures it),
still leaves every list to copy element by element and the encode on this one thread, so
the pause is priced and published rather than hidden. A snapshot of 100,000 keys —
16-byte keys and 100-byte values — costs about 59.9 ms to serialize and about 75.2 ms to
deserialize, a 12.7 MiB payload, and about 116.2 MiB of peak resident memory in a
process that builds the keyspace, serializes it and then decodes the payload back into a
second copy (`resource.getrusage(RUSAGE_CHILDREN).ru_maxrss`). The fixture is stated
because the payload is a function of it, and the figures come from measuring the tree
that ships.

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
server.py       entry point, listener, signal handling, dispatch
event_loop.py   selectors readiness dispatch (on_readable / on_writable / on_accept)
connection.py   per-connection socket, read/write buffers, lifecycle
resp.py         RESP2 parser and serializer
store.py        keyspace, expiry index, pending-effects queue
persistence.py  versioned snapshot format, atomic save/load
commands/       __init__.py, registry.py, server.py, string.py and list.py; twenty-six commands total
tests/          pytest suite, run in CI against Python 3.11 and 3.13
```

[Design notes](docs/DESIGN.md) cover why each of these is shaped the way it is, and the
deliberate differences from real Redis.
