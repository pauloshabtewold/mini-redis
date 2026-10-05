# mini-redis

A Redis clone in Python: a single-threaded event loop speaking the RESP2 wire protocol
over TCP, with **zero runtime dependencies**.

## Status

This is `v1`, and it is finished. The server accepts connections, parses RESP2 correctly
— multibulk arrays and inline commands, well-formed or not — and its command set is
complete: twenty-six commands — `PING`, `ECHO`, `HELLO`, `SET`, `GET`, `DEL`, `EXISTS`,
`TYPE`, `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `INCR`, `DECR`, `LPUSH`,
`RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `DBSIZE`, `KEYS`, `FLUSHALL`, `INFO`,
`CONFIG`. Keys expire lazily, on lookup, and on a sampled active sweep:
`--expiry-sweep-interval` bounds how often that sweep runs, not how long any one expired
key stays resident. Each pass samples a few of the keys that carry a TTL and deletes the
expired ones, so nothing bounds when a particular key is reclaimed — which is why expiry
on lookup is still the load-bearing half: a key you ask for is never served stale,
whatever the sweep has reached.

Two parts of the design are not in it. Rate limiting and replication are specified in
full and deliberately unbuilt, and [Designed and not built](#designed-and-not-built)
says what that leaves behind in the code. [Benchmark](#benchmark) has what the server
costs against real Redis, and [Limits](#limits) is the long list of what it does and
does not do under pressure.

## Benchmark

I took these numbers with `redis-benchmark 7.2.7`, from Homebrew, against this server
and against a private `redis-server 7.2.7`, each on a port of its own, on one Apple M2
running macOS 14.2.1 over loopback, with this server under CPython 3.13.1.

```
$ redis-benchmark --version
redis-benchmark 7.2.7
```

The reference ran with `--save ""` and `--appendonly no`, from a scratch directory. This
server ran with `--snapshot-interval 0 --log-level WARNING`, also from a scratch
directory. The invocation was the same against both, 100,000 requests per test over 50
connections:

```bash
redis-benchmark -h 127.0.0.1 -p <port> -n 100000 -c 50 -P 1 -q \
  -t ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,lrange_100,lrange_600
```

`-q` prints a rate and nothing else, so the table below comes from the same command
without `-q`, the only form that reports the latency distribution; each row's rate and
p99 come from the same run. **Each row is one run, and one run is worth less than it
looks on this machine.** Re-running the whole set moved individual rates by as much as
twenty per cent and some p99 figures by half again -- `PING_MBULK` came back at 65,147
and 76,161 against the 84,962 below, and the reference's `LRANGE_600` p99 at 4.255 ms and
3.783 ms against 2.743 ms -- on a laptop carrying other work, which is what a laptop
always is. What survived every repetition is the shape rather than the cell: the median
share of native stayed within about a point of the figure quoted here, every test cleared
the threshold with room to spare, and this server's p99 stayed two to three times the
reference's on the small commands. Read a row as the order of magnitude it establishes,
not as a number to compare against your own hardware.

The list is pinned because the tool's default run stops at
`SADD`, the first command in its sequence this server does not implement (see
[Limits](#limits)). Whether all ten tests actually ran was checked by reading the labels
on the result lines and not the exit status, for the reason in the third finding below.

| test | this server, requests per second | redis-server 7.2.7, requests per second | % of native | this server, p99 (ms) | redis-server 7.2.7, p99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| PING_MBULK | 84,962 | 115,875 | 73.3% | 1.247 | 0.567 |
| SET | 61,805 | 117,786 | 52.5% | 1.887 | 0.455 |
| GET | 74,349 | 106,724 | 69.7% | 1.503 | 0.927 |
| INCR | 69,541 | 116,414 | 59.7% | 1.511 | 0.679 |
| LPUSH | 70,922 | 109,170 | 65.0% | 1.351 | 0.663 |
| RPUSH | 68,166 | 113,250 | 60.2% | 1.503 | 0.487 |
| LPOP | 73,421 | 106,610 | 68.9% | 1.423 | 0.783 |
| RPOP | 77,700 | 116,279 | 66.8% | 1.247 | 0.575 |
| LRANGE_100 | 25,202 | 46,512 | 54.2% | 3.927 | 1.479 |
| LRANGE_600 | 8,382 | 13,240 | 63.3% | 11.359 | 2.743 |

With `-P 16`, the same command and the same list, so sixteen requests are in flight on
each connection, `-q` stays on and there is no p99 to report:

| test | this server, requests per second | redis-server 7.2.7, requests per second | % of native |
| --- | ---: | ---: | ---: |
| PING_MBULK | 336,700 | 1,428,571 | 23.6% |
| SET | 200,401 | 1,063,830 | 18.8% |
| GET | 253,807 | 1,162,791 | 21.8% |
| INCR | 194,553 | 1,190,476 | 16.3% |
| LPUSH | 204,499 | 980,392 | 20.9% |
| RPUSH | 208,768 | 934,579 | 22.3% |
| LPOP | 249,377 | 840,336 | 29.7% |
| RPOP | 230,415 | 961,538 | 24.0% |
| LRANGE_100 | 41,597 | 88,731 | 46.9% |
| LRANGE_600 | 9,245 | 15,482 | 59.7% |

These are two readings of one server, and only the second is the cost of the design. At
`-c 50 -P 1` each client sends a request and waits for the reply, so both servers spend
most of every request waiting on the loopback round trip, and the ratio measures the
network about as much as it measures the server. This server ran at 52.5% to 73.3% of
native there, median 64.1%, and that figure overstates how close it is. With `-P 16` the
clients keep the server busy and the ratio is what a single-threaded Python interpreter
costs per command: the eight small-reply commands, `PING_MBULK` through `RPOP`, ran at
16.3% to 29.7% of native, median 22.1%, and the two `LRANGE` tests at 46.9% and 59.7%.
Take the `-P 16` figure as what the design costs. The `-P 1` figure is what 50 clients
that do not pipeline see over loopback.

I fixed the ship threshold before measuring: 8% of native or better ships as it is, and
a miss would have bought one profiling and optimisation session. Both readings clear it
by a wide margin, so this is the version that shipped and no optimisation pass was run.
I did not profile either: `py-spy` needs root on macOS, nothing in this project runs as
root, and the profile existed only for a result that missed.

Three facts make these numbers reportable, and each is stated because its absence would
change them. The snapshot interval was `0` for every run of this server, so no periodic
save fell inside a measurement: a save blocks the one thread that answers clients, and a
run that crossed one would carry an unattributed stall inside its p99. The snapshot path
was still empty after the runs, which is how I know none fired. The rate limiter is not
built, so it was off by construction and not by configuration; a limiter that was on
would have tripped under `-c 50` at once and the benchmark would have measured its
rejection path. And the log level was `WARNING`, which leaves out the line this server
writes at `INFO` for each connection it opens and closes.

## Three findings

Each of these came from testing something that looked fine, and none of them is visible
in the code unless it is written down here.

**A length-prefixed format does not make a checksum redundant.** The argument for
leaving one out is that every length prefix is a bounds check, so a corrupt snapshot
fails at a known offset. Truncation does: it is refused at every offset. A flipped bit
mostly does not. I ran 3,000 single-bit-flip trials, one random bit each, against the
shipping encoder: a 2,036-byte snapshot of 41 keys holding a list and a TTL, 16,288 bit
positions. Decoded with the trailer recomputed over the corrupted bytes, which is what a
length-prefixed format with no checksum amounts to, **81.0% of the flips were silently
accepted as a snapshot holding different data** and only 19.0% were caught at all -- 475
of those by a length prefix running off the end, 48 by a type byte that named no kind,
and 5 by a key arriving twice. With the CRC32 trailer in place, all 3,000 were refused,
100%. The reason is that most bytes in a snapshot are payload and not structure, so a
flip inside a key, a value or an expiry passes every bounds check there is.
`HELLOWORLD` came back as `HELLOWOVLD`; a key named `beta` came back as `beua`, so
`GET beta` answered nil and a
key nobody wrote existed; an expiry of 1700000000000 came back as 1699995805696.
`tests/test_persistence_properties.py` pins both sides, and `docs/DESIGN.md` has the
other half of the picture, the count and version fields, where the length prefixes do
catch every flip.

**A bare `send()` is not a write path.** Against a copy of this server whose only write
path was one `send()` with nothing behind it to catch what the kernel would not take, a
long pipeline of `ECHO`s written before the client read a byte lost a large share of its
replies, a different share on every run. One reply arrived cut in half, and almost every
later one was never sent at all, because a `send()` on a full socket raises
`BlockingIOError` and writes nothing, so what is lost is a whole reply. Nothing about
that is visible until a client's burst outruns its own reading, which is what any
pipelining client library does. The write path here queues what the kernel will not take
and sends it on the next writable event; `docs/DESIGN.md` has the counts and how they
were taken.

**`redis-benchmark` drops an unrecognised `-t` name and still exits 0.** Measured on
7.2.7, the version the numbers above were taken with: `-t nosuchtest` printed nothing at
all, wrote nothing to standard error and exited 0; `-t set,nosuchtest` ran `SET` and
dropped the rest without a word, exit 0; and the pinned list with `lrange_100` misspelt
as `lrange100` ran nine tests, printed a table that looks complete, and exited 0 with the
`LRANGE_100` line simply absent. A missing test is invisible in a table of results,
so neither the exit status nor the number of lines is a check, and I read the labels.
The rule has three steps and they run in this order. Split the output into lines on `\r`
as well as `\n`, because with `-q` each result is preceded by carriage-returned progress
repaints. Discard every line that begins `LPUSH (needed to benchmark LRANGE)`: the tool
prints it to seed the list for the two `LRANGE` tests, it contains the word `LPUSH`, and
it appears whether or not the `LPUSH` test ran. Then require each of the ten names to
begin one of the remaining lines, followed by `:` or ` (`, because the two `LRANGE`
labels carry a parenthetical.

On 7.2.7, at the pinned request count, the real run reports nothing missing, with 8
lines carrying the seeding label. With `lpush` dropped from the list, so nine names, the
tool still exits 0 and the rule reports `LPUSH` missing, while a plain search for
`LPUSH` anywhere in the output and a rule that discards exactly one seeding line both
report nothing missing. The number of seeding lines is not fixed, because the tool
repaints that line while it seeds, so how many there are depends on how long seeding
takes; at a small request count there can be just one, and the singular rule and the
plural one agree. I verified the rule at the pinned count for that reason. Deleting the
`\r` characters instead of splitting on them failed a correct run at small request
counts on 6.2.14 and did not on 7.2.7 at 1,000, 2,000 or 100,000, so the split is kept
as the reading that is right on both.

## Designed and not built

Two parts of the design are specified in full and deliberately unbuilt: a per-connection
sliding-window rate limiter, and leader-follower replication. `ratelimit.py` and
`replication.py` each hold a docstring and nothing else, and that is a decision and not
an omission. I cut both on purpose, at the point where stopping left nothing half-built
in the tree, and I do not intend to build them; this is `v1` and there is no later
version in which they are finished.

The limiter was to count requests per connection over a window, with a deque of
timestamps popped from the left so that a check costs the same however busy the
connection is, and to answer an over-limit request with an error while leaving the
connection open. It would have been off by default, because a limit switched on trips
under `redis-benchmark -c 50` immediately and the benchmark would then measure the
rejection path. It limits a connection and not an address, so a client could reset its
budget by reconnecting; I would have documented that rather than engineered it away.
Replication was to be asynchronous: a follower connects, is sent the leader's keyspace
in the snapshot format as one bulk string, and then receives a live stream of what each
handler reports as its effects, plus the `DEL`s that expiry produces, in execution
order. A follower would have applied that stream through its own dispatcher, and the
leader's link to its own leader would have been a third kind of connection, exempt from
the limits a client is under.

Because the design was written before it was cut, three things in the code are present
and cannot be reached, and I would rather say so than leave them to be found.

`Connection` knows three roles, `client`, `follower` and `leader_link`, and the tests
construct a connection with the second; nothing in the server does. Every accepted
connection is a `client`, and no flag sets either of the other two: the flag that would
have started a server as a follower was never written. The extra values are there
because a check written against two roles misclassifies the link to a leader as an
ordinary client, and with nothing to exempt they exempt nothing. The `rate_limit_state`
and `replication_state` slots on `Connection` are the same kind of thing: they hold
`None` and nothing reads them.

Every command handler returns its reply together with an effects list, the commands a
follower would need replayed. A write that takes effect fills it in and everything else
returns it empty. `server.py` reads the list and throws it away, together with the
store's own queue of expiry deletions, which it empties after each command only so that
the queue cannot grow. Nothing consumes either. The shape stayed because the alternative
was editing every handler a second time on the day something did, and because a key that
expires on a `GET` has to produce a `DEL` with no write command in the request to carry
it; `docs/DESIGN.md` has the reasoning.

`PEXPIREAT` is registered for a stream that does not exist. It works as an ordinary
command, and it is also the absolute form that `EXPIRE` and `PEXPIRE` report as their
effect, so that a follower applying it later would compute the same deadline. The
follower would have applied it through the same dispatcher a client uses. With no
stream, that effect goes nowhere.

## Alternatives I rejected

Three questions come up whenever this design is described. Each has an answer that is a
choice and not an oversight.

**Why not several processes behind `SO_REUSEPORT`?** It is the obvious way past the GIL
and it is perhaps twenty lines: start several processes and let the kernel spread
incoming connections across their listening sockets. I rejected it because every process
would hold its own keyspace. A `SET` on a connection the kernel sent to one process
would be invisible to a `GET` on a connection it sent to another, so the result is
several unrelated servers sharing a port and not one faster server, and making them
agree needs cross-process coordination, which is a different and much larger project
than this one. One thread also makes every command atomic by construction: `INCR` and
`SET ... NX` need no lock because nothing else runs while they do. The price is the
`-P 16` figure above, and I chose to publish it over buying it back with processes that
no longer share anything.

**Why does a snapshot block the loop, and why not `fork()`?** Real Redis calls `fork()`
and lets a copy-on-write child serialize a point-in-time view while the parent keeps
answering. That is the right answer at scale and the one this design gives up. I
rejected it on cost and not on correctness: it needs a process boundary and a way to get
the result back to the parent, and it interacts badly with a single-threaded parent's
socket state, because the child inherits every open client connection and the listener.
The alternatives that stay in one process and do not fork tear: serializing in chunks
that yield between them lets a write land mid-save and produces a file that decodes
cleanly into a keyspace that never existed. So the pause is accepted whole and priced,
and the persistence paragraphs under [Limits](#limits) give what a save costs on a
stated fixture. `docs/DESIGN.md` has the argument for why even a cheap in-process copy
does not remove the pause.

**Why would every reconnect cost a full sync, and why not `PSYNC`?** Replication is the
unbuilt half, so this is a decision about a design that never ran. A follower that lost
its connection to the leader would have asked for the whole keyspace again.
`PSYNC`-style partial resync lets it say where it stopped instead, and it needs the
leader to keep a backlog of the stream and to do offset bookkeeping so that it can
answer. I rejected it because that machinery demonstrates nothing the full sync does
not: the full sync already shows an architecture in which a follower converges on its
leader after any interruption, and partial resync would only make reconnecting cheaper.
The price would have been that every reconnect costs the leader a serialization and the
follower a decode of the whole keyspace.

## Limits

One consequence of the command list under Status is worth knowing before you reach for a
client library. `INCRBY` and `DECRBY` are not among them, and `redis-py` defines
`.incr()` and `.decr()` as aliases for them — so `r.incr("k")` puts `INCRBY` on the wire
and comes back `ERR unknown command 'INCRBY'`, even though this server implements `INCR`
and answers it correctly. `r.execute_command("INCR", "k")` reaches it. `redis-py`'s
`.lpop(name, count)` has the same shape: it sends `LPOP name count` on the wire, and
this server's exact two-argument arity for `LPOP` answers a wrong-number-of-arguments
error rather than the two-element reply real Redis would give. The one that costs the
most is `pipeline()`, whose `transaction` argument defaults to true: the default call
wraps the batch in `MULTI`/`EXEC`, neither of which this server implements, so it fails
on `EXEC`. `r.pipeline(transaction=False)` sends the same commands and works for a batch
of some tens of thousands of small commands. It packs the whole batch and writes it
before it reads any reply, which is the client the pause described under the water marks
below leaves hanging, so a batch of hundreds of thousands can hang it at the shipped
defaults; `--write-buffer-high-water 0` is what lets it complete. `redis-benchmark`'s
default run completes `PING`, `SET`, `GET`, `INCR`, `LPUSH`, `RPUSH`, `LPOP` and `RPOP`,
and exits at `SADD`, the first command in its sequence this server does not implement at
all.

A `redis-py` client that asks for RESP3 by default, as the one this suite runs against
does, sends `HELLO 3` as it connects, and this server answers `NOPROTO unsupported
protocol version`, so every call fails before it reaches any of the commands above
unless the client is built with `protocol=2`, which everything above about `redis-py`
assumes.

**Nothing bounds how much memory a client can use.** There is no cap on key count, no
cap on total keyspace size, and no eviction policy to fall back on if there were — a
client with nothing but `SET` can grow the process until the host runs out of memory.
The read buffer is uncapped in size too: an unterminated command grows it for as long as
a client keeps sending, which at least costs that client a byte per byte, and a line
with no terminator yet has no declared length for either size cap below to compare
against anything. `--incomplete-command-timeout SECONDS` bounds that case by time
instead: a connection that has held a command it has only partly sent for longer than
SECONDS is closed, without a reply, and the close is logged as a warning. It defaults to
30, and 0 turns the check off. The clock starts when the command begins and is not
restarted by its later bytes, because a client sending one byte a second is the client
the flag is for; the price is that a command which honestly takes longer than SECONDS to
arrive, a large value over a slow link, is closed however steadily it comes. A long
pipeline is not penalised, since each command that completes starts a fresh clock for
whatever the connection holds after it, and a connection holding nothing is never closed
however long it idles. The clock also stops while the high-water mark described below
has a connection paused, because the server is what stopped reading it, and starts over
from zero when reading resumes. A line that *has* ended is bounded even with no flag of
its own: an inline command is refused past 64 KiB, the reference's own
ceiling, because it is complete the moment its newline arrives and nothing later can
reach it. `--max-value-size BYTES` refuses a single declared bulk element — including
the command name and any key, not only what a human would call the value — that
declares more than BYTES, before a body byte of it is read; it defaults to 64 MiB, and 0
turns the check off. `--max-multibulk COUNT` refuses a command declaring more than COUNT
elements; it defaults to 1,048,576, and 0 turns it off too. Neither bounds anything in
aggregate — each is a ceiling on one element, or on one command's element count, never on
a connection's traffic as a whole.
Queued replies are the cheapest way to spend this server's memory: every reply is
buffered whole, and the value a run of pipelined `GET`s all name was stored once, so a
few kilobytes of request can commit gigabytes (`docs/DESIGN.md` has the arithmetic).
`--write-buffer-limit BYTES` closes a connection whose queued replies still exceed BYTES
once the kernel has taken what it will; it defaults to 32 MiB, and 0 turns it off. The
paragraphs after this one say what that default is and costs. `KEYS *` has the same
shape: it materialises its whole reply as one array holding every key in the keyspace,
with no cap of its own, so the reply is bounded only by however large the keyspace has
already grown — real Redis has the same property and documents it. `INFO`'s
`used_memory` is peak resident memory rather than current on any platform without
`/proc`: it reads `/proc/self/statm` where that exists and falls back to
`resource.getrusage(...).ru_maxrss`, a high-water mark, so the two figures agree right
after a bulk load and diverge for a server that has since freed memory back to the
allocator.

**A queue of replies has two marks and a ceiling.** `--write-buffer-high-water BYTES`
(default 1 MiB) stops reading a connection while more than BYTES of replies are queued
for it, and `--write-buffer-low-water BYTES` (default 256 KiB) starts reading it again
once the queue has fallen to BYTES or fewer. There are two marks because one would pause
and resume a connection on every event, so a low-water mark that is not below the
high-water one is refused, at the CLI and by `Server.__init__`, unless the high-water
mark is 0. A high-water mark of 0 switches the pause off, so reading goes on however
much is queued and the low-water mark is never reached; with the pause on, a low-water
mark of 0 means resume only when the queue is empty. `--write-buffer-limit` is the
ceiling behind them. With the pause in force it is not what slows a client down: a
client that stops reading is paused long before it reaches 32 MiB, and what the limit
catches is a reply larger than it is, or the replies to one read's worth of pipelined
requests, which the pause cannot unqueue once they are parsed. The 32 MiB default is a
choice, and a departure from the reference, which leaves an ordinary client's output
buffer unlimited. This server does not: nothing else here bounds what a few kilobytes
of pipelined requests can queue, and a bounded queue is worth being different over.
`--write-buffer-limit 0` is the reference's behaviour.

**A value can be stored and not read back.** `--max-value-size` defaults to 64 MiB and
`--write-buffer-limit` to 32 MiB, and nothing relates the two, so a `SET` of a value
between them is accepted and the `GET` that follows closes the connection: the reply is
larger than the limit, and the limit is judged after the send, on what the kernel would
not take. The value stays stored. A hard limit does the same in the reference, so the
choice here is to say so and leave both numbers alone; `docs/DESIGN.md` has the sizes
measured against this server and against the reference. To read back every string value
the size cap admits, set `--write-buffer-limit` above `--max-value-size`, or to 0. That
does not reach a list, whose elements are each held to the size cap and whose length is
not capped: `LRANGE 0 -1` queues the whole list as one reply, judged whole against the
limit, so a list of elements that are all far under the cap can be stored and not read
back at any finite limit, and a `KEYS` reply is judged the same way. Only 0 reads every
one of them back.

**A client that stops reading is paused, not closed.** The pause is what keeps it from
ever approaching `--write-buffer-limit`, so that limit never fires for it;
`--incomplete-command-timeout` is suspended for every paused connection, whatever it has
or has not sent, so that flag does not bound it either; and it holds what is already
queued for it, at least the high-water mark's worth plus whatever the one batch that
crossed the mark queued, until it disconnects. That is bounded: per connection by the
mark plus that batch, which the limit in turn caps at the limit plus one reply while the
limit is on, and over all connections by `--max-connections`. It is also a divergence
from the reference, which keeps reading such a client and closes it at the hard limit
configured for ordinary clients, if there is one. It is accepted and not fixed: bounding
how long a connection may stay paused would be a control of its own, and
`--incomplete-command-timeout` is not it, since it is suspended for a paused connection
whether or not that connection holds a half-sent command. The pause has one more cost,
for a different client: one that writes every request before it reads any reply, as
`redis-py`'s `pipeline()` does with the batch it holds, hangs once the requests it has
still to send no longer fit in what the two kernels will hold, and nothing closes it.
Where that point falls moves from run to run, and with `--write-buffer-high-water 0` the
pause is off and the same client is answered in full. `docs/DESIGN.md` has the
measurements and the reasoning.

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
nothing is served and nothing more is logged. A failure that repeats on a timer would
write a traceback every interval for as long as the server ran, and the bound above stops
that one; it is not the only writer that can fill the buffer, and the connection lines
further down are the others. The residual is that a buffer filled from any of them still
parks the next write, and closing that needs either a descriptor this process does not own
or a second thread. A save writes a
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
the tree that ships. These figures are published here, and `docs/DESIGN.md` cites
them without repeating them.

**Two more flags cover the way out and the way in.** `--shutdown-drain-timeout SECONDS`
(default `5`) bounds the drain, which is the part of the way out that waits on clients,
and not the whole of it: the wait for the loop to notice the signal, up to one
`select()` timeout, comes first, and so does the snapshot save, which costs what the
keyspace costs and which this flag does not limit. On `SIGINT` or `SIGTERM` the loop
stops, and then, before anything is torn down: the listening socket leaves the select
set, so nothing further is accepted, though the socket itself stays open until the
teardown and a replacement server cannot bind the port while this process is still
running; a snapshot is saved; and the server spends up to SECONDS sending the replies
already queued for clients, then exits whether or not the kernel took all of them. The
save comes before the drain on purpose. A client that never reads holds the drain to its
whole timeout, and an operator who gives up and sends `SIGKILL` should not find that the
snapshot was the thing waiting behind it. It runs whether or not `--snapshot-interval`
is `0`: that interval schedules periodic saves, and a save on the way out is a different
thing, which is also why `CONFIG GET save` can answer an empty string from a server that
will still write a snapshot when it is stopped. The one pairing of flags that skips it
is the one described with the persistence flags above, where the file is being left
alone on purpose. During the drain the server keeps reading its clients and dispatches
nothing. Stopping reading looks like the way to keep the drain finite, and it is the
wrong one: a request left unread in a socket's receive queue turns the close that
follows into a reset, and a reset discards the closing socket's own send queue, which is
the replies this process handed to its kernel and the kernel had not yet put on the
wire. Replies the peer had already received are not taken back; the tail that never left
is what goes. What keeps the drain finite is that no command runs and so no new reply is
queued, while inbound bytes are read and thrown away: the receive queue is empty at the
close, so the close is orderly and the kernel goes on sending what it holds. A
connection the high-water mark had stopped reading is read again from the moment the
drain begins, for the same reason: whatever it had not read would still be in its
receive queue at the close. The deadline is checked between passes of the loop, so the
real bound is SECONDS plus one `select()` timeout, and the drain ends in one log line
however it ends, even when a pass raises. The line carries two counts, connections
closed while they still owed bytes and connections still owing bytes when the drain
ended, and it is a warning if either is non-zero and informational if both are zero.
Both are shown at the default `--log-level`, which is INFO, so a clean stop ends with a
line saying it was clean; `--log-level WARNING` leaves only the warning.

`--shutdown-drain-timeout 0` reads the opposite way from the limits it sits beside. For
`--write-buffer-limit`, `--max-value-size`, `--max-multibulk`, `--max-connections` and
`--incomplete-command-timeout`, `0` removes the limit, so a check that closes or refuses
something stops doing it and turning it off is permissive. This value is a time
allowance and what it allows is the drain, so `0` is no time at all, and means no drain:
turning it off is restrictive. (`--port 0` and the two intervals read in neither way:
the kernel chooses a port, or a periodic task is switched off, and none of the three is
a limit.) The two water marks read in neither way either, and each has a reading of its
own: `--write-buffer-high-water 0` disables the pause, so reading goes on however much
is queued, and `--write-buffer-low-water` is then unreachable, which is accepted and not
refused; with the pause on, `--write-buffer-low-water 0` means resume only when the
queue is empty, the most conservative resume and not a way of switching anything off.
What is lost is the part of the replies the kernel will not take in the one best-effort
flush each connection gets as it is closed, which still runs, and nothing after it does.
No read pass runs either, so a request that arrives at any time after the loop's last
pass, the whole of the snapshot save included, is still unread when its connection
closes, and the close then resets it and discards what the kernel was holding unsent as
well. An operator who reads the limits correctly and generalises will set this to `0`
expecting an unbounded wait, and lose queued replies on every restart. A negative value
is refused, and so is one above the same `2**63 - 1` ceiling the two intervals have.

A client that finishes sending and keeps reading, a socket half-close, is not a dead
peer during the drain. End of input on a connection that still owes bytes does not close
it: reading from that connection stops, because end of input stays readable and would
otherwise wake every pass of the loop, and the drain keeps sending until the connection
is drained or the deadline arrives. End of input on a connection that owes nothing still
closes it, because there is nothing for the close to discard. Outside the drain, end of
input is taken for a dead peer: the connection closes and whatever is queued for it is
discarded after the one best-effort flush, as the reference does.

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

**Two more flags cover where the server listens and what it says.** `--host HOST`
(default `127.0.0.1`) is the IPv4 address to listen on, or a name that resolves to one.
The default is loopback, so a server is local-only unless asked otherwise: it has no
authentication, and whatever can reach the port can read, overwrite or flush every key.
An empty `HOST` is refused, since it would listen on every interface, and `0.0.0.0` asks
for that by name. `--log-level` (default `INFO`; `DEBUG`, `INFO`, `WARNING` or `ERROR`,
in any case) sets how much is logged, once, in `main()`: importing `server` configures
nothing, so a program that embeds it keeps its own logging. INFO adds a line for each
connection opened and each closed to what WARNING shows. Those two lines have no count
bound of their own, unlike the refusal and task-failure lines above, and each is a
blocking write to standard error on the one thread this server has: a client that
connects and disconnects in a loop can fill a stalled reader's buffer by itself and park
the loop, which is the hazard those bounds exist for. `--log-level WARNING` removes
those two lines, and with them the line a clean shutdown drain writes, which is also
INFO; it does not remove the hazard. Two WARNING lines are the same kind of writer: one
per connection, with no count bound of their own. One is written when a connection is
closed for holding a half-sent command past `--incomplete-command-timeout`, and a
client reaches it by connecting, sending half a command and waiting out the timeout.
The other is written when a connection is closed for queued replies still over
`--write-buffer-limit`, and a client reaches it by asking for a reply bigger than the
limit plus whatever the kernel takes. A client that does either in a loop still drives
a blocking write per connection at WARNING. That is a residual this server accepts and
does not bound: the refusal and task-failure lines above are counted, these two are
not, and the write is the same blocking one on the same thread. DEBUG adds a line per
dispatched command naming it and counting its arguments, never showing a key or a value,
and that call sits behind a level check, so at INFO nothing is built for it. An unknown
level is a usage error and exits 2. Neither flag takes a number, so neither has a `0`
to misread.

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

The four crash-consistency tests kill a real server with `SIGKILL` and check what a
restart recovers. Each run takes seconds where the rest of the suite takes milliseconds,
so `pyproject.toml` deselects them, and
`python -m pytest -m manual tests/test_crash_consistency.py` selects them.

### In a container

```bash
docker build -t mini-redis .
docker run -d --name mini-redis -p 127.0.0.1:7000:6379 -v mini-redis-data:/data mini-redis
redis-cli -p 7000 PING
docker stop mini-redis
```

The image exposes port 6379, which the `-p` above publishes on loopback as 7000. It runs
the server as a non-root user whose uid is fixed so that a volume written by one build
is writable by the next, and keeps its snapshot at `/data/dump.mrdb`, inside a volume,
so that stopping the container and starting another over the same volume keeps the
keyspace. Inside the container the server listens on every interface, because a
published port is unreachable otherwise, and it has no authentication: a bare
`-p 6379:6379` offers every key to whatever can reach the host, so publish to loopback
as above unless you mean otherwise.

The `CMD` is in exec form on purpose. The server is then PID 1 and receives the
`SIGTERM` that `docker stop` sends, which is what makes it save and drain on the way
out. In shell form PID 1 is the shell, which does not forward the signal: `docker stop`
took 10.23 s, the container exited 137 and no snapshot was written, against 0.23 s, exit
0 and a snapshot written with the exec form. `docker stop` waits ten seconds by default
before it sends `SIGKILL`, so a keyspace whose save takes longer than that needs
`docker stop -t`.

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
server.py       entry point, listener, signals, shutdown drain, connection cap, water marks, dispatch
event_loop.py   selectors readiness dispatch (on_readable / on_writable / on_accept)
connection.py   per-connection socket, read/write buffers, lifecycle
resp.py         RESP2 parser and serializer
store.py        keyspace, expiry index, pending-effects queue
persistence.py  versioned snapshot format, atomic save/load
ratelimit.py    docstring only; designed in full and deliberately unbuilt
replication.py  docstring only; designed in full and deliberately unbuilt
commands/       __init__.py, registry.py, server.py, string.py and list.py; twenty-six commands total
Dockerfile      single-service image: non-root user, snapshot in a volume at /data
.dockerignore   the build context is an allowlist of the files the image runs from
tests/          pytest suite, run in CI against Python 3.11 and 3.13; the crash tests are manual
```

[Design notes](docs/DESIGN.md) cover why each of these is shaped the way it is, and the
deliberate differences from real Redis.
