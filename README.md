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

One part of the design is not in it. Replication is specified in full and deliberately
unbuilt, and [Designed and not built](#designed-and-not-built) says what that leaves
behind in the code. Rate limiting was cut the same way and built afterwards: the `v1`
tag does not contain it and the tree does, and it is off unless `--rate-limit` is given.

**Two tags, and neither is where the tree is.** `v1` is the commit the build ended at
and `v1.5` the commit the rate limiter closed at, and both stay where they are: moving a
pushed tag rewrites what anyone who already fetched it sees. So the tree is past both of
them, and deliberately — every commit since has either repaired something found in the
closing commits or corrected something this front page claimed. Read a tag
as the milestone it marks, and `main` as what the server is. `git rev-list v1..HEAD
--count` says how far apart they are, and it keeps moving.
[Benchmark](#benchmark) has what the server costs against real Redis, and
[Limits](#limits) is the long list of what it does and does not do under pressure, the
rate limiter included.

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

`-q` prints a rate and one point of the distribution, `p50`, so the table below comes
from the same command without `-q`, the only form that reports the distribution whole; each row's rate and
p99 come from the same run. **Each row is one run, and one run is worth less than it
looks on this machine.** Re-running the whole set moved individual rates by as much as
twenty per cent and some p99 figures by half again -- `PING_MBULK` came back at 65,147
and 76,161 against the 84,962 below, and the reference's `LRANGE_600` p99 at 4.255 ms and
3.783 ms against 2.743 ms -- on a laptop carrying other work, which is what a laptop
always is. What survived every repetition is the shape rather than the cell: the median
share of native stayed within about a point of the figure quoted here, every test
cleared the threshold, though not always with room to spare, since one `-P 16` round
put `SET` at 8.18% of native. What the p99 columns do not support is a ratio: the
table's own eight small commands give 1.62 to 4.15 times the reference, and the
reference's own p99 on those commands has been measured as low as 0.375 ms and as high
as 2.047 across later runs, so a ratio taken from any one pair of runs says more about
which pair than about this server. Read a row as the order of magnitude it establishes,
not as a number to compare against your own hardware.

The table is the measurement the shipped version was judged on, and the tree has moved
well past the tag it was taken at (`git rev-list v1..HEAD --count` says how far, and it
keeps moving) -- one of those commits changed `take_commands()`, the read path all ten
tests go through -- so I took it again, on a tree that did not yet hold the rate
limiter, against a private `redis-server 7.2.7` started for the run on a port of its
own. The fourteen `redis-benchmark` invocations ran at load averages of 3.97 to 9.82.
What a re-measurement was for here was the figure this section says to read, the median
share of native, and the honest answer is that it does not hold to a point either. At
`-P 16` the eight small-reply commands ran at 19.6% to 27.2% of native, median 23.5%,
and the two `LRANGE` tests at 49.9% and 60.9%; at `-P 1` the median share was 63.0%.
Taken again later on CPython 3.13.9, the interpreter the first table was measured on,
four paired runs at `-P 1` gave medians of 58.9%, 63.3%, 59.0% and 58.9% -- deviations of
-4.1, +0.3, -4.0 and -3.1 points, so 63.0% is the top of the spread and not a value a
re-measurement recovers -- and on 3.14.8, 57.6%. At `-P 16` on 3.13.9 the eight-small
median came back 21.8% and 30.7% in two consecutive pairs on one host. The spread tracks
the reference and not this server: its own `PING_MBULK` moved between 0.909M and 1.538M
requests per second across those two pairs. So read the median as a range and not as a
figure, and read the 8% ship threshold as the one claim here that reproduces: it was
cleared in every row of every run, at a worst case of 17.2%. The paragraph after the
second table holds the figures to set these beside. A row does not hold still the way the median does: in later
repetitions a row's share was as much as 17.4 points above and 16.4 below its figure in
the table at `-P 1`, with six of the ten rows more than 6.4 points off in a single run,
and as much as 18.2 above and 11.1 below at `-P 16`. The reference's own p99 on the
eight small commands does not hold still either: 0.455 to 0.927 ms in the first table,
and 0.375 to 0.951, 0.775 to 1.231 and 0.679 to 2.047 ms in three later runs of the set.
A ratio of the two p99 columns therefore describes one run and not the server, and the
first table's own run gives 1.62 to 4.15 times. The snapshot path was still empty before
each stop, which is what shows that no periodic save fired. It was not empty afterwards:
the stop saves even at `--snapshot-interval 0`, and a 700,118-byte snapshot appeared
after it in 7 of 7 instances. The rates in the table are left as they were taken,
because each cell is one run and a newer run is no better evidence of a cell than an
older one; what a re-measurement can settle is the share, and that is what is reported
here. The rate limiter was built after that re-measurement, so neither table was taken
with its check in the path. With the limit off, which is the default, the check is a
method call that tests one attribute and returns. Measured with `python -m timeit` on
the statement the dispatch loop actually writes, it costs about 38 ns per command on
CPython 3.13.9 at load average 4 -- 26 ns once the attribute lookup is hoisted out --
and the figure moves with the machine, 41 to 44 ns at load 5.2. That is about 5.7% of
the server's own per-command dispatch work, 39 ns against 697 ns for one `PING` through
`_dispatch_batch` measured on the same harness, and 1 to 2% of an observed pipelined
round trip. A `-P 16` comparison with and without it fell inside the noise. A harness
that wraps the call in a lambda and never subtracts the lambda's own 15 to 22 ns reads
far higher, 70.7 to 93.1 ns on CPython 3.14.8, which is a property of the instrument and
not of the check.

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
was empty when each run started and still empty when it ended, which is how I know none
fired -- checked before the stop, because the stop itself writes one even at
`--snapshot-interval 0`, so a path checked after the process exits says nothing. The rate limiter was
off, by construction and not by configuration, because it did not exist when either
measurement was taken. It is off by default now, and the reason is this benchmark: a
limit that was on would have tripped under `-c 50` at once, and the numbers would have
measured its rejection path. And the log level was `WARNING`, which leaves out the line
this server writes at `INFO` for each connection it opens and closes.

## Three findings

Each of these came from testing something that looked fine, and none of them is visible
in the code unless it is written down here.

**A length-prefixed format does not make a checksum redundant.** The argument for
leaving one out is that every length prefix is a bounds check, so a corrupt snapshot
fails at a known offset. Truncation does: it is refused at every offset. A flipped bit
mostly does not. Every bit position is swept, not sampled: a 2,036-byte snapshot of 41
keys holding a list and a TTL is 16,288 bit positions, and each one is flipped in turn,
so the figures below are exact and carry no "about". Decoded with the trailer recomputed
over the corrupted bytes, which is what a length-prefixed format with no checksum
amounts to, each flip lands in exactly one of three outcomes. **12,942 of the 16,288 —
79.4573% — were silently accepted as a snapshot holding different data.** 3,314, or
20.3463%, were caught, itemised in full because the parts have to sum to the whole:
2,759 by a length prefix running off the end, 280 by a type byte that named no kind, 202
by a key arriving twice, 32 by an unsupported version, 32 by a wrong magic, 8 by
trailing bytes past the last entry, and 1 by a list entry holding no elements. The
remaining 32, 0.1965%, are the four bytes of the CRC trailer itself: recomputing the
trailer overwrites the flip, so the decoder is handed the original bytes and returns the
original store — accepted, but as identical data rather than as different data, which is
why the first two shares do not sum to 100%. With the CRC32 trailer verified instead of
recomputed, every flip is refused, exhaustively, on every fixture: 94,960 positions,
94,960 refusals.

The accepted share is not a property of "a fixture of this shape". It is the payload
byte share, and it moves with the key set: a flip in a key is accepted as different data
unless it lands on a key some other entry already holds, in which case it is caught as a
duplicate. `key00` and `key01` differ by one bit, so plainly numbered keys give 202 such
catches and 79.4573% accepted, while a key set chosen so that no single flip can collide
gives **zero** duplicate catches and 80.6974% accepted, with 19.1061% caught. The two
fixtures differ in nothing but their keys, so the whole of the difference is those 202
catches: 12,942 plus 202 is 13,144. That pair
is the maximum over key choices at this size and not the figure for the shape — the two
cannot describe one blob, since the maximum is reached only where the duplicate-key count
is zero. The exact law, checked on every fixture swept: the accepted-different count plus
the duplicate-key count equals the payload bits, and the caught count minus the
duplicate-key count plus the trailer's 32 equals the structure bits. That is the finding:
most bytes in a snapshot are payload and not structure, so a flip inside a key, a value
or an expiry passes every bounds check there is, and no length-prefix, count, magic or
version bit is ever accepted. How much of the blob is payload moves the share far more
than the key set does: on a three-key fixture the decode accepts exactly 50.0000% as
different data, which is exactly its 51 payload bytes of 102. These corruptions are from
that three-key fixture, each reachable by a single flip: `HELLOWORLD` came back as
`HELLOWOVLD`; a key named `beta` came back as `beua`, so `GET beta` answered nil and a
key nobody wrote existed; an expiry of 1700000000000 came back as 1699995805696.
`tests/test_persistence_properties.py` pins the refusal side exactly — every flip a
`SnapshotError`, and for the right reason — and now pins every figure in this paragraph
as well, by running the same exhaustive sweep. `docs/DESIGN.md` has the other half of the
picture, the count and version fields, where every flip is caught, though not all of them
by a length prefix.

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
7.2.7, the version the numbers above were taken with: `-t nosuchtest` printed one byte, a
newline, wrote nothing to standard error and exited 0; `-t set,nosuchtest` ran `SET` and
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

At the pinned request count, with `redis-benchmark` 7.2.7 driving each of the two
servers in turn, the run reports nothing missing: over nine alternated pairs of runs, a
real `redis-server 7.2.7` carried 4 lines with the seeding label in four of the five
runs at load 6.5 or below, the fifth run's count not being recorded, and 5 to 11 at
higher load -- though 5 has since been measured at load 4.21, the lowest reading in a
later set, so do not read 4 as the low-load count, eleven having reproduced once, at load 8.05, and this server carried 7 or 8
below that load and 8 or 9 above it. How many there are is not fixed — the reason is
the repainting described below — and the gap between the servers is not constant
either: 3 or 4 lines at load 6.5 or below and from -2 to +4 above it, this server's
count minus the real server's, the first range being over the four low-load pairs whose
real-server count is recorded and not over all five. Read the per-server counts as
load-dependent and take neither the gap nor any single count as fixed. With `lpush`
dropped from the list, so nine names, the tool still exits 0 and the rule reports
`LPUSH` missing, while a plain search for `LPUSH` anywhere in the output and a rule that
discards exactly one seeding line both report nothing missing. The tool repaints that
line while it seeds, so how many there are depends on how long seeding takes; at a small
request count there can be just one, and the singular rule and the plural one agree. I
verified the rule at the pinned count for that reason. Deleting the `\r` characters
instead of splitting on them failed a correct run at small request counts on 6.2.14 and
did not on 7.2.7 at 1,000, 2,000 or 100,000, so the split is kept as the reading that is
right on both.

## Designed and not built

One part of the design is specified in full and deliberately unbuilt: leader-follower
replication. `replication.py` holds a docstring and nothing else, and that is a decision
and not an omission. I cut it on purpose, at the point where stopping left nothing
half-built in the tree, and I do not intend to build it.

The per-connection rate limiter was cut in the same decision and built afterwards. The
`v1` tag does not contain it: at the tag, `ratelimit.py` held a docstring and nothing
else. [Limits](#limits) says what it does and where it stops.

Replication was to be asynchronous: a follower connects, is sent the leader's keyspace
in the snapshot format as one bulk string, and then receives a live stream of what each
handler reports as its effects, plus the `DEL`s that expiry produces, in execution
order. A follower would have applied that stream through its own dispatcher, and the
leader's link to its own leader would have been a third kind of connection, exempt from
the rate limit a client is under.

Because the design was written before it was cut, four things in the code are
present and nothing in a running server puts them to the use they were written
for, and I would rather say so than leave them to be found. The first changed
when the rate limiter was built.

`Connection` knows three roles, `client`, `follower` and `leader_link`, and the rate
limiter reads them: a `client` is limited and the other two are let through. Nothing in
the server sets either of the other two. Every accepted connection is built as a
`client`, and the flag that would have started a server as a follower was never written,
so with a limit set the exemption runs on every command and has no connection it can
apply to. Only the tests reach it, by constructing a connection with each of the two
roles by hand. The test is for `client` and not against `follower` alone because the
roles point opposite ways: a follower is a peer syncing off this process, and
`leader_link` is this process's own outbound connection to its leader, which a check
written against `follower` alone would take for an ordinary client and limit, with no
error anywhere saying so. The `replication_state` slot on `Connection` is the same kind
of thing: it holds `None` and nothing reads or sets it. `rate_limit_state` was in this
group and is not any longer, since the limiter builds its window there.

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

The fourth is the `read`, `write` or `other` tag that each command carries from the
decorator that registers it. Nothing in the server reads it: the rate limiter counts
every command whatever its kind and exempts by the connection's role, and replication,
the other reader it could have had, is not built. Only the tests read it.
`docs/DESIGN.md` has why it is declared where the handler is.

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

**Why would every reconnect cost a full sync, and why not `PSYNC`?** Replication is not
built, so this is a decision about a design that never ran. A follower that lost
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
below leaves hanging. What decides it is the total size of the replies against the
high-water mark and not the number of commands, so no count is the threshold: 300,000
`SET`s, whose replies are five bytes each, completed in 2.27 s, while 600,000 hung and
300,000 `PING`s hung in two attempts of four and completed in about a second in the
other two. `--write-buffer-high-water 0` is what lets any of them complete. `redis-benchmark`'s
default run completes nine tests — `PING_INLINE` and `PING_MBULK` both, then `SET`,
`GET`, `INCR`, `LPUSH`, `RPUSH`, `LPOP` and `RPOP` — and exits at `SADD`, the first
command in its sequence this server does not implement at all. Two of the nine are `PING`,
because the tool sends it unframed and framed and this server answers both.

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
already grown — real Redis has the same property, and documents the command as
`O(N)` and dangerous on a large keyspace rather than documenting the reply's size.
`INFO`'s
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
ceiling behind them. With the pause in force it is not what slows a client down, because
it does not slow anything down: it closes, and the client that would have reached it is
paused instead. What the limit catches is a reply larger than it is, or the replies to
one read's worth of pipelined requests, which the pause cannot unqueue once they are
parsed. What the pause does not do is hold such a client anywhere near the high-water
mark. The batch that crossed the mark is still dispatched whole, so a paused connection
holds the mark plus that batch — bounded at the shipped defaults by the limit itself, 32 MiB,
thirty-two times the mark and the whole of the ceiling, because a connection that survives its
batch is by definition one whose last flush found the queue not exceeding the limit. That is a
bound from the mechanism and not the largest thing measured: over 152 observations sweeping the
batch from 28 to 40 replies, the most a still-open paused connection held was 33,239,356 bytes,
99.1% of it, and the running maximum was still rising at the last observation. Take the 32 MiB as
the bound and the 99.1% as how close a sample gets. The transient peak before the limit closes a
connection is bounded by the limit plus one reply, 34,603,020 bytes at 1 MiB replies,
and was measured to within 37,692 bytes of that. A rate-limit refusal counts as a
reply: the refusal path queues its error and reaches the same check. Neither bound was
exceeded in the 152, and the pause fired exactly once in every one. The pause postpones
the limit for a client that stops reading and does not prevent it for one that keeps
reading slowly; it never keeps either far away from the limit, and the paragraph on the
pause below says why. The 32 MiB default is a choice, and a departure from the reference,
which leaves an ordinary client's output buffer unlimited. This server does not: nothing
else here bounds what a few kilobytes of pipelined requests can queue, and a bounded
queue is worth being different over. `--write-buffer-limit 0` is the reference's
behaviour.

**A value can be stored and not read back.** `--max-value-size` defaults to 64 MiB and
`--write-buffer-limit` to 32 MiB, and nothing relates the two, so a `SET` of a value
whose reply is larger than the limit is accepted, and the `GET` that follows closes the
connection unless the kernel takes enough of the reply to bring what is left under the
limit, because the limit is judged after the send, on what the kernel would not take.
For a reply well over the limit that is nearly every time, and for one only just over
it, such as a 32 MiB value's, 13 bytes over, almost never. The reply is the value plus
its `$<length>` line and terminator, so the range begins at 33,554,420 bytes, twelve
under 32 MiB, and runs to the size cap. The value stays stored either way. A hard limit
does the same in the reference, so the choice here is to say so and leave both numbers
alone; `docs/DESIGN.md` has the sizes measured against this server and against the
reference. To read back every string value the size cap admits, set
`--write-buffer-limit` to at least `--max-value-size` plus the reply's `$<length>` line
and terminator, 13 bytes at the default cap, which makes 67,108,877 and is the largest
reply a `GET` can queue for a string, or to 0. A lower limit still closes the connection
on the largest value whenever the kernel takes less of the reply, at the send the limit
is judged on, than the limit falls short of it. That does not reach a list, whose
elements are each held to the size cap and whose length is not capped: `LRANGE 0 -1`
queues the whole list as one reply, judged whole against the limit, so a list of
elements that are all far under the cap can be stored and not read back at any finite
limit, and a `KEYS` reply is judged the same way. Only 0 reads every one of them back.

**A client that stops reading entirely is paused, not closed. One that merely reads
slowly is closed.** The pause postpones `--write-buffer-limit` for the first and does not
prevent it for the second, and it never keeps either away from the limit. The reason is
that the pause manufactures the batch the limit is judged against: a paused connection's
requests pile up in its kernel receive queue, the resume reads that pile in one go, and
every command in it is answered, so "the replies to one read's batch" is not bounded by
the mark at all. At the shipped defaults a client that never pipelines — one request per
`send()` — and reads steadily but slower than this server produces is closed mid-reply,
with the value still stored; `--write-buffer-limit 0` is what prevents that, and a
lock-step client that reads each whole reply before sending again is safe at the
defaults. `docs/DESIGN.md` has the measurement and the two controls.
`--incomplete-command-timeout` is suspended for every paused connection, whatever it has
or has not sent, so that flag does not bound it either; and the pause holds what is already
queued for it, at least the high-water mark's worth plus whatever the one batch that
crossed the mark queued, until it disconnects. That is bounded: per connection by the
mark plus that batch, which the limit itself caps while the limit is on — the
limit plus one reply, a refusal under `--rate-limit` included, is the transient
peak before a close, not the hold — and over all connections by
`--max-connections`. Read that bound as the product it is: the 32 MiB above,
times the default `--max-connections 1024`, puts the aggregate at 32 GiB — far
past the memory of any machine this is likely to run on, and the cap is what bounds the
count of connections, not what makes the total small. Ninety-six paused connections
drove 2.9 GB into swap on an 8 GiB machine, with every one still open and a fresh client
still answered in six milliseconds. That a client is held and never closed is also a
divergence from the reference, which keeps reading such a client and closes it at the
hard limit configured for ordinary clients, if there is one. It is accepted and not
fixed: bounding how long a connection may stay paused would be a control of its own, and
`--incomplete-command-timeout` is not it, since it is suspended for a paused connection
whether or not that connection holds a half-sent command.

The pause has two more costs, for two different clients. One that writes every request
before it reads any reply, as `redis-py`'s `pipeline()` does with the batch it holds,
hangs once the requests it has still to send no longer fit in what the two kernels will
hold, and nothing closes it. Where that point falls moves from run to run, and with
`--write-buffer-high-water 0` the pause is off and the same client is answered in full.
And a client that stops reading loses writes when the server is stopped. Its requests
wait unread for as long as it stays paused, which is until it disconnects; a stop
dispatches none of them, so a pipeline whose `send()` has already returned is lost in
full. It takes the pause to have fired, which takes more queued replies than the two
kernels will hold — below that the connection is still being read, and what is then
lost is whatever the loop had not read when the stop landed rather than the whole
pipeline: measured with the pause unable to fire at all, a prompt stop discarded 560,000
to 1,846,734 bytes of a 1,977,780-byte pipeline over ten runs, median 1,354,501, which
is 28.3% to 93.4% of it, and a server given three seconds to catch up discarded none in
five of five. Those are the smallest and the largest of ten, not a floor and a ceiling.
The pause is what makes the loss the whole pipeline and makes it certain. What follows
is a second harness, whose pipeline is 50,000 `SET`s, 1,588,890 bytes, and not the
1,977,780 bytes of the ten runs above; a byte count below is of that pipeline unless it
names another. Measured at the shipped defaults over twenty-three runs where it did
fire, each preceded by twelve 1 MiB replies so that it fires every time: of 50,000
pipelined `SET`s whose bytes the kernels had accepted, all 50,000 were discarded and
none executed, with the server exiting 0. How many are executed is not fixed: earlier
probes of the same shape executed up to about two thousand, so read the loss as the
whole pipeline rather than as a figure near it. With `--write-buffer-high-water 0` and
three seconds for the server to catch up, 50,000 of 50,000 executed in five runs. A
client's end of input is clean when everything it sent had reached the server's receive
queue by the drain's last look, the walk it makes in its `finally` over the connections
still open, because the drain reads and discards the receive queue of every connection
it was serving, including the ones no pass of it ever read. A client still waiting in
the accept backlog is never served, and one that had sent bytes sees a reset. Measured
at `--shutdown-drain-timeout 0` against a 700,000-byte pipeline the server never read,
a client that was owed nothing saw end of input in 10 of 10 runs where, before the
queue was read rather than asked about, it saw `ECONNRESET` in 10 of 10. A client that
was owed replies did not: it saw end of input in 0 of 10 runs, no different from
before, and in 9 of 40 across variants of the setup. The pipeline outran what the
connection's receive queue held. The client's own send queue, `SO_NWRITE`, read 568,968
bytes at the stop, so only 131,032 of the 700,000 bytes had arrived, and the pipeline
was 5.3 times what a paused connection's receive queue holds on this host; the rest
reached the server after the last look and was reset at the close. The discarded-bytes
figure was unchanged by the repair in both arms, at 131,032 and 131,040 bytes. What is
still a reset is a request that arrives after the last look and before the close, and
at that pipeline size that is the ordinary case and not a corner.
The drain's own line counts the inbound bytes the stop threw away in
all five shapes they come in, which for a client that never reads was exactly the bytes
the kernels had accepted, in each of those twenty-three runs: the whole 1,588,890 bytes
where the push had completed, which it had in 18 of the 23 at the 2 s the harness
allowed it and in 12 of 12 given 3 s. With the pause off the figure was 0 in 5 of 5 once
the server had three seconds to catch up; with a prompt stop it was 1,523,567, 737,152,
1,523,567, 1,195,680 and 1,195,680 bytes, with 2,143 to 26,964 of the 50,000 executed.
Turning the pause off removes the loss's certainty, not the loss. What the line cannot
count is what the peer's own kernel still holds unsent, which is not a question this
server can ask anyone, and how much that is depends on the client and not on the
timeout: on whether it reads at all and on whether it had finished sending. Every figure
from here to the end of this paragraph was taken at load average 2.9 to 7.0. A client
that never reads holds the drain open for its whole timeout, and the peer's kernel
delivers inside it: 0 bytes uncounted in 23 of 23 runs at the shipped
`--shutdown-drain-timeout 5`. `_drain_for` itself lasted 5.0042 to 5.1001 s over 12 runs
instrumented from inside the process, median 5.0949, and `SIGTERM` to exit 5.0287 to
5.3570 s over 35 runs, median 5.2230. The timeout plus one `select()` timeout is 5.1 s,
and the 0.1 ms the largest is over it is within what the two sweeps cost, which that
bound does not cover. A client that reads its replies ends the drain as soon as no reply
is owed, not after five seconds: `_drain_for` lasted 0.1 to 0.3 ms over 17 runs and
`SIGTERM` to exit 21 to 79 ms, median 46, over 20 ms in 15 of the 17. Whatever its
kernel still held is uncounted: non-zero in 8 of those 17 runs, 110,822 to 515,628 bytes
across the eight, median 380,328, equal to the client socket's `SO_NWRITE` read after
the server had exited in 7 of the 8 and off by exactly 8,192 bytes in the other, and all
17 were reported under an INFO line reading `complete`. At `--shutdown-drain-timeout 0`
there is no read pass, and the uncounted amount is what the peer still held, equal to
`SO_NWRITE` in 9 of the 10 non-zero runs below and off by exactly 8,192 bytes in the
tenth. Measured with a client that never reads, over 39 runs of a 1,588,890-byte
pipeline in three harness shapes of 15, 12 and 12 runs, it was non-zero in 10 of
the 39 and in none of the 15 where the stop came after the push had completed;
across the ten it ran 45,498 to 581,164 bytes, median 515,628, and the largest
is 36.6% of the pipeline. It is usually zero because by the time the drain's last
look runs the client has usually handed its kernel everything it was going to send,
and its own send queue is empty. These runs were taken while that look still asked
the kernel with `FIONREAD`, which on macOS loopback answered with the whole 1,588,890
bytes although `SO_RCVBUF` is 408,300, the receive queue overcommitting; the look now
reads the queue off instead. What stays uncounted is what a client still
pushing had not yet handed its kernel. On a 9,533,340-byte pipeline three runs gave
532,012 to 843,308 bytes, equal to `SO_NWRITE` in all three, so the amount follows the
client's send queue and not the size of the pipeline. Nothing in this server bounds it:
these are the largest values seen, not a ceiling. The drain's line is the only place the
loss is reported, and nothing bounds the loss itself. `docs/DESIGN.md` has the
measurements and the reasoning.

**Four more flags cover persistence and the active sweep.** Persistence is on by
default. `--snapshot-path` names the file a snapshot is written to and read back from
and defaults to `./dump.mrdb`, resolved against the directory the server was started in.
Nothing stops two servers sharing one, and the default plus one working directory is how
that happens: start the Quickstart twice in the same place and both answer `+OK`, both
exit 0, and the last stop to run overwrites the other's keyspace with its own. There is
no lock and no warning in either log, and neither process can tell the other is there.
Give each server its own `--snapshot-path`, or its own directory. The reference behaves
the same way with a shared `dir`, which is the only reason this is a caveat here rather
than a defect.
A file that is present but will not decode refuses startup rather than starting empty
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
such a failure reports is bounded: the first one is logged as one line naming the
exception's class and what it says, which for an `OSError` is the errno and the path it
failed on, and after
that a single line says how many times in a row it has now failed, once per hundred.
No traceback, at either step, and for a reason the same bound is about: rendering one
opens source files, and a descriptor table with no room left is one of the ways every
task guarded there fails, so the attempt would fail the same way and leave the operator
`--- Logging error ---` and no account at all.
That bound is not cosmetic. Logging writes to standard error with a blocking write, and
this server has one thread, so a reader that stops — a stalled collector, a pipeline
whose far end died — can fill its buffer and park the loop inside the tick, after which
nothing is served and nothing more is logged. A failure that repeats on a timer would
write its line every interval for as long as the server ran, and the bound above stops
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

**A clean stop saves, and a crash can still lose recent writes.** On `SIGTERM`, and on
`SIGINT` unless this process inherited that signal ignored, the server writes a snapshot
of the keyspace as it stood when the loop
stopped, and then spends up to `--shutdown-drain-timeout` seconds sending the replies
already queued for its clients before it exits; a save that fails is logged and the stop
carries on, and the one pairing of flags that skips the save is the one described with
the persistence flags above. The exception is not a corner. `nohup`, `setsid` and a
non-interactive shell's background job each hand a child `SIGINT` set to `SIG_IGN`, and a
server started that way installs no handler for it, by choice: a disposition this process
inherited as ignored is one its parent meant it to ignore, and overriding it would make
`SIGINT` stop a server the operator had arranged could not be stopped that way. So a
`SIGINT` sent to such a server does nothing at all — no save, no drain, no log line, and
the process goes on serving — and `SIGTERM` is what stops it. Measured: ignored, still
answering `PONG`, and `SIGTERM` then exiting 0 with a snapshot written.
What a crash leaves is the window between two saves: a
`SIGKILL`, a power cut or a process that dies mid-run loses whatever the last save did
not hold. The interval is counted from the moment a save finishes rather than from the
moment it starts — the rule `CONFIG GET save` publishes, and the reference's own. That
reply is spelt in the reference's syntax, where a rule fires once a clock is *more than*
`<seconds>` past the last save, so this server's one-save-every-N rule reads as `N-1 0`
and a default server answers `59 0` rather than `60 0`. It is not an off-by-one;
`docs/DESIGN.md` has why that spelling is the honest one. So
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
100,000 keys — 16-byte keys and 100-byte values — is a 13,300,016-byte payload, which is
12.7 MiB and is not a measurement at all: the format fixes it at twelve bytes of header,
133 per entry and a four-byte trailer, and it came back identical in 45 of 45 runs. The
three costs beside it are measurements and are published as the bands they came back in,
over 30 runs on CPython 3.13.9 at load average 3.6 to 4.2: 64.2 to 78.1 ms to serialize,
median 66.2; 83.2 to 93.8 ms to deserialize, median 85.7; and 106.3 to 115.5 MiB of peak
resident memory, median 112.5, in a process that builds the keyspace, serializes it and
then decodes the payload back into a second copy, with all three alive at the peak. Read
through `resource.getrusage(resource.RUSAGE_SELF).ru_maxrss`, which macOS reports in
bytes -- `RUSAGE_CHILDREN` is what this sentence used to name and it answers for
waited-for children, so in the one process described here it returns 0. None of the
three is a floor: a louder machine reads higher, which is why the load is part of each
figure rather than a caveat on it. The fixture
is stated because the payload is a function of it, and the figures come from measuring
the tree that ships. These figures are published here, and `docs/DESIGN.md` cites
them without repeating them.

**Two more flags cover the way out and the way in.** `--shutdown-drain-timeout SECONDS`
(default `5`) bounds the wait for clients and nothing else on the way out. The drain is
the step after the save: a sweep of the accept backlog, described below, at its entry, a
setup pass over the connections, then the wait, which is where the server sends the
replies already queued for clients, and a second sweep in its `finally`. The wait is the
one part this flag limits. The wait for the loop to notice the signal, up to one
`select()` timeout, comes before the drain, and so does the snapshot save, which costs
what the keyspace costs; the two sweeps and the reads that empty and count the
receive queues have no deadline of their own, and the teardown's flush, described
below, comes after the drain and has none either. This flag limits none of these.
On a stop signal -- `SIGTERM`, or `SIGINT` where that signal was not inherited ignored -- the loop stops, and then, before anything
is torn down: the listening socket leaves the select set, so no connection that arrives
from here on is served, though the socket itself stays open until the teardown and a
replacement server cannot bind the port while this process is still running; a snapshot
is saved; and the server spends up to SECONDS sending the replies already queued for
clients, then exits whether or not the kernel took all of them. The save comes before
the drain on purpose. A client that never reads holds the drain to its whole timeout,
and an operator who gives up and sends `SIGKILL` should not find that the snapshot was
the thing waiting behind it. It runs whether or not `--snapshot-interval` is `0`: that
interval schedules periodic saves, and a save on the way out is a different thing, which
is also why `CONFIG GET save` can answer an empty string from a server that will still
write a snapshot when it is stopped. The one pairing of flags that skips it is the one
described with the persistence flags above, where the file is being left alone on
purpose. During the drain the server keeps reading its clients and dispatches nothing.
Stopping reading looks like the way to keep the drain finite, and it is the wrong one: a
request left unread in a socket's receive queue turns the close that follows into a
reset, and a reset discards the closing socket's own send queue, which is the replies
this process handed to its kernel and the kernel had not yet put on the wire. Replies
the peer had already received are not taken back; the tail that never left is what goes.
What keeps the drain finite is that no command runs and so no new reply is queued, while
inbound bytes are read and thrown away: the receive queue is empty at the close, so the
close is orderly and the kernel goes on sending what it holds. A connection the
high-water mark had stopped reading is read again from the moment the drain begins, for
the same reason: whatever it had not read would still be in its receive queue at the
close. The deadline is checked between passes of the wait, so SECONDS plus one
`select()` timeout bounds the wait, and only the wait. The two sweeps accept what the
kernel completed into the listener's backlog after the stop, only to count what each
holds and close it, and they sit outside that bound, with no deadline of their own. The
kernel's accept backlog does not bound them either: a sweep ends the first time its
`accept()` finds the backlog empty, so it goes on while another connection is waiting at
every poll, and one sweep accepted 722 connections where `SOMAXCONN` is 128 here, which
means clients arrived while it ran. A sweep costs more while clients are still
connecting, as the floods below show, and these figures were taken at load average 2.9
to 7.0. Timed inside the sweep, 58 sweeps on a live server took 0.005 ms to 383.302 ms,
median 0.900, 14 of them under 0.2 ms and 6 over 28 ms; the six ran 106.8 to 383.3 ms
and include the slowest sweep of each of the three floods below. With 4, 7 and 32
processes connecting continuously at `--shutdown-drain-timeout 0`, `SIGTERM` to exit
passed 0.07 s in 17 of 22 runs, median 0.0846 s, and reached 0.4199 s, the slowest sweep
in each flood taking 139.0, 363.9 and 383.3 ms. Those are the largest seen, not a bound.
They matter at `--shutdown-drain-timeout 0`, where four steps between the loop
stopping and exit have no deadline: the snapshot save, the two sweeps, the reads
that empty and count the receive queues, and the teardown's flush. The flush
runs after the drain's line is written: `_shutdown` closes every connection
still open, up to `--max-connections` of them, and each close sends in a loop until its
buffer is empty or the kernel will take no more. Over 11 runs at
`--shutdown-drain-timeout 0`, with no limits and 150 connections each owed one
1,048,588-byte reply, the line read `incomplete` with 143 to 146 of the 150 still owed
bytes, readers that began after the signal then received 63,918,688 to 143,578,892 of
the 157,288,200 bytes queued, 40.6% to 91.3%, median 50.3%, and the process exited 0.052
to 0.278 s after the signal. The reads are the fourth step: each takes what the kernel
reported queued when it was asked, so their cost follows the bytes queued and not the
number of connections, and `docs/DESIGN.md` has the one measurement of it and says why
it gives no worst case. The drain ends in one log line however it ends, even when a
pass raises. The line carries two counts of replies, connections closed while they still
owed bytes and connections still owing bytes when the drain ended, and it is a warning
if either is non-zero and informational if both are zero. Both are shown at the default
`--log-level`, which is INFO, so a clean stop ends with a line that says `complete`;
`--log-level WARNING` leaves only the warning. That word is about replies alone: it
means every connection that was owed bytes got them, not that nothing inbound was thrown
away. Both counts are taken before the teardown's flush, so the line can report as still
owed bytes that the flush then hands over, as in the runs above, and the level
over-reports loss by that much. It carries one figure for requests as well: the inbound
bytes it threw away without dispatching them, which is the only report there is of the
writes a stop throws away, and which matters most for a connection the pause had stopped
reading, since that connection's whole pipeline is sitting unread. It counts all five
shapes that loss arrives in — what was in a read buffer when the drain began, what a
half-received command had already had parsed off that buffer, what the drain read and
discarded while it ran, what was still unread in a connection's receive queue when that
connection was closed or the drain ran out of time, and what sat on a connection the
kernel completed into the accept backlog after the stop, which is never served. The
receive queue is the shape that dominates, and the one a count of the drain's own reads
would miss entirely: the drain ends as soon as no connection is owed a reply, and at
`--shutdown-drain-timeout 0` it ends before a single read. That figure does not move the
level. The bytes of a command only half sent are discarded by a stop like those of a
request nobody will answer, and `has_incomplete_command` tells them apart only where
they have been read: it answers for bytes in a read buffer and for a multibulk held with
the buffer empty, which are the first two shapes, and it cannot answer for the shape
that dominates, because the bytes in a kernel receive queue were never parsed. So the
two kinds are distinguishable where they have been read and not where most of them are.
Even where they can be told apart, no threshold separates them by size, which is the
argument that carries: one client caught mid-upload of a large value holds more bytes
than a whole discarded pipeline does. Measured, 4,194,334 bytes, 4 of 4, for one client
that had sent a 4 MiB body of a declared 8 MiB `SET` value, 20 consumed and 4,194,314
buffered, against 1,588,890 for fifty thousand pipelined `SET`s. The product of the two
caps does not bound what one connection can hold for one unfinished command, because the
count includes the multibulk's header and, for every element, finished or arriving, its
length line, its body and its terminator. At `--max-value-size 100` the amount held and
reportable at `--max-multibulk` 3, 4, 6, 10 and 100 was 276, 384, 600, 1,033 and 10,754
bytes, against products of 300, 400, 600, 1,000 and 10,000: under at 3 and 4, equal at
6, over by 33 at 10 and by 754 at 100. With both caps on, the bound is the product plus
each element's own framing, the header and a length line and terminator per element,
which at the defaults is a little over 64 TiB, a figure no host holds, so it describes
the arithmetic and protects nothing. A threshold set above the pipeline misses the
pipeline and a threshold below it warns on an ordinary upload, so the level says what it
can say exactly -- whether a reply was lost -- and the figure is read for the rest.

`--shutdown-drain-timeout 0` reads the opposite way from the limits it sits beside. For
`--write-buffer-limit`, `--max-value-size`, `--max-multibulk`, `--max-connections`,
`--rate-limit` and `--incomplete-command-timeout`, `0` removes the limit, so a check
that closes or refuses something stops doing it and turning it off is permissive. This
value is a time allowance and what it allows is the wait for clients, so `0` is no time
at all, and means no drain pass — the setup pass and the two backlog sweeps still run,
but nothing is read: turning it off is restrictive. (`--port 0` and the two intervals
read in neither way: the kernel chooses a port, or a periodic task is switched off, and
none of the three is a limit.) The two water marks read in neither way either, and each
has a reading of its own: `--write-buffer-high-water 0` disables the pause, so reading
goes on however much is queued, and `--write-buffer-low-water` is then unreachable,
which is accepted and not refused; with the pause on, `--write-buffer-low-water 0` means
resume only when the queue is empty, the most conservative resume and not a way of
switching anything off. What is lost is the part of the replies the kernel will not take
in the one best-effort flush each connection gets as it is closed, which still runs, and
nothing after it does. No read pass runs either, so a request that arrives at any time
after the loop's last pass, the whole of the snapshot save included, is never
dispatched. It is not left to be reset at the close, though: the drain reads it off
and counts it first, in the close of a connection that owed nothing and in the walk
it makes over the connections still open as it ends, as the paragraph on the
drain's two limits says. An operator who reads the limits correctly and generalises
will set this to `0` expecting an unbounded wait, and lose queued replies on every
restart. A negative value is refused, and so is one above the same `2**63 - 1` ceiling
the two intervals have.

A client that finishes sending and keeps reading, a socket half-close, is not a dead
peer during the drain. End of input on a connection that still owes bytes does not close
it: reading from that connection stops, because end of input stays readable and would
otherwise wake every pass of the loop, and the drain keeps sending until the connection
is drained or the deadline arrives. End of input on a connection that owes nothing still
closes it, because there is nothing for the close to discard. Outside the drain, end of
input is taken for a dead peer: the connection closes and whatever is queued for it is
discarded after the one best-effort flush, as the reference does.

The drain has two limits worth knowing. The first is that a client that is still sending
when it ends can still lose the tail of its replies. The drain finishes as soon as the
last byte reaches the kernel, which on loopback is long before a slow client has read
it, and handed to the kernel is not on the wire: the kernel can still be holding the
tail unsent. A request arriving after the drain's last look at the receive queues, the
walk it makes over the connections still open as it ends, leaves unread bytes at the
close, which resets the connection and discards what the kernel was holding. What the
peer had already received stays received. A client that sends and then stops loses no
reply tail this way, since by the last pass every reply it is owed has been handed over
or counted as lost. At `--shutdown-drain-timeout 0` the limit does not begin earlier,
although no pass of the drain reads at all: the last look still runs, so a request
arriving after the main loop's last read and before that walk, throughout the save, is
read off, counted and thrown away and not left for the close to reset. Measured, 100,000
bytes sent during a million-key save met a reset in 4 of 4 runs before the walk read the
queue and an orderly close in 4 of 4 after, on macOS and on Linux. The window
that remains is a limitation of the design, and a half-close from the server's
side does not remove it: with a request unread and replies still held by the
kernel, a close after `shutdown(SHUT_WR)` lost as much as a plain close, and both
ended in a reset at the client. What does work is a close that waits, either for
the kernel's own send queue to empty or for the peer's end of input. The first is
a question put to the kernel that this server does not ask, and it ties the exit
to the pace of the slowest reader, up to the deadline; the second needs the
client's cooperation and has no bound of its own against one that never gives
it. This server does neither. The second limit is the other direction, and it is the
last of the pause's costs above: a request the stop never dispatches is a write the
client was already told had left, whether a pass of the drain read and threw it away or
the last look did. That one is reported, in the drain line's third figure, for
everything either of them read; a request that arrives after the last look is neither
read nor reported. `--write-buffer-high-water 0` removes the pause's share of it — the
whole pipeline, lost with certainty — and not the loss itself: a stop still discards
whatever the loop had not read, which is the loss the ten prompt-stop runs described
above measured, with the pause unable to fire. `docs/DESIGN.md` has the reasoning.

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

**Two more flags cover how fast one connection may ask.** `--rate-limit REQUESTS`
(default `0`) answers `-ERR rate limit exceeded` to a command, and does not run it,
once its connection has been permitted REQUESTS commands inside the last
`--rate-limit-window SECONDS` (default `1`, whole seconds). Permitted, and not
sent: a refusal records nothing, so a connection can send more than REQUESTS
commands in a window: with a limit of 5 and a 2 s window, a client whose 8 sends spanned
3.1 s was permitted 7 of them. Spread is what buys that, not the refusals — the same 8
sends inside one 2 s window were permitted 5 and refused 3, and no window span in any
run held more than 5 permits. A client library's own handshake spends the budget too, and
`redis-py`'s spends two of it: it opens with two `CLIENT SETINFO` commands, which this
server answers `unknown command`, and a refusal of that kind spends the permit exactly as
a dispatched command does. So `--rate-limit N` leaves a `redis-py` client N minus 2 for
its own work, and at `--rate-limit 1` or `2` it cannot run a single user command.
`0` turns the limiter off, and it is off unless asked for: the
benchmark above uses `-c 50`, and a limiter that was on would have measured its own
rejection path. The window slides rather than resetting on a fixed boundary, so no burst
straddles one, and it is counted on the monotonic clock. The count is of commands and
not of reads. A client that pipelines writes many commands in one `send()` and the
server can read them in one `recv()`, so a check made once per readable event would
charge the whole batch one unit and bound how often the socket was read, not how many
requests were made; this one sits inside the loop that dispatches the batch, where
`--write-buffer-limit` is also consulted, for the same reason, and a refusal is queued
and then reaches that same check, so a flood of refusals is held to the limit plus one
reply like any other. Against a real `server.py` process on a port the kernel chose, 500
`PING`s written in one `sendall` were answered with 500 `PONG` and none refused when no
flag was given, and with 100 `PONG` and 400 refused under
`--rate-limit 100 --rate-limit-window 1`.

A refused command gets that error and nothing else. It is not dispatched, the connection
stays open and is still read, and its next command is judged on its own, because a
close would look to the client like a protocol error and the reconnect that followed
would arrive with a full budget. That holds while the queue of replies stays under
`--write-buffer-limit`: a refusal is a reply like any other and the limit applies to
it. What that takes is more refusals than the kernel will absorb, and not many: the
limit is judged after the send, and on loopback ten refusals' 267 bytes leave every
time, so ten do not close the connection and about fifty thousand do. The budget is
per connection and not per address, and that is the cost: a client that closes and
reconnects starts again with a full one. I documented that rather than engineering it
away, since counting per address needs a table shared between connections and this loop
has none. `--max-connections` is not a mitigation: it bounds how many connections, and
so how many budgets, exist at once, and not how often a client may open a new one, and
one client opening connections one after another was permitted 98,281 commands per
second against a limit of 100 per second. A connection whose role is not
`client` is never limited, which changes nothing in a running server today because
nothing sets any other role; [Designed and not built](#designed-and-not-built) says why
the exemption is there.

`--rate-limit-window` takes no `0`. It is the one numeric flag here with no reading of
`0`: every other has one -- off, no drain, resume only when the queue is empty, a port
the kernel chooses -- and on a window `0` could only mean a limiter that permits
everything, since 100 requests per 0 seconds puts every reading outside the window the
moment it is taken, so an operator who typed `0` meaning off would have a limiter
configured that never fires and says nothing. The window refuses `0`, and a negative
value, with a message that names `--rate-limit 0` as what turns the limiter off. A
negative `--rate-limit` is refused as well, both flags at the CLI and again in
`Server.__init__`, and the window takes the same `2**63 - 1` ceiling as the other
durations. `docs/DESIGN.md` has why the clock is monotonic and why a refusal is not a
close.

**Two more flags cover where the server listens and what it says.** `--host HOST`
(default `127.0.0.1`) is the IPv4 address to listen on, or a name that resolves to one.
The default is loopback, so a server is local-only unless asked otherwise: it has no
authentication, and whatever can reach the port can read, overwrite or flush every key.
An empty `HOST` is refused, since it would listen on every interface, and `0.0.0.0` asks
for that by name. `--log-level` (default `INFO`; `DEBUG`, `INFO`, `WARNING` or `ERROR`,
in any case) sets how much is logged, once, in `_parse_and_run()`: importing `server`
configures nothing, so a program that embeds it keeps its own logging. INFO adds a line
for each connection opened and each closed to what WARNING shows. Those two lines have
no count bound of their own, unlike the refusal and task-failure lines above, and each
is a blocking write to standard error on the one thread this server has: a client that
connects and disconnects in a loop can fill a stalled reader's buffer by itself and park
the loop, which is the hazard those bounds exist for. `--log-level WARNING` removes
those two lines, and with them the line a drain that lost no replies writes, which is
also INFO -- and with it the only report there is of the request bytes a stop discarded,
because a stop that loses requests but no replies writes that line and no other. Even
so, `--log-level WARNING` does not remove the hazard: two WARNING lines are the same
kind of writer, one per connection, with no count bound of their own. One is written
when a connection is closed for holding a half-sent command past
`--incomplete-command-timeout`, and a client reaches it by connecting, sending half a
command and waiting out the timeout. The other is written when a connection is closed
for queued replies still over `--write-buffer-limit`, and a client reaches it by asking
for a reply bigger than the limit plus whatever the kernel takes. A client that does
either in a loop still drives a blocking write per connection at WARNING. That is a
residual this server accepts and does not bound: the refusal and task-failure lines
above are counted, these two are not, and the write is the same blocking one on the same
thread. DEBUG adds a line per dispatched command naming it and counting its arguments,
never showing a key or a value, and that call sits behind a level check, so at INFO
nothing is built for it. An unknown level is a usage error and exits 2. Neither flag
takes a number, so neither has a `0` to misread.

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
restart recovers. They are deselected for what they do rather than for what they cost:
each starts servers of its own and waits out a real snapshot interval, so a default run
neither kills a process nor sits on a wall clock. They are not the slow part of the
suite -- all four together are about 6 s, where a default run's own slowest two tests
are about 10 s and 8 s. So `pyproject.toml` deselects them, and
`python -m pytest -m manual tests/test_crash_consistency.py` selects them.

### In a container

```bash
docker build -t mini-redis .
docker run -d --name mini-redis -p 127.0.0.1:7000:6379 -v mini-redis-data:/data mini-redis
until docker logs mini-redis 2>&1 | grep -q 'listening on'; do sleep 0.1; done
redis-cli -p 7000 PING
docker stop mini-redis
```

The `until` line is not decoration. `docker run -d` returns as soon as the container is
created, and a `PING` sent before the server has bound answers
`Error: Server closed the connection` -- 3 of 3 without it. The server prints
`listening on` once the listener is bound, which `run()` does after installing its own
stop handlers, for `SIGTERM` always and for `SIGINT` unless the process inherited that
signal ignored, and so after the snapshot has loaded. That is
what makes it the right line to wait for before sending a command, and the same line the
test suite's own launcher reads; it is a later point than the one `main()`'s handler
marks, which the paragraphs below measure. A stop that arrives during startup prints it
too and then exits, so the line says the port is open, not that the server will stay up.
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

The exec form is not a complete answer. The handler that `SIGTERM` needs exists only
once `main()` has installed it, and the interpreter's own startup and `server.py`'s
imports run before that. The kernel delivers no signal to PID 1 that has only its
default disposition, so a `docker stop` that lands in that window is discarded: the
container serves on to the end of the stop timeout and dies on `SIGKILL` with no save,
and every write it answers `OK` in that time is gone after a restart. Measured on a
400k-key volume, a `SET` was acknowledged and then lost: `docker stop` took 10.28 s, the
container exited 137 with no drain line, and after the restart the `GET` answered nil
and `DBSIZE` was back to 400,000. The window ends when `main()`'s one handler is armed,
and that point does not grow with the keyspace: from the daemon's `StartedAt` to a
marker the container printed once the handler was in place it was 209.0 to 607.4 ms over
14 fresh starts on an empty volume, median 237.2, quartiles 222.8 and 339.7, and the
median on a 400k-key volume was 262.2 ms, 25.0 ms more. The start is read from the
daemon's clock and the marker from the container's own, so each figure carries any
offset between them. `run()`'s two handlers are armed at a later point, after the
snapshot load, and that one does grow: median 242.7 ms on the empty volume and
1,257.8 ms on the 400k-key one, 5.2 times later. A stop that lands between the two is
recorded by `main()`'s handler and honoured by `run()`, so only the first point decides
whether a stop is lost. The starts were taken at load average 6.1 to 7.4 and 14 is a
small sample, so 607.4 ms is the largest seen and not a ceiling, and a quieter machine
should read lower. Waiting for `listening on` before the first command or the first
`docker stop`, as the `until` line above does, avoids it.

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
server.py       entry point, listener, signals, shutdown drain, connection cap, rate limit, water marks, dispatch
event_loop.py   selectors readiness dispatch (on_readable / on_writable / on_accept)
connection.py   per-connection socket, read/write buffers, lifecycle
resp.py         RESP2 parser and serializer
store.py        keyspace, expiry index, pending-effects queue
persistence.py  versioned snapshot format, atomic save/load
ratelimit.py    per-connection sliding-window rate limiter, off by default
replication.py  a docstring and nothing else; specified in full, deliberately unbuilt
commands/       __init__.py, registry.py, server.py, string.py and list.py; twenty-six commands total
Dockerfile      single-service image: non-root user, snapshot in a volume at /data
.dockerignore   the build context is an allowlist of the files the image runs from
tests/          pytest suite, run in CI against Python 3.11 and 3.13; the crash tests are manual
```

[Design notes](docs/DESIGN.md) cover why each of these is shaped the way it is, and the
deliberate differences from real Redis.
