# mini-redis

A Redis clone in Python: a single-threaded event loop speaking the RESP2 wire protocol
over TCP, with **zero runtime dependencies**. Twenty-six commands, strings and lists, TTL
expiry, snapshot persistence, backpressure, a connection cap and a per-connection rate
limiter.

This is `v1` and it is finished. Leader-follower replication is specified in full and
deliberately unbuilt; [Designed and not built](#designed-and-not-built) says what that
leaves in the code. `v1` and `v1.5` are tags and the tree has moved past both of them, on
purpose — every commit since repairs something found in the closing commits or corrects
something this page claimed.

## Quickstart

Python 3.11+, and `redis-cli` to follow the transcript (`brew install redis`, or
`apt-get install redis-tools`). It is not a runtime dependency; there are none.

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

The `listening on <host>:<port>` line at startup is what tells you *this* server
answered, and it names the address actually bound. It goes to standard output; every other line goes to
standard error. Wait for it before sending a command. On the default 6379 a real Redis
already there means either `Address already in use` or, worse, a healthy-looking `PONG`
from that server instead.

```bash
.venv/bin/python -m pytest
```

A healthy run ends `4 deselected` — the four crash-consistency tests, which kill a real
server with `SIGKILL` and are selected by `-m manual`. The skip count before it belongs to
the platform: one on macOS, nine on Linux, where the BSD file-flag and ACL facilities this
server uses are absent. `-rs` prints the reason for each.

An installed copy runs as `python -m server`; there is no console script. The modules
install at the top level under generic names, so give it a virtual environment of its own.

```bash
.venv/bin/pip install -e '.[build]'
.venv/bin/python -m build
```

## Benchmark

`redis-benchmark 7.2.7` against this server and against a private `redis-server 7.2.7`,
each on its own port, one Apple M2 on macOS 14.2.1 over loopback, this server under
CPython 3.13.1 at `--snapshot-interval 0 --log-level WARNING`. The reference ran with
`--save "" --appendonly no`. 100,000 requests per test over 50 connections:

```bash
redis-benchmark -h 127.0.0.1 -p <port> -n 100000 -c 50 -P 1 -q \
  -t ping_mbulk,set,get,incr,lpush,rpush,lpop,rpop,lrange_100,lrange_600
```

| test | this server | redis-server 7.2.7 | % of native | p99 (ms) | ref p99 (ms) |
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

With `-P 16`, sixteen requests in flight per connection:

| test | this server | redis-server 7.2.7 | % of native |
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

**Take the `-P 16` figure as what the design costs.** At `-P 1` both servers spend most of
each request waiting on loopback, so that ratio measures the network about as much as the
server. Pipelined, the eight small-reply commands run at 16.3% to 29.7% of native and the
two `LRANGE` tests at 46.9% and 59.7% — that is what a single-threaded Python interpreter
costs per command.

**Each cell is one run, and one run is worth less than it looks.** Re-running moved
individual rates by as much as twenty per cent and some p99 figures by half again. What
survived every repetition is the shape rather than the cell: the median share held within a
few points, and the 8% ship threshold I fixed before measuring was cleared in every row of
every run, worst case 8.18%. Read a row as an order of magnitude, not a number to compare
against your own hardware. I did not profile: `py-spy` needs root on macOS, nothing here
runs as root, and the profiling session existed only for a result that missed.

## Three findings

Each came from testing something that looked fine, and none is visible in the code.

**A length-prefixed format does not make a checksum redundant.** Every length prefix is a
bounds check, so truncation is refused at every offset — but a flipped bit mostly is not.
Sweeping every bit position of a 2,036-byte snapshot of 41 keys, 16,288 positions, and
decoding with the trailer recomputed over the corrupted bytes, which is what no checksum
amounts to: **12,942 of 16,288, 79.4573%, were silently accepted as a snapshot holding
different data.** 3,314 were caught — 2,759 by a length prefix running off the end, 280 by
an unknown type byte, 202 by a duplicate key, 32 by a version, 32 by a magic, 8 by trailing
bytes, 1 by an empty list. The last 32 are the CRC trailer itself, where recomputing
overwrites the flip. With the trailer *verified* instead, every flip is refused:
94,960 positions, 94,960 refusals, over eight fixtures. The accepted share is the payload
byte share, and most of a snapshot is payload: a three-key fixture accepts exactly 50.0000%,
its 51 payload bytes of 102. `HELLOWORLD` came back `HELLOWOVLD`; an expiry of
1700000000000 came back 1699995805696.

**A bare `send()` is not a write path.** Against a copy whose only write path was one
`send()`, a long pipeline of `ECHO`s written before the client read a byte lost a large
share of its replies, a different share every run — one arrived cut in half and almost
every later one was never sent, because `send()` on a full socket raises `BlockingIOError`
and writes nothing, so what is lost is a whole reply. Nothing about it is visible until a
client's burst outruns its own reading, which is what every pipelining client library does.

**`redis-benchmark` drops an unrecognised `-t` name and still exits 0.** On 7.2.7,
`-t nosuchtest` printed one newline and exited 0; the pinned list with `lrange_100` misspelt
ran nine tests and printed a table that looks complete. A missing test is invisible in a
table of results, so neither the exit status nor the line count is a check. I read the
labels: split on `\r` as well as `\n`, discard the `LPUSH (needed to benchmark LRANGE)`
seeding lines, then require each of the ten names to begin a remaining line.

## Limits

This is the long answer to "what does it do under pressure", one flag at a time. Every
figure here was measured on the tree that ships.

| flag | default | what it does |
| --- | --- | --- |
| `--write-buffer-limit` | 32 MiB | closes a connection whose queued replies still exceed it after the kernel has taken what it will; `0` removes it. A divergence: the reference leaves an ordinary client unlimited |
| `--write-buffer-high-water` | 1 MiB | above it a connection stops being read; `0` disables the pause. A non-zero limit must be above it, or neither mark could act, and that pairing is refused |
| `--write-buffer-low-water` | 256 KiB | reading resumes at or below it; with the pause on, `0` means resume only when the queue is empty |
| `--incomplete-command-timeout` | 30 s | closes a connection holding a partly sent command for longer, counted from when the command began; suspended while the connection is paused |
| `--max-value-size` | 64 MiB | refuses one inbound bulk element — the command name and any key included — before a body byte is read |
| `--max-multibulk` | 1,048,576 | refuses a command declaring more elements |
| `--max-connections` | 1024 | closes, with nothing written, a connection arriving while that many are connected. `>=`, so a cap of 50 admits 50 |
| `--rate-limit` | `0` (off) | answers `-ERR rate limit exceeded` to a command once its connection has been permitted that many inside the window, without dispatching it and without closing |
| `--rate-limit-window` | 1 s | the sliding span, on the monotonic clock. The one numeric flag with no reading of `0` |
| `--snapshot-interval` | 60 s | saves to `--snapshot-path` (default `./dump.mrdb`) every interval and on a clean stop; `0` stops the periodic save only |
| `--ignore-snapshot` | off | starts with an empty keyspace without reading `--snapshot-path`, whether or not it is readable, so a good snapshot is discarded as readily as a damaged one. With `--snapshot-interval 0` beside it no save runs at all and the file is left untouched, which is the escape hatch for a snapshot the server will not load |
| `--expiry-sweep-interval` | 100 ms | the active sweep's own floor. Nothing under about 100 ms is reachable: the loop's `select()` timeout is 0.1 s |
| `--shutdown-drain-timeout` | 5 s | bounds the wait for clients on the way out. `0` is the restrictive reading — no read pass — where `0` on the limits above is permissive |
| `--host` | `127.0.0.1` | loopback, because there is no authentication. An empty host is refused; `0.0.0.0` asks for every interface by name |
| `--log-level` | `INFO` | `INFO` adds a line per connection opened and closed; `DEBUG` a line per dispatched command, never a key or a value |

Every numeric flag but `--port` refuses a negative value at the CLI and again in
`Server.__init__`, and the durations refuse anything above `2**63 - 1`.

**Nothing bounds how much memory a client can use.** No cap on key count, no cap on
keyspace size, no eviction policy. A client with nothing but `SET` can grow the process
until the host runs out. The read buffer is uncapped too: an unterminated command grows it
for as long as a client keeps sending, which `--incomplete-command-timeout` bounds in time
rather than in size.

**A value can be stored and not read back.** `--max-value-size` is 64 MiB and
`--write-buffer-limit` is 32 MiB, and nothing relates the two, so a `SET` of a value whose
reply exceeds the limit is accepted and the `GET` that follows closes the connection — the
client gets as much as the kernel took and then silence, which is indistinguishable from a
crash. The reply is the value plus its `$<length>` line and terminator, so the range begins
at 33,554,420 bytes. A list or a keyspace does worse: `LRANGE 0 -1` and `KEYS` are each one
reply judged whole, and list length is uncapped, so elements far under the cap can be
unreadable at any finite limit. Only `0` reads every one back.

**Whether a slow client is closed depends on the size of its replies, not on whether it is
reading.** The pause stops reading a connection over the high-water mark, but the batch
that crossed the mark is still dispatched whole, and a resume takes up to one `recv()` —
64 KiB — of what piled up. For small replies that product is small: a client asking for a
2 KiB value and draining 64 KiB every 20 ms was never closed over 25 s, where the same
client with the pause off was closed inside a second. For 1 MiB replies the pause postpones
the close rather than preventing it, 4.4 to 5.3 s against 2.7 to 2.8 s. A client that writes
every request before reading any reply — `redis-py`'s `pipeline()` — hangs instead, and
nothing closes it. `--write-buffer-high-water 0` is what lets those complete.

**A clean stop saves, and a crash loses the window between two saves.** On `SIGTERM`, and
on `SIGINT` unless this process inherited that signal ignored, the server saves a snapshot
and then spends up to `--shutdown-drain-timeout` sending the replies already queued. The
interval is counted from when a save finishes, which is the rule `CONFIG GET save`
publishes and the reference's own — a default server answers `59 0`. The save blocks the
one thread that answers commands: 100,000 keys of 16-byte keys and 100-byte values is a
13,300,016-byte payload, 64 to 78 ms to serialize and 83 to 94 ms to read back.

**A stop throws away requests, and says how many.** The drain reads and discards what
arrives while it runs, because a request left unread turns the close into a reset and a
reset discards what the kernel had not yet sent. Its one log line carries two counts of
replies — connections closed while still owed bytes, and connections still owed bytes at
the deadline — and a third figure for requests: every inbound byte the stop threw away
undispatched, counted in five shapes. The line is a warning when a reply was lost and
informational when none was. That word is about replies alone: a stop that discarded
1,588,890 bytes of a pipeline and lost no reply still reads `complete`, and the figure is
what says otherwise.

**A refused command costs a permit and nothing else.** The rate limiter's budget is per
connection, so a client gets a fresh one by reconnecting — one opening connections in
sequence was permitted 98,281 commands per second against a limit of 100. `--max-connections`
bounds how many budgets exist at once, not how often a client may open one. A client
library's handshake spends the budget too: `redis-py` opens with two `CLIENT SETINFO`
commands, which this server answers `unknown command`, so `--rate-limit N` leaves it N − 2.

**Client libraries.** `redis-py` needs `protocol=2`; the default constructor sends
`HELLO 3` and this server answers `NOPROTO`. `.incr()` and `.decr()` send `INCRBY`/`DECRBY`,
which are not implemented — `execute_command("INCR", k)` reaches it. `pipeline()` defaults
to `transaction=True` and fails on `EXEC`; `pipeline(transaction=False)` works.
`redis-cli` works for ordinary commands and `--pipe`, `-x`, `-r`, `--no-raw`, `--stat` and
`--latency`. The keyspace-walking modes do not work at all: `--scan`, `--bigkeys`,
`--memkeys` and `--hotkeys` each print `SCAN error: ERR unknown command 'SCAN'` and stop.
`--eval` needs `EVAL`, and `--rdb` sends two `REPLCONF`s before it reaches `SYNC`.

## Designed and not built

Leader-follower replication is specified in full and deliberately unbuilt. `replication.py`
holds a docstring and nothing else. That is a decision: I cut it at the point where stopping
left nothing half-built, and I do not intend to build it. The rate limiter was cut in the
same decision and built afterwards; `v1` does not contain it and the tree does.

Four things are in the code and put to no use by a running server, and I would rather say so
than leave them to be found. `Connection` knows three roles — `client`, `follower` and
`leader_link` — and the rate limiter exempts the last two, but nothing sets them, so the
exemption runs on every command with nothing to apply to. The `replication_state` slot holds
`None`. Every handler returns its reply with a list of the effects a follower would need
replayed; `server.py` reads that list and throws it away, together with the store's own
queue of expiry deletions. And `PEXPIREAT` is registered for a stream that does not exist —
it works as an ordinary command, and it is also the absolute form `EXPIRE` and `PEXPIRE`
report as their effect, so a follower applying it later would compute the same deadline.

## Alternatives I rejected

**Several processes behind `SO_REUSEPORT`** is the obvious way past the GIL and perhaps
twenty lines, and every process would hold its own keyspace — a `SET` on one connection
invisible to a `GET` on another, which is several unrelated servers sharing a port rather
than one faster server. One thread also makes every command atomic by construction.

**`fork()` for the snapshot**, which is what real Redis does and the right answer at scale,
needs a process boundary and a way back, and it interacts badly with a single-threaded
parent's socket state. The alternatives that stay in one process and do not fork tear: a
write landing mid-save produces a file that decodes cleanly into a keyspace that never
existed. So the pause is accepted whole and priced.

**`PSYNC`-style partial resync** would have let a reconnecting follower say where it
stopped, and needs the leader to keep a backlog and do offset bookkeeping. It demonstrates
nothing the full sync does not.

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
tests/          test suite, run in CI against Python 3.11, 3.13 and 3.14
scripts/        check-the-front-page.sh: clones this repository and runs the commands on this page
docs/           DESIGN.md, why each module is shaped this way; MEASUREMENTS.md, how every figure was taken
```

### In a container

```bash
docker build -t mini-redis .
docker run -d --name mini-redis -p 127.0.0.1:7000:6379 -v mini-redis-data:/data mini-redis || exit 1
started=$(docker inspect -f '{{.State.StartedAt}}' mini-redis)
deadline=$(( $(date +%s) + 30 ))
until [ "$(docker inspect -f '{{.State.Running}}' mini-redis)" = true ] &&
      docker logs --since "$started" mini-redis 2>&1 | grep -q 'listening on'; do
  [ "$(docker inspect -f '{{.State.Running}}' mini-redis)" = true ] ||
    { docker logs mini-redis; exit 1; }
  [ "$(date +%s)" -lt "$deadline" ] ||
    { echo 'gave up waiting for listening on'; docker logs mini-redis; exit 1; }
  sleep 0.1
done
redis-cli -p 7000 PING
docker stop mini-redis
```

Every line of that `until` loop is load-bearing, and each is there because the version
without it failed. `docker run -d` returns as soon as the container is created, and a `PING`
sent before the server has bound answers `Error: Server closed the connection`. `--since`
scopes the grep to this run, because `docker logs` is cumulative and an unscoped grep
matches the *previous* run's line the moment you `docker start` a container you had stopped.
`|| exit 1` catches the name conflict when you re-run the block. The `Running` test sits
ahead of the grep, so a match on a stopped container cannot short-circuit it. And the
deadline bounds the loop, because with `--log-driver none` set daemon-wide `docker logs`
cannot read at all.

The `CMD` is in exec form so the server is PID 1 and receives the `SIGTERM` that
`docker stop` sends. **That is not a complete answer.** The handler exists only once
`main()` has installed it, and the interpreter's own startup and imports run before that;
the kernel delivers no signal to a PID 1 that has only its default disposition. A
`docker stop` landing in that window is discarded, the container serves to the end of the
stop timeout, dies on `SIGKILL` with no save, and a write it answered `OK` is gone after a
restart — reproduced on both a 400k-key volume and an empty one. The window is interpreter
startup and does not grow with the keyspace: a median 177 ms from the daemon's `StartedAt`
to the handler being armed, against 184 ms for the handler `run()` installs after the
snapshot load on an empty volume and 559 ms on a 400k-key one. Waiting for `listening on`
avoids it.

One container at a time per volume: the snapshot path is fixed by the `CMD`, so two
containers over one volume both answer `OK`, both exit 0, and the first one's acknowledged
write is gone after a restart. A fresh named volume is the mount that always works — a
volume takes its ownership from the image's `/data` only when it is empty at first mount, so
one another image populated first is refused with `error: cannot write snapshot
/data/dump.mrdb: /data is not writable`. Exit 137 has two causes here: the stop timeout, and
the OOM killer when `--memory` is too small for the snapshot, which leaves an empty log.
`docker inspect -f '{{.State.OOMKilled}}'` tells them apart.

---

[Design notes](docs/DESIGN.md) cover why each module is shaped the way it is and every
deliberate difference from real Redis. [How the figures were taken](docs/MEASUREMENTS.md) is the
method behind every figure in both files -- what to run, what the reading depends on, and what a
second one should be expected to agree with, including the handful it names as published without a
first reading I can hand anyone. The observations themselves, and the log of every defect that
escaped a gate, are kept outside this repository.
