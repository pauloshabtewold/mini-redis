# Design notes

Why mini-redis is shaped the way it is, and where it deliberately differs from real Redis.
Split out of the README so that file stays short. Nothing here is needed to run or read the
code. Every figure below was taken rather than estimated, and the method behind each
one is in [MEASUREMENTS.md](MEASUREMENTS.md) beside this file.

## The protocol layer

**Everything is `bytes`.** A parser built on `str` has to guess an encoding the moment it
reads off the socket, and a value that is not in that encoding comes out corrupted on the
other side. Keys, values, command names and error bodies are `bytes` from the first line of
the parser through the store and into the snapshot format.

**A bulk string's body is delimited by its declared length, never by a search.** A bulk
body may legally contain the `\r\n` pair that ends every header line. A parser that finds
the end by searching truncates such a value and leaves the rest to be re-parsed as a bogus
follow-on command. `resp.py` reads the body as a slice at a computed offset and only checks
that the two bytes after it are the terminator. Header lines are the opposite case — their
length is declared nowhere — so searching is exactly right for them, and only for them.

**A header search carries a resume position.** Each parsing step reports what it consumed
and the buffer length at which another attempt could make progress. A header line cannot
report such a length, so it carries the position its terminator search reached instead.
Without that, locating a header walks the whole buffer on every readable event, which is
quadratic in what one connection has sent — and on a single-threaded loop, every other
client's problem too.

**A malformed byte is refused the moment it arrives.** `*1\r\nX` answers
`ERR Protocol error: expected '$', got 'X'` as the `X` lands, where the reference searches
for the terminator first and says nothing until the line ends. Byte zero cannot
retroactively become `$`, so the verdict is identical and only its timing differs.

## The event loop and connections

**`select()` has a bounded timeout** so a stop signal is noticed on an idle loop. It also
floors both periodic intervals: a deadline is checked only when `run_once()` returns, so
with no traffic a deadline can be noticed up to one timeout late. Under traffic
`run_once()` returns as soon as a socket is ready.

**The loop never asks the selector for an empty events mask.** On the kqueue backend macOS
and the BSDs use, `modify()` with a mask of zero raises `ValueError` and drops the
registration on its way out, where epoll accepts it. So a mask of zero is spelled
*unregistered*, an absent connection reads as zero, and "not registered" is a legal state
rather than a `KeyError` every caller has to guard.

**Partial writes cost nothing.** A reply that cannot leave in one `send()` sits in a
per-connection buffer until a later writable event drains it. Write interest is cleared the
moment the buffer empties — a connection left permanently writable spins the loop at full
CPU without dropping a single reply.

**One exception boundary covers a whole connection.** A single-threaded loop turns every
uncaught exception into a total outage for every client, so the boundary is mandatory rather
than defensive. Anything other than a retryable error closes that one connection and leaves
the server and every other connection running.

**A protocol error carries the commands parsed ahead of it.** When a batch fails partway
through, whatever already parsed out of the same read is answered first, then the error is
queued, then the connection closes — in that order, every time. The commands are carried on
the exception, since the list the parser built is otherwise a local that the raise discards
with its frame.

## The store

**One lookup answers every existence question.** `SET`'s `NX`/`XX`, `INCR`'s
read-modify-write, every type check and a plain `GET` all go through `Store.lookup()`, never
a raw dict test. A key past its deadline is deleted the moment that lookup finds it, so no
handler can see a key its own logic already considers gone. `DBSIZE`, `KEYS` and `INFO`'s
keyspace line are the deliberate exception: counting or listing through `lookup()` would
delete every expired key they scanned past, so all three read the keyspace directly and
filter what they report.

**A list value is a `collections.deque`, never a `list`.** `LPUSH` and `LPOP` are O(1) at
both ends on a deque and O(n) at the head of a list, which is the whole shape of the
command set.

**Deadlines are wall-clock and durations are not.** An expiry deadline is Unix milliseconds
because `PEXPIREAT` is Unix milliseconds by protocol — it has to be a timestamp two machines
read the same way. Every duration, including the rate limiter's window, is
`time.monotonic()`: a wall-clock window breaks under an NTP correction, and in the direction
that *permits* a flood, since a clock stepped forward puts every reading outside the window
at once.

**A past-dated expiry deletes rather than schedules.** `EXPIRE k -1` removes the key inside
the handler rather than writing a deadline the next sweep would act on.

**The sweep is bounded by elapsed time, not an iteration count.** The reference's own
numbers, read off 7.2.7's `expire.c`: twenty keys a pass and a one-millisecond budget. An
iteration cap bounds dict lookups; only a time budget bounds the stall a sampling pass puts
in the p99. The sample is drawn without replacement in O(1) per draw, through an index
beside the expiry map.

**A reply and its effect are different facts.** Every handler returns reply bytes and a list
of the effects a follower would need replayed — `PING` included, where the list is always
empty. What a client sees is not the whole of what happened: a `GET` that finds an expired
key has to produce a deletion nobody asked for, with no write command in the request to
carry it, so that `DEL` goes onto the store's own queue instead. Both lists reach
`server.py` and both are discarded there, because replication is specified and unbuilt.

**The last `LPOP` off a list emits no `DEL` beside the pop.** A follower applies the pop
through its own dispatcher and triggers the identical empty-container cleanup against its
own copy, so a `DEL` would be redundant rather than corrective. The pair is idempotent,
which is exactly why a redundant `DEL` would go unnoticed: it would double the replication
traffic on every list that empties, with no test able to tell.

## Persistence

**The checksum covers the header as well as the entries.** A flipped bit in the key count
or the version is caught by the trailer rather than by a bounds check, and the trailer is
verified before any length prefix is trusted.

**The startup probe costs what it measures.** Growing the probe from a create-and-remove to
a write-and-sync took its own life from a median 0.10 ms to 3.03 ms — long enough that two
servers starting together in one directory see each other's file, which is why the probe's
name ends in a suffix of its own rather than a save's: before that split, one of them warned
that a save had likely been interrupted in 90 of 150 pairs. The device it catches is one
`df` still reported space on and that answered the first save `ENOSPC`.

**The save `fsync`s the temporary file and then the directory.** The bytes and the directory
entry that names them are two separate durability guarantees, and a power cut between them
loses the entry. `fcntl(fd, F_FULLFSYNC)` where the platform has it, `os.fsync()` where it
does not, and a filesystem that cannot sync a directory is tolerated because the snapshot is
already in place by then.

**The real path is never open for writing.** A save writes a temporary file beside the
snapshot and `os.rename()`s it into place, so a crash mid-save leaves the previous snapshot
intact and a file nothing reads. A file left under that name is named in a warning at every
later start and never removed — it may hold the only copy of a newer save.

**The startup check tries the write rather than inspecting the path.** It creates the same
temporary file a save creates, **writes a byte to it and syncs that byte**, then removes it.
The byte is what catches a full device: a probe that only created and removed a file passed
a volume that then answered the first save `ENOSPC`, after which the server answered `SET`
with `OK` and lost the keyspace at the stop. What one byte cannot answer is whether there is
room for the next snapshot, whose size the check cannot know.

**Why the snapshot blocks the loop**, and why not `fork()`: the README's rejected
alternatives cover it. Even a cheap in-process copy does not remove the pause — a pure
Python copy still leaves every list to copy element by element and the encode on the one
thread.

## Backpressure, the cap and the limiter

**A queue of replies has two marks and a ceiling.** One mark would pause and resume a
connection on every event, so there are two and the band between them is hysteresis. Both
interests are decided in one place, and read is decided before write: a paused connection
whose buffer has just emptied wants neither, which is the empty mask the selector cannot
hold, and deciding read first means a buffer that drained to empty has already passed
through the resume.

**The mark is consulted inside the dispatch loop as well as after the batch**, for the same
reason the limit is: a pause decided once per batch would let one `recv()` of pipelined
`GET`s queue the whole batch before anything throttled. What it cannot do is unqueue what
the batch has already parsed.

**The cap on a queued reply closes rather than throttles.** Refusing to read a client that
has stopped reading cannot slow it down: such a client blocks in `send()` waiting for room
only its own reading would create, and both sides wait forever. This is a hard limit in the
reference's sense, and a hard limit closes a connection for holding too much, not for
failing to read.

**The incomplete-command deadline is suspended while a connection is paused.** The server
is what stopped reading it, so a deadline that kept running would close a client for the
server's own decision. Skipping the sweep but leaving the arm time alone closes it the
instant reading resumes; leaving the deadline running closes it while it is paused. So
pausing clears the deadline and resuming arms it afresh, and a client is owed a full timeout
from the moment the server starts reading it again.

**The inbound caps fire at the header, before a body byte is read.** A declared bulk length
over `--max-value-size` is refused on the header line, so an oversized value is never
buffered. The check sits at the framing layer, before an element has a role, which is why
the cap applies to the command name and any key as well.

**The connection cap refuses at accept rather than reserving a slot.** A refused connection
is closed with nothing written, because a reply would be a `send()` from the accept callback,
on a socket nothing tracks, on the one thread there is, for a client that by construction is
not being kept. The count is the connection set's own length — a second counter would
disagree with the set silently, in both directions, the first time a path changed one and not
the other. The comparison is `>=` where every byte limit is `>`: a byte limit compares a
quantity already held against a ceiling it may legally reach, and the cap asks whether there
is room for one more.

**Refusals are bounded by count.** The first of an episode is a warning, the rest are debug,
and a short line is written once per hundred so a run that never ends still leaves a trace.
A client retrying in a loop is refused as fast as it can connect, and logging writes to
standard error with a blocking write on the only thread here. A close cannot end an episode:
a freed slot is what a retrier is waiting for, so with slots turning over one comes between
every pair of refusals and re-arming on a close would warn on each.

**The rate limit is counted per command, not per read.** One `recv()` carries a batch, so a
check made once per readable event would charge thousands of pipelined commands one unit and
bound how often the socket was read rather than how many requests were made. The window pops
readings from the left rather than filtering on read — a version that counted the readings
still inside the window answers every question correctly and keeps one reading per request
for the life of the connection, a leak that behaves.

**A refused command is answered, not closed.** To the client a close is indistinguishable
from the one a protocol error gets, and the reconnect that followed would arrive with a
fresh budget, so the limit would be undone by the way it was enforced. A refusal is a reply
like any other, so a flood of them does eventually reach the write-buffer limit — after as
many as the two kernels' buffers hold divided by the 26 bytes of one refusal. On this host
that is a 408,300-byte receive buffer plus a 146,988-byte send buffer, so 21,357 of them.

## The shutdown sequence

A stop takes the listener out of the select set, saves a snapshot, drains, and tears down —
in that order.

**The save comes before the drain** because a client that never reads holds the drain to its
whole timeout, and an operator who gives up and sends `SIGKILL` should not find that the
snapshot was the thing waiting behind it. The listener leaves the select set first, so
nothing further is served, while the socket stays open until teardown and a replacement
server cannot bind the port.

**The drain reads its clients and dispatches nothing.** Stopping reading looks like the way
to keep it finite and is the wrong one: a request left unread in a receive queue turns the
close that follows into a reset, and a reset discards the closing socket's own send queue —
the replies this process handed its kernel and the kernel had not yet put on the wire. What
keeps the drain finite is that no command runs, so no new reply is queued.

**Two sweeps of the accept backlog, not one.** The kernel goes on completing handshakes into
the listener's backlog for the whole of the save and the drain, and those clients are never
served. They are accepted only to be asked what each holds and closed. The sweep in the
`finally` counts everything the entry sweep does and everything that arrived during the
drain; the entry sweep is kept for its other effect, which is closing a client already
waiting at the stop rather than up to a whole timeout later. One sweep accepted 722
connections where `SOMAXCONN` is 128, and three clients arriving after a stop held 201,000
bytes that a count without the sweeps reported as zero. The sweeps have no deadline of their
own and are bounded by arrivals rather than by the backlog's size: over 58 sweeps on a live
server they ran 0.005 ms to 383.302 ms.

**One descriptor is held in reserve** behind the listener and released immediately before the
shutdown save — not before the first sweep. At a full descriptor table the save cannot open
its temporary file, and a failed save costs the keyspace where a short figure costs a number.
The save closes that file before returning, so the slot is free again by the time the sweeps
run.

**The drain's line counts requests in five shapes**, because the bytes sit in five places:
what a read buffer held when the drain began, what a half-received command had already had
parsed off that buffer — `1,400,019` bytes of one `MSET` reported as zero until that was
added — what a pass of the drain read, what was still unread in a receive queue, and what sat
on a connection the kernel completed into the backlog. The receive-queue shape dominates,
and a count built from the drain's own reads would miss it entirely. The figure deliberately
does not move the line's level, because no threshold separates a half-sent command from an
unanswered request by size: one client caught mid-upload of a declared 8 MiB value held
4,194,334 bytes, more than a whole discarded pipeline of fifty thousand `SET`s.

**The unread receive queue is read off rather than asked about.** `FIONREAD` is asked for a
*ceiling* and the queue is then read up to it, because the ioctl's answer is not portable: a
kernel that walks the queue summing each buffer's whole length answers for the part of one
buffer an earlier `recv()` already took, which the read path has counted once already, so
the five shapes stop being additive. The buffer size that makes the difference visible is
measured rather than derived: one non-blocking `send()` of 10 MiB into a socketpair on Linux
was accepted 438,528 bytes at a time. Emptying the queue also turns the close that follows
into a FIN where it was a reset.

**What the timeout bounds, and what it does not.** It bounds the wait for clients and
nothing else. Measured against a client that never reads, over twelve runs at a load
average of 3.61 to 5.31, `_drain_for` itself ran 5.0011 to 5.1024 s at the shipped
five-second timeout and `SIGTERM` to exit 5.1457 to 5.2740 s — the difference being the
save ahead of it, the two sweeps and the teardown's own flush, none of which the flag
covers. That flush is the last thing to run: over eleven stops with 150 connections each
owed a 1,048,588-byte reply, 157,288,200 bytes queued, readers that began after the signal
still received 40.6% to 91.3% of it. A 400,000-key keyspace of 16-byte keys and 100-byte
values — a 53,200,016-byte snapshot, the same size in twelve of twelve runs — takes 0.3080
to 0.3286 s to save on the way out, which is the scale to hold against `docker stop`'s own
ten seconds.

**A half-close is not a dead peer during the drain.** End of input on a connection that still
owes bytes does not close it: the client is finished asking and still reading, and reading
from it stops because end of input stays readable and would wake every pass. Outside the
drain, end of input is taken for a dead peer.

## Deliberate differences from real Redis

Each of these is a choice, and each was checked against `redis-server 7.2.7`.

- **`SELECT` is not implemented.** There is one keyspace, which `CONFIG GET databases`
  answers truthfully with `1` against the reference's `16`, so there is no index to select.
  The cost falls on the client: `redis-cli -n 1 SET k v` prints the failure on standard error
  and then writes database 0 anyway.
- **An unknown command answers `ERR unknown command '<name>'`** and never appends the
  arguments. That does not make this server's message the shorter one: the reference renders
  the name through `%.128s` and tops out at 181 bytes, where this echoes the name whole,
  bounded only by `--max-value-size` — a 200,000-byte command name answers 200,025 bytes
  here.
- **`HELLO` takes the protocol version and nothing else.** Anything but `2` answers
  `NOPROTO unsupported protocol version`, where the reference has a parse error for a
  non-numeric argument and accepts `3` as a second protocol. `HELLO 2 AUTH u p` is an arity
  error here and `WRONGPASS` there; `HELLO x y` is an arity error here and a version parse
  error there, because this counts its arguments before it looks at any of them.
- **A negative multibulk count is a protocol error.** `*-1\r\n` gets
  `ERR Protocol error: invalid multibulk length` and the connection closes, where the
  reference consumes any count of zero or less without replying. A negative *bulk* length is
  rejected by both.
- **A bulk body must be followed by its terminator.** `*1\r\n$4\r\nPINGXX\r\n` declares four
  bytes and supplies six, which this refuses with
  `ERR Protocol error: unterminated bulk string`; the reference advances by the declared
  length, answers `PONG` and treats the stray bytes as an empty inline command.
- **Every `\r` that ends a header must be followed by `\n`.** The reference finds the `\r`
  and steps two bytes past it without looking, so `*1\rX$1\r\na\r\n` parses there and is a
  framing error here. Three positions, one rule, and this server is the stricter.
- **A NUL is a byte like any other here and a string terminator there.** A NUL inside a
  multibulk command name is part of the name, where the reference's C string conversion stops
  at it; a NUL in multibulk *framing* is a protocol error here and silence there, which
  leaves a client waiting on a reply that never comes. Binary-safe here, C string there.
- **The inbound caps are divergences, not ports.** `--max-value-size` defaults to 64 MiB
  against the reference's 512 MiB `proto-max-bulk-len`; `--max-multibulk` has no reference
  equivalent at all, since the reference refuses nothing below `INT_MAX` and reserves space
  incrementally. A length exactly at either cap is accepted — the comparison is `>` on both
  servers.
- **A terminated inline line over 64 KiB is refused** with the reference's own
  `ERR Protocol error: too big inline request`. The reference's own refusal is a band and not
  a boundary: its check runs only on a read that has not yet found a newline, and it reads in
  16 KiB quanta, so every line up to 80,896 bytes was answered and every line from 98,303
  bytes up was refused, with both answers in between.
- **An unterminated line is never refused for its size**, in any of the three places one can
  run on. A line with no terminator yet has no declared length to compare against anything;
  `--incomplete-command-timeout` bounds it in time instead.
- **`EXPIRE`, `PEXPIRE` and `PEXPIREAT` take exactly three arguments.** The reference takes
  `NX`, `XX`, `GT` and `LT` after the key; this accepts none of them, so there is no minimum
  to leave room for and `EXPIRE k 10 NX` is an arity error.
- **`LPOP` and `RPOP` take a key and nothing else.** The reference accepts an optional count,
  which also changes the reply from a bulk string to an array.
- **`FLUSHALL` takes no arguments.** This loop is single-threaded, so the reference's `ASYNC`
  and `SYNC` would name the same operation.
- **`DBSIZE`, `KEYS` and `INFO`'s keyspace count all filter logically-expired keys.** The
  reference filters in `KEYS` alone: with two expired keys resident it answers `3` to
  `DBSIZE` and returns one key from `KEYS *`. Two introspection commands disagreeing about
  one number is worse than either answer alone.
- **`CONFIG GET` answers from a table of the five parameters this server genuinely has**, and
  every value is a true statement about the process rather than real configurability. `save`
  is read from the running server and spelt in the reference's syntax, where a rule fires once
  a clock is *more* than its seconds past the last save — so a one-second interval reads
  `0 0`, which the reference's own parser would refuse. A default-configured reference answers
  `3600 1 300 100 60 10000`. Only `GET` is implemented; every other subcommand answers
  `ERR syntax error`.
- **`INFO` reports eight fields and no persistence section.** What a save is doing is a
  property of the process rather than of the keyspace a client reads, and the reference's
  persistence section reports mostly on a background save this server has no equivalent of.
  Three of the eight — `blocked_clients` and two counters — exist because `redis-cli --stat`
  reads exactly those and renders a missing field as `LLONG_MIN`. The two counters diverge
  deliberately: a connection the cap refused is counted as received, and a command the rate
  limiter refused as processed, because a saturated server and a limited client should not
  read as idle.
- **`redis_version` reports this project's own `0.1.0`.** The field name is the reference's
  because tooling reads that key; the value is this server's because it is not
  `redis-server 7.2.7`, and saying otherwise would make the one field a client trusts the one
  field that lies.
- **A stop signal saves even when periodic saving is off.** A reference started with
  `--save ""` writes nothing on `SIGTERM`; here `--snapshot-interval 0` disables the schedule
  only. The one pairing that skips the stop's save is `--ignore-snapshot` with that interval,
  where the file is being left alone on purpose.
- **A stop waits for the replies already queued.** The reference does not: a client owed a
  great deal saw the process exit at once and received only what its kernel already held.
  A reply already computed is not thrown away by a stop; the cost is that a client which
  never reads holds the exit to the whole timeout.
- **A connection over the cap is closed with nothing written.** The reference answers
  `-ERR max number of clients reached` and then closes. The silence is deliberate and the
  cost falls on the client, which can see that it was refused and not why.
- **An ordinary client's output buffer is limited by default.** The reference leaves it
  unlimited (`normal 0 0 0`); `--write-buffer-limit 0` is that behaviour.
- **A client that stops reading is paused, not closed.** The reference keeps reading it and
  closes it at whatever hard limit is configured.
- **A command that takes too long to arrive is closed.** The reference has no such deadline:
  a `PING` held half-sent for eight seconds was answered when the rest arrived, its `timeout`
  directive reads `0`, and the only bound on a half-sent command is
  `client-query-buffer-limit` at 1 GiB.
