# How the figures were taken

Nearly every number in `README.md` and `docs/DESIGN.md` is something that was measured. This
file is the method behind each one: what to run, on what, and what a second reading should be
expected to agree with. It deliberately carries no figures of its own. Every value this project
publishes has one home in one of the other two documents, a rule a gate enforces, and a method
file that restated the values would be a second copy of each of them going stale on its own
schedule. So read an entry here beside the figure it describes, not instead of it.

It also carries no raw data. The observations behind these figures were taken in a working log
that is not published, which means a re-take is the only way to check one. That is the point of
this file: before it existed, the front page asserted and nothing a reader had access to
substantiated.

## How to read an entry

Each entry says what **kind** of claim the figure is, because that decides what agreement means:

- **a band** -- the minimum and maximum of a stated number of runs. A re-take agrees if it lands
  in the band or moves it outward for a reason you can name. Nothing here claims a band is a
  bound.
- **a largest seen** -- one observation, kept because it demonstrates that something is possible
  rather than that it is typical. A re-take will usually come back smaller, and that is not a
  disagreement. Where a figure is of this kind it is marked here, because it is the shape most
  easily misread as a capacity.
- **arithmetic** -- computed from a flag default or a fixture size, not observed. It cannot
  disagree; it can only be wrong, and you can check it with a calculator.
- **a host fact** -- what this machine's kernel, filesystem or interpreter answered. Your host
  will answer differently and both answers are right.
- **a source fact** -- read out of real Redis's own source rather than measured.

Two things decide almost every re-take and are named per entry: **the load average**, because
several of these figures move with it by more than the band they are published in, and **the
platform**, because this server uses BSD facilities where they exist and the two kernels differ
on what an unread socket holds.

## The figures in README.md

### The benchmark tables

A band of one run per cell -- each cell is a single `redis-benchmark` invocation, which the
section says in its own words. The invocation, the host, the interpreter and the flags both
servers ran under are published with the tables; run that command against this server and
against a private `redis-server` of the same version, each on its own port, and compare. Do not
use a reference server you did not start: an installed daemon's own configuration decides its
side of every row. Run the long form and not `-q`: the quiet form prints a rate and p50, so the
table's p99 columns cannot come from it, and the page published the quiet form for its own p99
figures until 2026-10-14.

Everything the section says underneath the tables comes from paired runs -- both servers driven
in one run, alternating which goes first, so the load is the same for both legs -- and those
captures are kept. Read the shares, the median and the lowest row against the load they carry:
load is the one thing these figures are sensitive to, and it moves them further than any of the
bands here. The ship threshold is the lowest share in its own set and not a floor.

### The bit-flip sweep

Arithmetic over an exhaustive sweep, and the one figure cluster here that is fully reproducible:
no randomness, no server, no network. Encode the fixture the section describes, flip every bit
of the blob in turn, and decode each result; the counts follow. Three tests in
`tests/test_persistence_properties.py` pin the sweep's own totals, so `python -m pytest
tests/test_persistence_properties.py` is a faster check than re-deriving them. The eight-fixture
total is not pinned by a test and needs the sweep run over all eight.

### The cost of a snapshot

A band over repeated runs, each in a fresh child process so that the resident-memory reading is
that run's own maximum rather than a running one. Build the keyspace the section names, time
`encode` and `decode` in process, and read peak resident memory from the child after reaping it.
Two host facts decide the memory figure: peak resident size is reported in bytes on macOS and in
kibibytes on Linux, and the payload size must come out exactly as published or the fixture is not
the same fixture.

### The container's startup window

A band of medians over fresh container starts at a stated load average, measured from the
daemon's own start timestamp to a marker the server prints once its first signal handler is
armed. Take it at two load averages: it moves with load by more than the band. The second figure
in that section is the same measurement over a volume already holding the
large keyspace that section names, which is what makes the window grow. A third figure, the same
handler on an empty volume, was struck on 2026-10-14: nothing in the log derived it, and
re-taking it needs a container daemon this host is under instructions to leave stopped, so it was
removed rather than left standing on nothing.

What this section does **not** promise is that a stop at a given delay is honoured, and the
attempt to make that exact failed for a reason worth knowing before you try it: the kill time
comes from the daemon's clock and the marker from the container's, and bracketing that offset
gives a window wider than every margin being measured. What is demonstrated instead is the loss
itself -- a write answered and then absent after a restart -- which needs no clock comparison.

### The write-buffer pause and the close times

A band over a few runs per arm, at an unstated load average, which is the weakness of these two
figures: a re-take at a different load moves them. Pipeline reads of one value size, drain a
fixed amount on a fixed period at the client, and change nothing between the two arms but
`--write-buffer-high-water 0`. Both arms must use the same reader, because what is being
compared is the pause and not the client. The two value sizes are in the section because the
answer depends on the size of the replies, which is the whole finding.

### The drain's discarded-request-bytes shapes

Each shape is one measurement, taken by sending that shape of traffic and reading the count off
the one line the drain writes. The shapes are a pipeline nothing reads, a multibulk that declares
more elements than it sends, traffic that arrives after the signal, and a client caught
mid-upload of a declared value. `--shutdown-drain-timeout 0` is the arm that matters: it ends the
drain before a single read, which is what showed that the receive queue dominates and that a
count built from the read path alone reports nothing for a discarded pipeline.

### The rate limiter under reconnection

One measurement: open sequential connections in a loop against a small `--rate-limit` and count
what gets through per second. The second figure in that paragraph is a property of one Python
client's own handshake and will differ with any other client; it is published because the page
discusses that client.

### The oversized-reply boundary

Arithmetic, from two flag defaults and the framing a reply carries. No run.

### The suite's published ending

The deselected count is a promise about this repository's own configuration. The skip counts are
host facts: this server uses BSD facilities where they exist, so a Linux run skips the tests that
need them and macOS skips one. `sh scripts/check-the-front-page.sh` runs the page's own
invocation and reports the skip count rather than asserting it, which is how those two numbers
are kept honest; the Linux count comes from CI.

### The container's volume and exit-status facts

Two container runs and an inspection. The not-writable refusal comes from running the image over
a volume another image populated with a different owner. The out-of-memory exit is a run under a
memory limit over a large volume, and the only way to tell it from any other exit of the same
status is the daemon's own out-of-memory flag, since the log is empty either way.

## The figures in docs/DESIGN.md

### The refusal-flood capacity

A host fact and then arithmetic. Read the send and receive buffer sizes off a loopback pair on
your own machine; the refusal count follows exactly from those two numbers and the size of one
refusal, which is why no load can move it and why a re-take on a host with different buffers
gets a different number without anything disagreeing. The experiment that raised the buffers
deliberately is recorded as refuted, so if your count does not follow from your own buffer sizes,
suspect the measurement and not the arithmetic.

### The accept-backlog sweeps

The capacity figure is **a largest seen**: one sweep, kept because it demonstrates that the sweep
is not bounded by the listen backlog. A later set of floods never came close to it, so a re-take
should expect the backlog's own size, not that number. The timing band is a band over sweeps
instrumented inside the counting function itself -- there is no flag that exposes it -- taken with
several processes connecting continuously at `--shutdown-drain-timeout 0`. The range is all the
page gives, and deliberately: this figure used to carry a median too, struck on 2026-10-14,
because a median over sweeps is a statement about how often clients arrive and the record behind
it never said. The two ends of the range are what carry the claim, which is about the absence of a
bound rather than about a typical sweep.

### The drain's own timings

A band over runs with one client that never reads, at the shipped timeout, re-taken on
2026-10-14 with its records kept. Two different measurements, which is why the page gives two
bands: the drain's own duration is reachable only from inside the process, by wrapping the method
and timing it, while the signal to the process's exit is black box. The gap between them is the
save, the two backlog sweeps and the teardown flush. Stall the client with more owed than the
water marks would ever let through, which means turning the limit and the high-water mark off for
the run, or the pause ends the stall before the drain begins and the drain is timed against
nothing. The maximum sits just over the bound the mechanism gives, which is the interesting part
of it.

### The teardown flush after the drain's line

A band over repeated stops, each with the number of connections that section names, every one
of them owed one large reply and whose readers begin only after the signal. Count the bytes that arrive after the stop
and compare them against what the drain's line reported as still owed. `--shutdown-drain-timeout
0` is the arm, because the figure is about what the teardown delivers after the drain has
reported.

### The save a stop writes over a large keyspace

A band over runs, re-taken on 2026-10-14 with its records kept: build the keyspace at the key and
value sizes the page names, then time one save. The byte count beside it is arithmetic from those
two sizes and the format's own header and trailer, so it is checkable without running anything --
and it is worth checking first, because a fixture of another shape gives a byte count that does
not match and is then measuring a different keyspace. It came back identical in every run.

### The kernel's socket-buffer answers

Host facts, and the one cluster here that **cannot be re-taken on this machine**: the figure the
section attributes to Linux is a Linux answer, and macOS answers differently by a factor. Take it
with one non-blocking send of several mebibytes into a socket pair, recording what each call
accepts, and name the platform with the result. The portability problem this cluster exists for is
checkable without either kernel: substitute the other kernel's answer, derived from the real one
so it is wrong by construction, and require the reported figure to equal what the peer sent.

### The active expiry sweep's constants and its floor

The two constants are source facts, read out of real Redis's own expiry code. The floor is one
measurement at three different intervals, all of which give the same cadence because the event
loop's own select timeout decides it.

### The startup write probe

Bands over many trials, and the one cluster that needs a filesystem you have to build. The window
figure comes from interrupting the server inside the bracket around its bind line over at least
dozens of trials, on both the tree with the repair and the tree without it, counting the probe
files left behind. The false-warning figure comes from starting two servers in one directory over
many pairs. The out-of-room arm needs a small filesystem image filled until a one-byte write is
refused -- a device that reports space available and refuses the write is the condition the probe
exists for, and no amount of load will produce it. How much space that device still reported is
not published here any more, struck on 2026-10-14 with nothing in the log behind it; the figure
survives in the comment above the probe, where it is the observation that argues for writing a
byte rather than a claim a reader would act on.

### The deliberate-differences list

Every row is a differential against a real `redis-server 7.2.7` driven at the wire level beside
this one, command by command. Start your own reference on its own port; one you did not start has
its own configuration and may also be in use. Two rows need the reference started with its debug
command enabled, which is not its default.

## Figures I cannot hand you a first reading for

The working log behind this project kept the derivation for most of what it published and, for
six figures, did not. That was found while writing this file on 2026-10-13 and dealt with on
2026-10-14: three were re-taken with their records kept -- the drain's own band, the band from the
signal to the process's exit, and the duration of the save a stop writes over a large keyspace,
all three now carrying the load they were taken at -- and three were struck, each for a reason the
entry above gives: the accept-backlog median, the container's empty-volume startup figure, and the
space the out-of-room device still reported.

What is left is the one that cannot be fixed by either route:

- **the cells of the two benchmark tables.** Each is a single run of a tool whose own output moves
  by a fifth between runs, so no re-take verifies a cell; the table is the `v1` reading and its
  captures were not kept. What stands behind the section is the reproduction published underneath
  it, whose captures are kept, and the page says which is which. A re-take of the table is a new
  table, not a check on this one.

## Where a later reading exists that the page does not carry

Three clusters were re-measured after they were published, and the page kept the original because
the original was not wrong: the container's startup window at a second load average, the
write-buffer close times at a stated load, and the accept-backlog capacity over a set of floods
that never reached the published observation. In each case a re-take will land outside the
published band for a reason that is about load or about the shape being a largest seen rather
than about the server having changed.
