"""The Connection class: owns the socket, the read and write buffers, the role, and the only outbound API."""

import array
import enum
import fcntl
import itertools
import socket
import termios

import resp

# One recv() per readable event, never a loop
RECV_SIZE = 65536


def unread_in_kernel(sock: socket.socket) -> int:
    # how many bytes the peer has sent that this process has not read, asked of the kernel rather
    # than read out of it, so it costs one syscall and takes nothing out of the receive queue.
    # 0 rather than a raise, because the callers are assembling a figure for one log line at a
    # stop and a report that raises on the way out is worse than one that is short: OSError for a
    # kernel that refuses the question on some descriptor this never sees, and ValueError because
    # that -- not OSError -- is what ioctl raises for a socket object whose descriptor is already
    # closed, which is reachable from a scripted socket in the suite and would otherwise escape
    # the one finally clause the drain writes its line from
    held = array.array("i", [0])
    try:
        fcntl.ioctl(sock.fileno(), termios.FIONREAD, held, True)
    except (OSError, ValueError):
        return 0
    # a signed 32-bit field, which is what FIONREAD writes on both platforms this runs on; a
    # receive queue past 2 GiB would be misreported and no socket option here allows one
    return held[0]


class Role(enum.StrEnum):
    """A connection's place in the topology.

    LEADER_LINK points the opposite way from FOLLOWER. FOLLOWER is a peer
    syncing FROM this process, while LEADER_LINK is this process's own
    outbound connection TO its leader. A check written as a two-value
    test classifies the link as an ordinary client, which then rate-limits
    the process's own replication stream with no error anywhere.

    StrEnum so the value renders directly into an INFO `role:` field and
    into log lines with no conversion.
    """

    CLIENT = "client"
    FOLLOWER = "follower"
    LEADER_LINK = "leader_link"


_NEXT_ID = itertools.count(1)


class BatchProtocolError(resp.ProtocolError):
    """Raised by take_commands() when a batch's parse fails partway through.

    commands holds every command already parsed out of this read batch
    before the failure -- [] when nothing had been. It rides on the
    exception rather than on the connection because take_commands() builds
    the list in a local that the raise would otherwise discard along with
    the frame; the exception is the only thing that outlives it and already
    crosses the connection/server boundary. A subclass of resp.ProtocolError
    rather than a new type, so every existing catch of that -- including
    this module's own callers -- keeps catching it.
    """

    def __init__(self, message: bytes, commands: list[list[bytes]]) -> None:
        super().__init__(message)
        self.commands = commands


class Connection:
    def __init__(
        self,
        sock: socket.socket,
        addr: tuple[str, int],
        role: Role = Role.CLIENT,
    ) -> None:
        self._sock = sock
        # the only way to name a connection in a log line or an error message
        self.addr = addr
        # a counter, since descriptors are reused and fileno() would give two connections the same id
        self.id = next(_NEXT_ID)
        # bytearray because a consumed prefix is deleted (del buf[:n]) rather than the buffer being re-allocated.
        self.read_buffer = bytearray()
        # a monotonic reading the server writes, None while this connection holds no incomplete
        # command: armed on the transition into holding one, cleared on the transition out or
        # when the server stops reading, and not re-armed while the same command is outstanding
        # and reads are live. a batch in which a command completed is the exception: whatever is
        # held after a completed command is the start of the next one, so it gets a deadline of
        # its own. without that, a client whose every read cuts a pipeline mid-command would
        # carry the first deadline through the whole pipeline and be closed at the timeout
        # however quickly each command completed. the server reads the clock; this module holds
        # buffers and parse state and no policy, so it imports none and never writes the slot
        self.incomplete_since = None
        self.write_buffer = bytearray()
        # the buffer length the last incomplete parse said it needs, so a partial command is not re-parsed from byte zero on every readable event
        self._parse_needed = 0
        # how far a terminator search has already proved there is no terminator. a header
        # line's length is declared nowhere, so _parse_needed can never bound one; without
        # this, locating a header costs a scan of the whole buffer on every readable event,
        # which is quadratic in the bytes one connection has sent
        self._scan_from = 0
        # a multibulk whose header has been consumed and whose elements are still arriving. holding
        # the argv here is what keeps locating N elements O(N) rather than O(N) per element
        self._argv: list[bytes] | None = None
        self._elements_remaining = 0
        # bytes already taken out of the read buffer that belong to a command which has not
        # completed: a multibulk's consumed header and its finished elements. the read buffer
        # alone understates what a connection holds for exactly the reason has_incomplete_command
        # below reads _argv as well, and the shutdown drain reports these bytes as discarded, so
        # the quantity has to be kept rather than inferred -- take_commands deletes the bytes it
        # consumes and nothing afterwards can recover the count. this module maintains it; the
        # server reads it and zeroes it once it has been counted
        self.consumed_for_incomplete_command = 0
        self.closed = False
        self.role = role
        # filled and interpreted only by their owning module
        self.rate_limit_state = None
        self.replication_state = None
        # the third opaque slot, and the only one whose filler is server.py itself rather
        # than a module of its own: set in Server._on_accept once this connection is
        # registered and tracked, cleared back to None in Server._close. Untyped like the
        # two slots above, so this module gains no import to name what fills it. The
        # contract is what makes reaching into it from commands/ safe: a handler may read
        # the named public attributes of the object held here, and nothing else -- no
        # method calls, no underscore-prefixed names. A private reach would be the same
        # category of defect as commands/ touching store.py's own internals, just outside
        # the boundary written to catch that one
        self.server = None

    def fileno(self) -> int:
        return self._sock.fileno()

    def unread_in_kernel(self) -> int:
        # what the kernel says the peer has sent that this process has not read, in one syscall and
        # taking nothing out of the receive queue. it is not the figure a stop reports: on a socket
        # this process has read from, a kernel that sums each queued buffer's whole length answers
        # for a buffer it has already handed half of over, so discard_unread_from_kernel() below
        # takes this as a ceiling and reads the rest to find out. the ioctl is the answer only
        # where nothing has ever read the socket, which is the accept-backlog sweep's case and
        # reaches the module-level function above directly, on a socket that never became a
        # Connection
        # 0 rather than a raise for a closed socket, or a kernel that refuses the question on some
        # descriptor this never sees, because what is being assembled is a figure for one log line
        # and a report that raises on the way out is worse than one that is short -- and a 0 here
        # is also what keeps the loop below from running at all for such a socket
        if self.closed:
            return 0
        return unread_in_kernel(self._sock)

    def discard_unread_from_kernel(self) -> int:
        # the same quantity unread_in_kernel() answers for, established by reading it off the
        # socket and throwing it away rather than by trusting the kernel's own figure. the ioctl is
        # asked for a CEILING and not for the answer, because the answer is not portable: a kernel
        # that walks its receive queue summing each queued buffer's whole length reports a buffer
        # this process has already read half of as still unread in full, and the drain's read path
        # has counted that half once already -- so the five shapes the figure is made of stop being
        # additive, which is worse than a number being wrong. reads cannot find bytes that are not
        # there, so what this returns is right on a kernel that answers either way, and the
        # disagreement costs nothing but the syscalls. docs/DESIGN.md carries the measurements: the
        # buffer size, the figure such a kernel reported, and what the emptied queue does to the
        # close that follows
        #
        # the ceiling is also what bounds it. a peer that goes on sending while this runs cannot
        # extend the loop past what the kernel claimed when it was asked, so this stays one bounded
        # step at a point in the stop sequence that no deadline covers
        #
        # a closed socket, or a kernel that refuses the question, gives a ceiling of 0 through the
        # method above and the loop below then does not run, which is the whole of the handling
        # this needs for either
        ceiling = self.unread_in_kernel()
        discarded = 0
        while discarded < ceiling:
            try:
                chunk = self._sock.recv(min(RECV_SIZE, ceiling - discarded))
            except (OSError, ValueError):
                # BlockingIOError is an OSError, so an emptied queue ends this loop by the same arm
                # as a descriptor that will not answer, and neither is worth a line: the first is
                # the ordinary way out on a kernel whose ceiling was generous, and the second is
                # the case unread_in_kernel() already answers 0 for
                break
            if not chunk:
                break
            discarded += len(chunk)
        # deliberately not extended into read_buffer: these bytes are discarded undispatched, and a
        # buffer the drain's own read path counts and clears is the one place they could be counted
        # twice
        return discarded

    @property
    def has_incomplete_command(self) -> bool:
        # the buffer alone is not enough: a multibulk whose header is consumed and whose latest element ended exactly at the buffer's end leaves _argv a list and the buffer empty, with elements still owed
        return self._argv is not None or bool(self.read_buffer)

    def receive(self) -> bool:
        # true means the peer is still connected, not that data arrived: the BlockingIOError branch returns True having read nothing
        try:
            data = self._sock.recv(RECV_SIZE)
        except BlockingIOError:
            # a subclass of OSError, so it is handled first
            return True
        except OSError:
            # a peer that vanishes mid-connection raises ConnectionResetError rather than returning zero bytes on some platforms, so any other OSError means the same thing here as a zero-length read.
            return False
        if not data:
            return False
        self.read_buffer.extend(data)
        return True

    def take_commands(
        self, *, max_value_size: int = 0, max_multibulk: int = 0
    ) -> list[list[bytes]]:
        # one step per pass -- an inline command, a multibulk header, or a single element -- because
        # a step's bytes leave the buffer as soon as it completes and are never scanned again
        commands = []
        try:
            while self.read_buffer:
                if len(self.read_buffer) < self._parse_needed:
                    # nothing between here and that length can change the parse's answer, so re-running it is pure cost
                    break
                if self._argv is not None:
                    body, consumed, needed = resp.parse_bulk_element(
                        self.read_buffer, self._scan_from, max_value_size=max_value_size
                    )
                    if consumed == 0:
                        self._parse_needed = needed
                        # a non-zero hint means the header was found and only the body is
                        # short, so the resume position is spent and would otherwise point
                        # past this element's own terminator into the next one's
                        self._scan_from = 0 if needed else self._resume_after_crlf(1)
                        break
                    self._parse_needed = 0
                    self._scan_from = 0
                    del self.read_buffer[:consumed]
                    # this element's bytes have left the buffer and belong to a command that is
                    # still arriving, so they move from the buffer's length into the count below
                    self.consumed_for_incomplete_command += consumed
                    self._argv.append(body)
                    self._elements_remaining -= 1
                    if self._elements_remaining == 0:
                        commands.append(self._argv)
                        self._argv = None
                        # the command is whole and is on its way to be dispatched, so nothing is
                        # held for it any more
                        self.consumed_for_incomplete_command = 0
                elif self.read_buffer[0:1] == b"*":
                    count, consumed, needed = resp.parse_multibulk_header(
                        self.read_buffer, self._scan_from, max_multibulk=max_multibulk
                    )
                    if consumed == 0:
                        self._parse_needed = needed
                        self._scan_from = self._resume_after_crlf(0)
                        break
                    self._parse_needed = 0
                    self._scan_from = 0
                    del self.read_buffer[:consumed]
                    # a count of zero is RESP's empty array: the header is consumed and no command comes out of it
                    if count:
                        self._argv = []
                        self._elements_remaining = count
                        # the header's own bytes are the first thing held for this command
                        self.consumed_for_incomplete_command += consumed
                    else:
                        # nothing is outstanding: the header was the whole of it. the reset is
                        # defensive and a no-op on every path the wire can produce: this branch is
                        # reached only while _argv is None, the counter is zeroed wherever _argv
                        # becomes None, so it is already 0 here -- the same equivalence the
                        # header's += above has to a plain =, and written as an assignment for the
                        # same reason, that the invariant belongs to the surrounding code and not
                        # to this line
                        self.consumed_for_incomplete_command = 0
                else:
                    argv, consumed, needed = resp.parse_command(
                        self.read_buffer, self._scan_from,
                        max_value_size=max_value_size, max_multibulk=max_multibulk,
                    )
                    if consumed == 0:
                        self._parse_needed = needed
                        # an inline line ends at a single \n, so there is no pair to be split
                        # across two reads and nothing to step back for
                        self._scan_from = len(self.read_buffer)
                        break
                    self._parse_needed = 0
                    self._scan_from = 0
                    # progress is driven by `consumed`, not by `argv`
                    del self.read_buffer[:consumed]
                    # an inline step leaves nothing outstanding: it either completed a command or
                    # consumed an empty line, and a multibulk cannot be in progress on this branch.
                    # the reset is defensive and a no-op on every path the wire can produce: this
                    # branch is reached only while _argv is None, the counter is zeroed wherever
                    # _argv becomes None, so it is already 0 here -- written as an assignment
                    # because that invariant belongs to the surrounding code and not to this line
                    self.consumed_for_incomplete_command = 0
                    if argv is not None:
                        commands.append(argv)
        except resp.ProtocolError as exc:
            # commands parsed ahead of the failure ride on the exception rather than
            # being lost with this frame -- the connection still has to be told, and
            # told to close, which is why this re-raises rather than returning what
            # was parsed so far
            raise BatchProtocolError(exc.message, commands) from exc
        return commands

    def _resume_after_crlf(self, floor: int) -> int:
        # one byte short of the end, because a \r already in the buffer pairs with a \n
        # that has not arrived yet: resuming at the end would step over that pair and the
        # header would never be found. floor keeps the element scan past its own `$`
        return max(floor, len(self.read_buffer) - 1)

    def queue(self, data: bytes) -> None:
        # the only outbound API. the socket is private, so a search for a raw socket write anywhere outside this module finds every bypass.
        self.write_buffer.extend(data)

    def flush(self) -> bool:
        # true means the peer is still connected, not that the buffer emptied: a short write is the normal outcome this path exists for
        while self.write_buffer:
            try:
                # the bytearray goes to send() directly: a live memoryview of it would make the del below raise BufferError
                sent = self._sock.send(self.write_buffer)
            except BlockingIOError:
                return True
            except OSError:
                return False
            if not sent:
                # POSIX forbids a zero return for a stream socket with bytes to send, and no real socket state produces one. treated as a dead peer rather than as progress: progress leaves write interest registered against a buffer nothing drains, and that spins a single-threaded loop at 100% CPU for every client, where a close costs only this one
                return False
            del self.write_buffer[:sent]
        return True

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._sock.close()
