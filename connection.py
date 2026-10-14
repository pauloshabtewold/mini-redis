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
    # asked of the kernel, not read out of it: one syscall, nothing taken off the receive queue
    # 0 and not a raise -- ValueError is a closed descriptor's, and the drain's one line must land
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

    StrEnum so a value can be rendered without conversion, which nothing
    does today: INFO's one `role:` field is the replication role, a
    constant, and `commands/server.py` says beside it that the two
    vocabularies are disjoint. No log line here names a connection's role
    either. The enum is what makes a typo an AttributeError at the line
    that has it; the str is for a reader that does not exist yet.
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
        # bytearray because a consumed prefix is deleted (del buf[:n]) rather than re-allocated
        self.read_buffer = bytearray()
        # a monotonic reading the server writes, None while no command is held: a completed command
        # starts a fresh deadline, or a pipeline cut mid-command every read dies on the first one
        self.incomplete_since = None
        self.write_buffer = bytearray()
        # the length the last incomplete parse asked for, so a partial command is not re-parsed
        self._parse_needed = 0
        # how far a terminator search has proved there is none: a header line's length is declared
        # nowhere, so without this locating one is quadratic in the bytes a connection has sent
        self._scan_from = 0
        # a multibulk whose header has been consumed and whose elements are still arriving. holding
        # the argv here is what keeps locating N elements O(N) rather than O(N) per element
        self._argv: list[bytes] | None = None
        self._elements_remaining = 0
        # bytes off the read buffer belonging to an incomplete command -- a multibulk's header and
        # finished elements: take_commands deletes them, so the count cannot be inferred afterwards
        self.consumed_for_incomplete_command = 0
        self.closed = False
        self.role = role
        # filled and interpreted only by their owning module
        self.rate_limit_state = None
        self.replication_state = None
        # untyped like the two above, so this module gains no import for what fills it: a handler
        # may read the named public attributes of what is here and nothing else, no method calls
        self.server = None

    def fileno(self) -> int:
        return self._sock.fileno()

    def unread_in_kernel(self) -> int:
        # not the figure a stop reports: a kernel summing each queued buffer's whole length counts
        # a half-read one in full, so this is a ceiling, and 0 for a closed socket and not a raise
        if self.closed:
            return 0
        return unread_in_kernel(self._sock)

    def discard_unread_from_kernel(self) -> int:
        # the ioctl is a ceiling and not the answer: a kernel summing each queued buffer's whole
        # length double-counts a half-read one, so the figure's five shapes stop being additive
        ceiling = self.unread_in_kernel()
        discarded = 0
        while discarded < ceiling:
            try:
                chunk = self._sock.recv(min(RECV_SIZE, ceiling - discarded))
            except (OSError, ValueError):
                # BlockingIOError is an OSError, so an emptied queue ends the loop by the same arm
                # as a descriptor that will not answer, and neither is worth a line
                break
            if not chunk:
                break
            discarded += len(chunk)
        # not extended into read_buffer: these bytes are discarded undispatched, and that buffer is
        # the one place the drain's own read path could count them twice
        return discarded

    @property
    def has_incomplete_command(self) -> bool:
        # the buffer alone is not enough: a multibulk whose latest element ended exactly at the
        # buffer's end leaves _argv a list and the buffer empty, with elements still owed
        return self._argv is not None or bool(self.read_buffer)

    def receive(self) -> bool:
        # true means still connected, not that data arrived: the BlockingIOError arm reads nothing
        try:
            data = self._sock.recv(RECV_SIZE)
        except BlockingIOError:
            # a subclass of OSError, so it is handled first
            return True
        except OSError:
            # a peer that vanishes raises ConnectionResetError on some platforms rather than
            # returning zero bytes, so any other OSError means what a zero-length read means
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
                    # nothing short of that length changes the parse's answer, so re-running is cost
                    break
                if self._argv is not None:
                    body, consumed, needed = resp.parse_bulk_element(
                        self.read_buffer, self._scan_from, max_value_size=max_value_size
                    )
                    if consumed == 0:
                        self._parse_needed = needed
                        # a non-zero hint means the header was found and only the body is short, so
                        # the resume position would point past this element's own terminator
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
                    # a count of zero is RESP's empty array: header consumed, no command out of it
                    if count:
                        self._argv = []
                        self._elements_remaining = count
                        # the header's own bytes are the first thing held for this command
                        self.consumed_for_incomplete_command += consumed
                    else:
                        # the header was the whole of it. the reset is defensive and already 0 on
                        # every path the wire produces, since _argv is None on this branch
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
                    # an inline step leaves nothing outstanding: a command or an empty line, and no
                    # multibulk on this branch, so the defensive reset is already 0 on every path
                    self.consumed_for_incomplete_command = 0
                    if argv is not None:
                        commands.append(argv)
        except resp.ProtocolError as exc:
            # commands parsed ahead of the failure ride on the exception rather than being lost
            # with this frame; it re-raises because the connection still has to be told to close
            raise BatchProtocolError(exc.message, commands) from exc
        return commands

    def _resume_after_crlf(self, floor: int) -> int:
        # one byte short of the end: a \r already here pairs with a \n that has not arrived, and
        # resuming at the end steps over the pair. floor keeps the element scan past its own `$`
        return max(floor, len(self.read_buffer) - 1)

    def queue(self, data: bytes) -> None:
        # the only outbound API: the socket is private, so a search for a raw send finds any bypass
        self.write_buffer.extend(data)

    def flush(self) -> bool:
        # true means still connected, not that the buffer emptied: a short write is the normal case
        while self.write_buffer:
            try:
                # the bytearray goes to send() directly: a live memoryview makes the del BufferError
                sent = self._sock.send(self.write_buffer)
            except BlockingIOError:
                return True
            except OSError:
                return False
            if not sent:
                # POSIX forbids a zero return here, so it is a dead peer and not progress: progress
                # leaves write interest on a buffer nothing drains, spinning this loop at 100% CPU
                return False
            del self.write_buffer[:sent]
        return True

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._sock.close()
