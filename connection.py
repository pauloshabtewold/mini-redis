"""The Connection class: owns the socket, the read and write buffers, the role, and the only outbound API."""

import enum
import itertools
import socket

import resp

# One recv() per readable event, never a loop
RECV_SIZE = 65536


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
        self.write_buffer = bytearray()
        # the buffer length the last incomplete parse said it needs, so a partial command is not re-parsed from byte zero on every readable event
        self._parse_needed = 0
        # a multibulk whose header has been consumed and whose elements are still arriving. holding
        # the argv here is what keeps locating N elements O(N) rather than O(N) per element
        self._argv: list[bytes] | None = None
        self._elements_remaining = 0
        self.closed = False
        self.role = role
        # filled and interpreted only by their owning module
        self.rate_limit_state = None
        self.replication_state = None

    def fileno(self) -> int:
        return self._sock.fileno()

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

    def take_commands(self) -> list[list[bytes]]:
        # one step per pass -- an inline command, a multibulk header, or a single element -- because
        # a step's bytes leave the buffer as soon as it completes and are never scanned again
        commands = []
        while self.read_buffer:
            if len(self.read_buffer) < self._parse_needed:
                # nothing between here and that length can change the parse's answer, so re-running it is pure cost
                break
            if self._argv is not None:
                body, consumed, needed = resp.parse_bulk_element(self.read_buffer)
                if consumed == 0:
                    self._parse_needed = needed
                    break
                self._parse_needed = 0
                del self.read_buffer[:consumed]
                self._argv.append(body)
                self._elements_remaining -= 1
                if self._elements_remaining == 0:
                    commands.append(self._argv)
                    self._argv = None
            elif self.read_buffer[0:1] == b"*":
                count, consumed, needed = resp.parse_multibulk_header(self.read_buffer)
                if consumed == 0:
                    self._parse_needed = needed
                    break
                self._parse_needed = 0
                del self.read_buffer[:consumed]
                # a count of zero is RESP's empty array: the header is consumed and no command comes out of it
                if count:
                    self._argv = []
                    self._elements_remaining = count
            else:
                argv, consumed, needed = resp.parse_command(self.read_buffer)
                if consumed == 0:
                    self._parse_needed = needed
                    break
                self._parse_needed = 0
                # progress is driven by `consumed`, not by `argv`
                del self.read_buffer[:consumed]
                if argv is not None:
                    commands.append(argv)
        return commands

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
                return True
            del self.write_buffer[:sent]
        return True

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._sock.close()
