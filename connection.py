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
        commands = []
        while True:
            argv, consumed = resp.parse_command(self.read_buffer)
            if consumed == 0:
                break
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
