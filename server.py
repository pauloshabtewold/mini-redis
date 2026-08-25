"""Entry point: CLI flags, signal handling, and the event loop's three callback bodies. Nothing is scheduled to run periodically."""

import argparse
import logging
import signal
import socket
from collections.abc import Callable

import commands
import resp
from connection import Connection, Role
from event_loop import EventLoop

DEFAULT_PORT = 6379
# the replication sync command is unauthenticated and is safe only bound to loopback.
LISTEN_HOST = "127.0.0.1"
# bounds how long a stop signal waits to be noticed, and is the floor of this design's periodic intervals (a 100 ms expiry sweep).
SELECT_TIMEOUT_SECONDS = 0.1

# logging.lastResort sends an ERROR record to stderr with no configuration
logger = logging.getLogger(__name__)


def build_arg_parser() -> argparse.ArgumentParser:
    # separate from main() so the parser can be inspected without running the server.
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    return parser


class Server:
    def __init__(self, port: int) -> None:
        self.port = port
        self._connections: set[Connection] = set()
        self._loop = EventLoop(
            self._on_accept, self._on_readable, self._on_writable, SELECT_TIMEOUT_SECONDS
        )
        self._running = False

    def _open_listener(self) -> socket.socket:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((LISTEN_HOST, self.port))
        listener.setblocking(False)
        listener.listen(socket.SOMAXCONN)
        return listener

    def _on_accept(self, listener: socket.socket) -> None:
        try:
            sock, addr = listener.accept()
        except OSError:
            # a peer aborting between readiness and accept is normal, and no per-connection boundary can cover this because no connection exists yet; EMFILE arrives here too and looks identical
            return
        # one accept per readable event, so the level-triggered readiness re-reports a remaining backlog on the next select() return.
        sock.setblocking(False)
        conn = Connection(sock, addr, role=Role.CLIENT)
        # register before tracking, so a connection is never in the set while unregistered, which would raise from the selector partway through shutdown
        self._loop.register(conn)
        self._connections.add(conn)

    def _on_readable(self, conn: Connection) -> None:
        self._guard(conn, self._read_and_dispatch)

    def _on_writable(self, conn: Connection) -> None:
        # the drain has two entry points and one body, so the write-interest decision is taken in exactly one place
        self._guard(conn, self._flush)

    def _guard(self, conn: Connection, step: Callable[[Connection], None]) -> None:
        # a single-threaded loop turns every uncaught exception into a total outage for every client, which is why this boundary is mandatory rather than defensive -- BlockingIOError and InterruptedError are named first because both are OSError subclasses
        try:
            step(conn)
        except (BlockingIOError, InterruptedError):
            return
        except Exception:
            logger.exception("closing %s after an unhandled exception", conn.addr)
            self._close(conn)

    def _read_and_dispatch(self, conn: Connection) -> None:
        if not conn.receive():
            self._close(conn)
            return
        try:
            parsed_commands = conn.take_commands()
        except resp.ProtocolError as exc:
            # exc.message is already the RESP error body; delivery is best-effort since the connection closes right after, so a short send() drops the tail of a message the peer was about to lose anyway
            conn.queue(resp.encode_error(exc.message))
            self._flush(conn)
            self._close(conn)
            return
        # every command take_commands() returns is dispatched: level-triggered readiness re-reports unread socket bytes, not commands already taken out of the buffer, so a leftover here is never revisited and the client waits forever
        for argv in parsed_commands:
            conn.queue(commands.dispatch(conn, argv))
        # one flush for the whole batch, not one per command: N replies concatenate into one buffer and N syscalls buy nothing
        self._flush(conn)

    def _flush(self, conn: Connection) -> None:
        if conn.closed:
            return
        if not conn.flush():
            self._close(conn)
            return
        # write interest tracks the buffer's emptiness exactly: a connection left permanently writable spins the loop at 100% CPU without dropping a single reply
        self._loop.set_write_interest(conn, bool(conn.write_buffer))

    def _close(self, conn: Connection) -> None:
        # idempotent, because the protocol-error path closes twice: _flush closes on a failed send and the caller closes again, and a second unregister raises from inside _guard's own recovery -- the one exception that escapes the boundary and takes the process with it
        if conn.closed:
            return
        try:
            # a queued reply is owed to the client and conn.close() discards it, so take whatever the kernel will still accept. this cannot block, and failing changes nothing since the connection is going either way
            conn.flush()
        except Exception:
            # _close is reached from _guard's own recovery, where a raise is the one exception that escapes the boundary and takes the process with it
            pass
        # unregister before closing: fileno() is -1 once the socket is closed, and the selector then finds the registration only by scanning its whole map for a matching object.
        self._loop.unregister(conn)
        conn.close()
        self._connections.discard(conn)

    def _request_stop(self, signum, frame) -> None:
        self._running = False

    def run(self) -> None:
        # raised before the handlers exist, because a signal delivered between installing them and this line would be cleared by the handler and then overwritten here.
        self._running = True
        signal.signal(signal.SIGINT, self._request_stop)
        signal.signal(signal.SIGTERM, self._request_stop)
        listener = self._open_listener()
        self._loop.register_listener(listener)
        try:
            while self._running:
                self._loop.run_once()
        finally:
            for conn in list(self._connections):
                self._close(conn)
            listener.close()
            self._loop.close()


def main(argv=None) -> None:
    args = build_arg_parser().parse_args(argv)
    Server(args.port).run()


if __name__ == "__main__":
    main()
