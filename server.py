"""Entry point: CLI flags, signal handling, and the event loop's three callback bodies. Nothing is scheduled to run periodically."""

import argparse
import signal
import socket

import resp
from connection import Connection, Role
from event_loop import EventLoop

DEFAULT_PORT = 6379
# the replication sync command is unauthenticated and is safe only bound to loopback.
LISTEN_HOST = "127.0.0.1"
# bounds how long a stop signal waits to be noticed, and is the floor of this design's periodic intervals (a 100 ms expiry sweep).
SELECT_TIMEOUT_SECONDS = 0.1


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
        if not conn.receive():
            self._close(conn)
            return
        try:
            conn.take_commands()
        except resp.ProtocolError:
            self._close(conn)

    def _on_writable(self, conn: Connection) -> None:
        raise RuntimeError(
            f"write interest registered for {conn.addr} with no drain to service it"
        )

    def _close(self, conn: Connection) -> None:
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
