"""Selectors readiness dispatch: on_readable, on_writable, on_accept, and nothing else."""

import selectors
from collections.abc import Callable
from typing import Any

from connection import Connection


class EventLoop:
    """This module's import list is the architecture: it knows Connection
    and nothing else. The callback bodies belong to server.py,
    because the alternative, which is putting the periodic tick here, where
    select() returns, makes the thinnest module in the project import
    the store, persistence, the command layer and eventually replication.
    The same rule sets the annotations below, where on_accept takes the
    listening socket and the other two take the ready Connection, but the
    listener is typed Any rather than socket.socket, because naming that
    type would mean importing socket here for the sake of a hint."""

    def __init__(
        self,
        on_accept: Callable[[Any], None],
        on_readable: Callable[[Connection], None],
        on_writable: Callable[[Connection], None],
        timeout: float,
    ) -> None:
        self._on_accept = on_accept
        self._on_readable = on_readable
        self._on_writable = on_writable
        self._timeout = timeout
        self._selector = selectors.DefaultSelector()

    def register_listener(self, sock) -> None:
        # none as the key data is what marks this descriptor as the listener rather than a connection.
        self._selector.register(sock, selectors.EVENT_READ, None)

    def register(self, conn: Connection) -> None:
        # the connection is its own key data, so dispatch needs no parallel descriptor lookup.
        self._selector.register(conn, selectors.EVENT_READ, conn)

    def unregister(self, conn: Connection) -> None:
        self._selector.unregister(conn)

    def run_once(self) -> None:
        # the timeout is what lets the periodic tasks run on an idle server, with an unbounded select() they would only ever run when client traffic happened to arrive.
        events = self._selector.select(self._timeout)
        for key, mask in events:
            data = key.data
            if data is None:
                self._on_accept(key.fileobj)
                continue
            conn = data
            # one select() return can carry both read and write readiness for the same descriptor, and the second dispatch would otherwise reach a socket the first one already closed.
            if mask & selectors.EVENT_READ and not conn.closed:
                self._on_readable(conn)
            if mask & selectors.EVENT_WRITE and not conn.closed:
                # no code registers write interest, so this arm is unreached
                self._on_writable(conn)

    def close(self) -> None:
        self._selector.close()
