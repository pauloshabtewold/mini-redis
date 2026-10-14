"""Selectors readiness dispatch, and the interest bookkeeping that decides what it reaches.

run_once dispatches to on_readable, on_writable and on_accept and to nothing else; what
each callback does belongs to server.py. The registration methods and the two interest
setters keep the selector's own mask as the one record of what a connection is
dispatched for.
"""

import selectors
from collections.abc import Callable
from typing import Any

from connection import Connection


class EventLoop:
    """This module's import list is the architecture: it knows Connection
    and nothing else. The callback bodies belong to server.py,
    because the alternative, which is putting the periodic tick here, where
    select() returns, makes the thinnest module in the project import
    the store, persistence and the command layer.
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
        # none as the key data is what marks this descriptor the listener rather than a connection
        self._selector.register(sock, selectors.EVENT_READ, None)

    def register(self, conn: Connection) -> None:
        # the connection is its own key data, so dispatch needs no parallel descriptor lookup
        self._selector.register(conn, selectors.EVENT_READ, conn)

    def unregister(self, conn: Connection) -> None:
        # idempotent, since mask 0 is spelled unregistered: a connection whose interest was cleared
        # is already absent when the close path reaches here, where a second unregister must not raise
        self._apply(conn, 0)

    def unregister_listener(self, sock) -> None:
        # the pair of register_listener: leaving the select set stops accept dispatch, while the
        # connections already registered keep being served
        self._selector.unregister(sock)

    def _mask_of(self, conn: Connection) -> int:
        # absent reads as 0, making not-registered a legal state and not a KeyError to guard
        try:
            return self._selector.get_key(conn).events
        except (KeyError, ValueError):
            # two exceptions, one answer: KeyError while the socket is open, ValueError once it is
            # closed, since a closed socket's descriptor is -1 and the selector finds it nowhere
            return 0

    def _apply(self, conn: Connection, new_mask: int) -> bool:
        # the one place a registration changes, and never with an empty mask: kqueue's
        # modify(conn, 0) raises ValueError and drops the registration where epoll accepts it
        mask = self._mask_of(conn)
        if new_mask != mask:
            if new_mask == 0:
                self._selector.unregister(conn)
            elif mask == 0:
                self._selector.register(conn, new_mask, conn)
            else:
                self._selector.modify(conn, new_mask, conn)
            return True
        # the answer is whether the registration changed, so a caller tells an edge from a level
        return False

    def set_write_interest(self, conn: Connection, wanted: bool) -> bool:
        # the selector's mask is the one source of truth: a second copy on Connection would disagree
        # both ways, stranding replies in a buffer nothing drains or leaving a connection writable
        mask = self._mask_of(conn)
        if wanted:
            new_mask = mask | selectors.EVENT_WRITE
        else:
            new_mask = mask & ~selectors.EVENT_WRITE
        return self._apply(conn, new_mask)

    def set_read_interest(self, conn: Connection, wanted: bool) -> bool:
        # the mirror of set_write_interest: both bits clear leaves the selector, either bit returns
        mask = self._mask_of(conn)
        if wanted:
            new_mask = mask | selectors.EVENT_READ
        else:
            new_mask = mask & ~selectors.EVENT_READ
        return self._apply(conn, new_mask)

    def run_once(self) -> None:
        # bounded so server.py's periodic work runs on an idle server: an unbounded select() would
        # tie the expiry sweep and the snapshot save to whenever client traffic arrived
        events = self._selector.select(self._timeout)
        for key, mask in events:
            data = key.data
            if data is None:
                self._on_accept(key.fileobj)
                continue
            conn = data
            # one select() return can carry read and write readiness for one descriptor, and the
            # second dispatch would reach a socket the first already closed
            if mask & selectors.EVENT_READ and not conn.closed:
                self._on_readable(conn)
            if mask & selectors.EVENT_WRITE and not conn.closed:
                # reached only once set_write_interest registers EVENT_WRITE, for a non-empty buffer
                self._on_writable(conn)

    def close(self) -> None:
        self._selector.close()
