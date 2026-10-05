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
        # none as the key data is what marks this descriptor as the listener rather than a connection.
        self._selector.register(sock, selectors.EVENT_READ, None)

    def register(self, conn: Connection) -> None:
        # the connection is its own key data, so dispatch needs no parallel descriptor lookup.
        self._selector.register(conn, selectors.EVENT_READ, conn)

    def unregister(self, conn: Connection) -> None:
        # idempotent, because mask 0 is spelled unregistered: a connection whose interest was cleared is already absent by the time the close path reaches here, and a second unregister would raise from the one place that must not.
        self._apply(conn, 0)

    def unregister_listener(self, sock) -> None:
        # the pair of register_listener. leaving the select set is what stops accept dispatch, while the connections already registered keep being served.
        self._selector.unregister(sock)

    def _mask_of(self, conn: Connection) -> int:
        # absent reads as 0, which makes not-registered a legal state rather than a KeyError for every caller to guard.
        try:
            return self._selector.get_key(conn).events
        except (KeyError, ValueError):
            # the same answer by two exceptions: a connection the selector does not hold raises KeyError while its socket is open, and ValueError once it is closed, because a closed socket's descriptor is -1, which the selector refuses to look up by number and then finds by identity nowhere. neither says anything but not-registered.
            return 0

    def _apply(self, conn: Connection, new_mask: int) -> bool:
        # the one place that changes a registration, and it never asks the selector for an empty mask: kqueue's modify(conn, 0) raises ValueError and drops the registration on its way out, where epoll accepts it, so a design built on modify would pass on Linux and fail on a Mac. mask 0 is spelled unregistered instead.
        mask = self._mask_of(conn)
        if new_mask != mask:
            if new_mask == 0:
                self._selector.unregister(conn)
            elif mask == 0:
                self._selector.register(conn, new_mask, conn)
            else:
                self._selector.modify(conn, new_mask, conn)
            return True
        # the answer is whether the registration changed, so a caller can tell an edge from a level without asking the selector again.
        return False

    def set_write_interest(self, conn: Connection, wanted: bool) -> bool:
        # the selector's mask is the single source of truth: a second copy on Connection would disagree silently in both directions, stranding replies in a buffer nothing drains or leaving a connection permanently writable.
        mask = self._mask_of(conn)
        if wanted:
            new_mask = mask | selectors.EVENT_WRITE
        else:
            new_mask = mask & ~selectors.EVENT_WRITE
        return self._apply(conn, new_mask)

    def set_read_interest(self, conn: Connection, wanted: bool) -> bool:
        # the mirror of set_write_interest. clearing both bits leaves the selector, and either bit brings the connection back.
        mask = self._mask_of(conn)
        if wanted:
            new_mask = mask | selectors.EVENT_READ
        else:
            new_mask = mask & ~selectors.EVENT_READ
        return self._apply(conn, new_mask)

    def run_once(self) -> None:
        # bounded so that periodic work in server.py -- the expiry sweep, the snapshot save -- runs on an idle server too: an unbounded select() would tie it to whenever client traffic happened to arrive. Checked every pass either way, not fired on every pass: each still waits out its own configured interval before it runs.
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
                # reached once set_write_interest registers EVENT_WRITE, which it does only while the write buffer is non-empty
                self._on_writable(conn)

    def close(self) -> None:
        self._selector.close()
