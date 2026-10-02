import selectors
import socket
from unittest import mock

import pytest

from connection import Connection
from event_loop import EventLoop

READ = selectors.EVENT_READ
WRITE = selectors.EVENT_WRITE


class Harness:
    def __init__(self):
        self.accepted = []
        self.readable = []
        self.writable = []
        self.order = []
        # a short timeout, because several tests drive passes that are meant to find nothing ready and an unbounded select() there would hang the suite
        self.loop = EventLoop(self._accept, self._read, self._write, 0.01)
        self.conns = []
        self.peers = []

    def _accept(self, sock):
        self.accepted.append(sock)

    def _read(self, conn):
        self.readable.append(conn)
        self.order.append(("read", conn))

    def _write(self, conn):
        self.writable.append(conn)
        self.order.append(("write", conn))

    def make(self):
        real, peer = socket.socketpair()
        real.setblocking(False)
        conn = Connection(real, ("test", 0))
        self.conns.append(conn)
        self.peers.append(peer)
        return conn

    def mask(self, conn):
        # zero stands for absent, the same spelling the loop itself uses
        try:
            return self.loop._selector.get_key(conn).events
        except KeyError:
            return 0

    def is_registered(self, sock):
        try:
            self.loop._selector.get_key(sock)
        except KeyError:
            return False
        return True

    def close(self):
        try:
            for conn in self.conns:
                conn.close()
            for peer in self.peers:
                peer.close()
        finally:
            self.loop.close()


@pytest.fixture
def h():
    harness = Harness()
    try:
        yield harness
    finally:
        harness.close()


def masks_asked(spy):
    # the mask each call asked the selector for, whatever the call's own position arguments were
    return [call.args[1] for call in spy.call_args_list]


def test_registering_a_connection_asks_for_read_only(h):
    conn = h.make()
    h.loop.register(conn)
    assert h.mask(conn) == READ
    assert h.loop._selector.get_key(conn).data is conn


def test_setting_write_interest_adds_the_write_bit(h):
    conn = h.make()
    h.loop.register(conn)
    assert h.loop.set_write_interest(conn, True) is True
    assert h.mask(conn) == READ | WRITE


def test_clearing_write_interest_leaves_read_registered(h):
    conn = h.make()
    h.loop.register(conn)
    h.loop.set_write_interest(conn, True)
    assert h.loop.set_write_interest(conn, False) is True
    assert h.mask(conn) == READ


def test_clearing_read_interest_leaves_write_registered(h):
    conn = h.make()
    h.loop.register(conn)
    h.loop.set_write_interest(conn, True)
    assert h.loop.set_read_interest(conn, False) is True
    assert h.mask(conn) == WRITE


def test_clearing_both_interests_unregisters_rather_than_asking_for_an_empty_mask(h):
    sel = h.loop._selector
    # both routes to an empty mask: the write bit going last, and the read bit going last with nothing queued
    via_write = h.make()
    via_read = h.make()
    h.loop.register(via_write)
    h.loop.register(via_read)
    h.loop.set_write_interest(via_write, True)
    h.loop.set_read_interest(via_write, False)
    # a spy rather than the selector's own refusal, because epoll accepts an empty mask and the assertion has to hold there too
    with (
        mock.patch.object(sel, "modify", wraps=sel.modify) as modify,
        mock.patch.object(sel, "register", wraps=sel.register) as register,
    ):
        assert h.loop.set_write_interest(via_write, False) is True
        assert h.loop.set_read_interest(via_read, False) is True
    assert 0 not in masks_asked(modify)
    assert 0 not in masks_asked(register)
    assert h.mask(via_write) == 0
    assert h.mask(via_read) == 0
    assert not h.is_registered(via_write)
    assert not h.is_registered(via_read)


def test_a_connection_unregistered_by_clearing_both_is_registered_again_by_either_interest(h):
    for restore in (h.loop.set_read_interest, h.loop.set_write_interest):
        conn = h.make()
        h.loop.register(conn)
        h.loop.set_write_interest(conn, True)
        h.loop.set_read_interest(conn, False)
        h.loop.set_write_interest(conn, False)
        assert not h.is_registered(conn)
        assert restore(conn, True) is True
        # dispatch finds the connection through the key's data, so a re-registration that dropped it would fire nothing
        assert h.loop._selector.get_key(conn).data is conn


def test_restoring_read_interest_on_an_unregistered_connection_registers_it_read_only(h):
    conn = h.make()
    h.loop.register(conn)
    h.loop.unregister(conn)
    assert h.loop.set_read_interest(conn, True) is True
    assert h.mask(conn) == READ


def test_restoring_write_interest_on_an_unregistered_connection_registers_it_write_only(h):
    conn = h.make()
    h.loop.register(conn)
    h.loop.unregister(conn)
    assert h.loop.set_write_interest(conn, True) is True
    assert h.mask(conn) == WRITE


def test_restoring_read_interest_on_a_write_only_connection_modifies_rather_than_registers(h):
    sel = h.loop._selector
    conn = h.make()
    h.loop.register(conn)
    h.loop.set_write_interest(conn, True)
    # what end of input does during a shutdown drain to a connection that still owes bytes: read interest goes, write interest stays, and the connection is still registered
    assert h.loop.set_read_interest(conn, False) is True
    assert h.mask(conn) == WRITE
    # a spy on modify and nothing else, because the resulting mask reads the same whether the applier modified the registration or tore it down and made it again, and a spy on register cannot tell them apart where the standard library's own modify is an unregister and a register, as it is on kqueue
    with mock.patch.object(sel, "modify", wraps=sel.modify) as modify:
        assert h.loop.set_read_interest(conn, True) is True
    assert masks_asked(modify) == [READ | WRITE]
    assert h.mask(conn) == READ | WRITE
    assert h.loop._selector.get_key(conn).data is conn


def test_an_unchanged_mask_does_not_touch_the_selector(h):
    sel = h.loop._selector
    both = h.make()
    read_only = h.make()
    absent = h.make()
    h.loop.register(both)
    h.loop.set_write_interest(both, True)
    h.loop.register(read_only)
    with (
        mock.patch.object(sel, "register", wraps=sel.register) as register,
        mock.patch.object(sel, "modify", wraps=sel.modify) as modify,
        mock.patch.object(sel, "unregister", wraps=sel.unregister) as unregister,
    ):
        # a bit already set, a bit already clear, and an absent connection asked to give up interest it never had
        assert h.loop.set_read_interest(both, True) is False
        assert h.loop.set_write_interest(both, True) is False
        assert h.loop.set_write_interest(read_only, False) is False
        assert h.loop.set_read_interest(absent, False) is False
        assert h.loop.set_write_interest(absent, False) is False
    assert register.call_count == 0
    assert modify.call_count == 0
    assert unregister.call_count == 0
    assert h.mask(both) == READ | WRITE
    assert h.mask(read_only) == READ
    assert h.mask(absent) == 0


def test_unregister_is_idempotent(h):
    registered = h.make()
    never_registered = h.make()
    h.loop.register(registered)
    h.loop.unregister(registered)
    h.loop.unregister(registered)
    h.loop.unregister(never_registered)
    assert h.mask(registered) == 0
    assert h.mask(never_registered) == 0


def test_unregister_is_idempotent_for_a_closed_connection(h):
    # a closed socket reports descriptor -1, which the selector rejects with ValueError before it looks anything up, where an open one it does not hold raises KeyError. both are the same state to a caller: not registered.
    bystander = h.make()
    h.loop.register(bystander)
    never_registered = h.make()
    never_registered.close()
    unregistered = h.make()
    h.loop.register(unregistered)
    h.loop.unregister(unregistered)
    unregistered.close()
    closed_while_registered = h.make()
    h.loop.register(closed_while_registered)
    closed_while_registered.close()
    for absent in (never_registered, unregistered):
        h.loop.unregister(absent)
        h.loop.unregister(absent)
        assert h.loop.set_write_interest(absent, False) is False
        assert h.loop.set_read_interest(absent, False) is False
    # the selector still finds a registration by identity after its socket closes, so the one that was never removed is removed here rather than skipped as absent
    h.loop.unregister(closed_while_registered)
    assert list(h.loop._selector.get_map()) == [bystander.fileno()]
    assert h.mask(bystander) == READ


def test_unregister_listener_removes_the_listener_and_leaves_connections_registered(h):
    listener = socket.socket()
    try:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        conn = h.make()
        h.loop.register_listener(listener)
        h.loop.register(conn)
        assert h.is_registered(listener)
        h.loop.unregister_listener(listener)
        assert not h.is_registered(listener)
        assert h.mask(conn) == READ
    finally:
        listener.close()


def test_a_listener_unregistered_mid_loop_dispatches_no_further_accepts(h):
    listener = socket.socket()
    client = socket.socket()
    try:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        h.loop.register_listener(listener)
        client.connect(listener.getsockname())
        # the harness never accept()s, so the pending connection keeps the listener readable on every pass; two dispatches before the unregister is what makes silence after it mean something
        h.loop.run_once()
        h.loop.run_once()
        assert len(h.accepted) == 2
        h.loop.unregister_listener(listener)
        for _ in range(3):
            h.loop.run_once()
        assert len(h.accepted) == 2
    finally:
        client.close()
        listener.close()


def test_run_once_dispatches_read_and_write_for_one_connection_in_one_pass(h):
    conn = h.make()
    peer = h.peers[-1]
    h.loop.register(conn)
    h.loop.set_write_interest(conn, True)
    peer.sendall(b"x")
    h.loop.run_once()
    assert h.readable == [conn]
    assert h.writable == [conn]


def test_the_selector_itself_refuses_an_empty_mask(h):
    # shaped as a disjunction because the backends disagree: kqueue raises and drops the registration, epoll accepts the empty mask. it exists so the applier's never-ask-for-zero test cannot pass vacuously on a backend that tolerates it.
    sel = h.loop._selector
    conn = h.make()
    h.loop.register(conn)
    try:
        sel.modify(conn, 0, conn)
    except ValueError:
        assert not h.is_registered(conn)
    else:
        assert sel.get_key(conn).events == 0
