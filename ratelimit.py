"""Per-connection sliding-window rate limiter, checked once per command before dispatch."""

import collections


class SlidingWindow:
    """At most `limit` permits per `window` seconds, for one connection.

    A deque of readings, popped from the left while the oldest is at or past the window.
    O(1) amortised: every reading is appended once and popped once, which is what a
    single-threaded loop needs, because an expensive check does not slow one client down,
    it slows every client down.

    The readings are monotonic, never wall clock. This measures a duration, and a
    wall-clock window breaks under an NTP correction in the direction that *permits* a
    flood -- a clock stepped forward empties the window. Expiry deadlines are the
    opposite case and use wall clock, because PEXPIREAT is Unix-milliseconds by protocol.
    """

    def __init__(self, limit: int, window: float) -> None:
        self.limit = limit
        self.window = window
        self._readings: collections.deque[float] = collections.deque()

    def allow(self, now: float) -> bool:
        """True and the permit is spent; False and nothing is recorded."""
        cutoff = now - self.window
        readings = self._readings
        # popped and not filtered: counting the readings inside the window answers correctly and
        # keeps one per request for the connection's life. <= because one window old has left it
        while readings and readings[0] <= cutoff:
            readings.popleft()
        # >= because the question is whether there is room for one more permit, which is
        # the same reason the connection cap compares with >= where every byte limit uses >
        if len(readings) >= self.limit:
            return False
        readings.append(now)
        return True

    def __len__(self) -> int:
        """How many permits are still inside the window -- what a test asserts is bounded."""
        return len(self._readings)
