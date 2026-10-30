"""Sliding-window reply quotas with reservations for concurrent turns."""

from collections import deque
from dataclasses import dataclass, field
from time import monotonic


@dataclass
class Window:
    sent: deque = field(default_factory=deque)
    pending: int = 0


@dataclass
class Turn:
    window: Window
    pending: bool = True
    sent: bool = False


class Quotas:
    def __init__(self, clock=monotonic):
        self.clock = clock
        self.windows = {}

    def reserve(self, key, seconds, maximum):
        window = self.windows.setdefault(key, Window())
        now = self.clock()
        while window.sent and now - window.sent[0] >= seconds:
            window.sent.popleft()
        # This check and reservation have no await between them.
        if len(window.sent) + window.pending >= maximum:
            return None
        window.pending += 1
        return Turn(window)

    def commit(self, turn):
        if turn and turn.pending:
            turn.window.sent.append(self.clock())
            turn.sent = True
            self.release(turn)

    @staticmethod
    def release(turn):
        if turn and turn.pending:
            turn.window.pending -= 1
            turn.pending = False
