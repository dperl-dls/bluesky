from typing import Any, Callable, Generator, Optional, Set

from bluesky.protocols import Status
from bluesky.utils import Msg


class ParallelPlanStatus:
    def __init__(self, done=False) -> None:
        self._callbacks: Set[Callable[[Status], None]] = set()
        self._done = done

    def add_callback(self, callback: Callable[[Status], None]) -> None:
        """Add a callback function to be called upon completion.

        The function must take the status as an argument.

        If the Status object is done when the function is added, it should be
        called immediately.
        """
        if self.done:
            callback(self)
        else:
            self._callbacks.add(callback)

    def exception(self, timeout: Optional[float] = 0.0) -> Optional[BaseException]: ...

    def set_done(self):
        if self._done:
            return
        else:
            self._done = True
            while self._callbacks:
                self._callbacks.pop()(self)

    @property
    def done(self) -> bool:
        """If done return True, otherwise return False."""
        return self._done

    @property
    def success(self) -> bool:
        """If done return whether the operation was successful."""
        return True if self.done else False


def run_sub_plan(
    plan: Callable[[], Generator[Msg, Any, Any]], group: Any | None = None
):
    yield from plan()
    return ParallelPlanStatus()
