from collections import deque
from typing import Any, Callable, Deque, Generator, Optional, Set
from uuid import uuid4

from bluesky.protocols import Status
from bluesky.utils import Msg, ensure_generator, plan

ALLOWED_VERBS = {
    "collect",
    "complete",
    "kickoff",
    "locate",
    "null",
    "prepare",
    "set",
    "sleep",
    "stage",
    "stop",
    "trigger",
    "unstage",
    "wait",
}


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


class ParallelPlanManager:
    def __init__(self) -> None:
        self._running_plans: Deque[_RunningSubplan] = deque()
        self._i: int = 0

    def next(self):
        """Send resp into the subplan whose turn is next, and return the resulting
        message, or None if all subplans are waiting."""
        if self.running:
            return p.send(p._resp) if (p := self._get_next_plan()) is not None else None
        # TODO is this an error?

    def add_subplan(self, plan: Generator[Msg, None, None], status: ParallelPlanStatus):
        self._running_plans.append(_RunningSubplan(plan, status))

    def print_plans(self):
        return str(list(self._running_plans))

    def store_response(self, resp):
        self._running_plans[self._i - 1]._resp = resp

    @property
    def running(self):
        return len(self._running_plans) > 0

    def _get_next_plan(self):
        if self._i >= len(self._running_plans):
            # we have gone around all subplans
            self._i = 0
            return None
        while (p := self._running_plans[self._i]).waiting:
            self._i += 1
            if self._i >= len(self._running_plans):
                # all remaining sub_plans are waiting
                return None
        self._i += 1
        return p


class _RunningSubplan:
    """Holds a little info about a currently running subplan"""

    def __init__(self, plan, status):
        self._plan = ensure_generator(plan())
        self._status: ParallelPlanStatus = status
        self._waiting = False
        self._waiting_status: Status | None = None
        self._group_id = uuid4()
        self._resp: Any | None = None

    def send(self, resp):
        """Send into the subplan, and return the resulting message"""
        msg = self._plan.send(resp)
        msg = self._validate_msg(msg)
        return msg

    @property
    def waiting(self):
        # this should check an actual status and reset _waiting if done
        return self._waiting

    def _validate_msg(self, msg: Msg):
        """Make sure that messages are allowed and update"""
        if msg.command not in ALLOWED_VERBS:
            raise IllegalSubplanCommand(
                f"Command {msg.command} cannot be executed in parallel "
                "and is not allowed in sub-plans."
            )
        if msg.command == "wait":
            self._waiting = True
            # TODO: actually wait for things - see waiting() above - get the status
            return Msg("null", message="Replaces a wait from a subplan!")
        return msg


class IllegalSubplanCommand(Exception): ...


class SubplanNotFinished(Exception): ...


@plan
def run_sub_plan(
    plan: Callable[[], Generator[Msg, Any, Any]], group: Any | None = None
):
    """Run the specified plan in parallel to the main execution. Returns a status
    object representing the subplan.

    Any groups in the subplan are"""

    return (yield Msg("run_parallel", plan))


@plan
def wait_for_all_subplans():
    """Block and wait for all currently running subplans to complete"""

    return (yield Msg("wait_for_all_subplans", plan))
