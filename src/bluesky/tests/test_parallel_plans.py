import sys


if sys.version_info >= (3, 10):
    from functools import partial
    from unittest.mock import ANY

    import pytest
    from ophyd_async.core.mock_signal_utils import get_mock_put, set_mock_put_proceeds
    from ophyd_async.core.signal import SignalRW
    from ophyd_async.epics.signal import epics_signal_rw
    from bluesky.utils import IllegalMessageSequence
    from bluesky import preprocessors as bpp
    import bluesky.plan_stubs as bps
    from bluesky.utils.parallel_plans import run_sub_plan

    @pytest.fixture
    async def motors():
        motors = tuple(
            epics_signal_rw(float, name=f"motor{i}", read_pv=f"MOCK_PV_{i}")
            for i in range(4)
        )
        [await m.connect(mock=True) for m in motors]
        return motors

    def _set_motor(motor, pos: float):
        yield from bps.abs_set(motor, pos, wait=True)


@pytest.mark.skipif(sys.version_info < (3, 10), reason="ophyd-async only on py3.10+")
class TestParallelPlans:
    async def test_multiple_plans_run(self, RE, motors: tuple[SignalRW, ...]):
        motor1, motor2, motor3, motor4 = motors

        @bpp.run_decorator()
        def _parallel_plan():
            [set_mock_put_proceeds(m, False) for m in (motor1, motor2, motor3)]
            st1 = yield from run_sub_plan(partial(_set_motor, motor1, 3), group="moves")
            st2 = yield from run_sub_plan(partial(_set_motor, motor2, 5), group="moves")
            st3 = yield from run_sub_plan(partial(_set_motor, motor3, 7), group="moves")
            yield from bps.abs_set(motor4, 9)
            for st in (st1, st2, st3):
                assert not st.done
            get_mock_put(motor4).assert_called_with(9, wait=True, timeout=ANY)
            [set_mock_put_proceeds(m, True) for m in (motor1, motor2, motor3)]

        RE(_parallel_plan())

        assert await motor1.get_value() == 3
        assert await motor2.get_value() == 5
        assert await motor3.get_value() == 7

    async def test_run_para_must_be_in_run(self, RE, motors: tuple[SignalRW, ...]):
        _, motor2, _, _ = motors

        def _parallel_plan():
            st2 = yield from run_sub_plan(partial(_set_motor, motor2, 5), group="moves")

        with pytest.raises(IllegalMessageSequence):
            RE(_parallel_plan())
