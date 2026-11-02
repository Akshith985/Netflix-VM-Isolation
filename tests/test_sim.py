from stctl.core import Config, Shape
from stctl.sim import STRATEGIES, Params, gen_trace, simulate

CFG = Config(
    shapes=(Shape("s4", 4, 16, 1), Shape("s8", 8, 32, 1), Shape("s16", 16, 64, 1),
            Shape("s32", 32, 128, 0), Shape("s64", 64, 256, 0)),
    overhead_cpu=0.5, overhead_memory_gi=1, boot_seconds=15, idle_ttl_seconds=60,
)
TRACE = gen_trace(seed=3, hours=0.5)


def test_trace_is_deterministic():
    assert gen_trace(1, 0.2) == gen_trace(1, 0.2)


def test_all_tasks_complete_for_every_strategy():
    for s in STRATEGIES:
        assert simulate(TRACE, CFG, s)["tasks"] == len(TRACE)


def test_latency_never_below_pod_start_time():
    floor = Params().start_seconds
    for s in STRATEGIES:
        assert simulate(TRACE, CFG, s)["p50"] >= floor


def test_shares_are_valid_fractions():
    for s in STRATEGIES:
        m = simulate(TRACE, CFG, s)
        assert 0 <= m["idle_share"] < 1
        assert 0 <= m["unused_cpu"] < 1
        assert 0 <= m["hit_rate"] <= 1
        assert m["cpu_hours"] > 0
