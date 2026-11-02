import pytest
from stctl.core import Config, Shape, pick_shape

CFG = Config(
    shapes=(Shape("s4", 4, 16), Shape("s8", 8, 32), Shape("s16", 16, 64)),
    overhead_cpu=0.5, overhead_memory_gi=1, boot_seconds=1, idle_ttl_seconds=1,
)

def test_rounds_up_to_smallest_fit():
    assert pick_shape(CFG, 3, 10).name == "s4"

def test_overhead_pushes_to_next_shape():
    assert pick_shape(CFG, 4, 10).name == "s8"   # s4 only has 3.5 cpu usable

def test_memory_can_be_the_constraint():
    assert pick_shape(CFG, 2, 20).name == "s8"

def test_too_big_raises():
    with pytest.raises(ValueError):
        pick_shape(CFG, 100, 10)
