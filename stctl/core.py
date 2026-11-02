"""Shape catalog and rounding logic."""
from dataclasses import dataclass
import yaml


@dataclass(frozen=True)
class Shape:
    name: str
    cpu: float
    memory_gi: float
    warm: int = 0


@dataclass(frozen=True)
class Config:
    shapes: tuple
    overhead_cpu: float
    overhead_memory_gi: float
    boot_seconds: float
    idle_ttl_seconds: float


def load_config(path):
    with open(path) as f:
        raw = yaml.safe_load(f)
    shapes = tuple(sorted((Shape(**s) for s in raw["shapes"]),
                          key=lambda s: (s.cpu, s.memory_gi)))
    return Config(
        shapes=shapes,
        overhead_cpu=raw["overhead"]["cpu"],
        overhead_memory_gi=raw["overhead"]["memory_gi"],
        boot_seconds=raw["boot_seconds"],
        idle_ttl_seconds=raw["idle_ttl_seconds"],
    )


def allocatable(cfg, shape):
    """What a task can actually use: the machine minus overhead."""
    return shape.cpu - cfg.overhead_cpu, shape.memory_gi - cfg.overhead_memory_gi


def pick_shape(cfg, cpu, memory_gi):
    """Round a request up to the smallest shape that fits it."""
    for shape in cfg.shapes:
        a_cpu, a_mem = allocatable(cfg, shape)
        if cpu <= a_cpu and memory_gi <= a_mem:
            return shape
    raise ValueError(f"No shape fits {cpu} cpu / {memory_gi} Gi")
