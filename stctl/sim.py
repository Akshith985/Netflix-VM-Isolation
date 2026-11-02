"""Discrete-event replay: compare warm-pool strategies on a synthetic workload.

No cluster needed. Time is simulated, so a 2-hour trace runs in about a second.
"""
import bisect
import heapq
import itertools
import json
import math
import random
from collections import defaultdict
from dataclasses import dataclass, replace

from .core import Shape, load_config, pick_shape

STRATEGIES = ("none", "fixed", "adaptive")
FINE_CPUS = (2, 4, 6, 8, 12, 16, 24, 32, 48, 64)
# (cpu request, weight): skewed toward small and mid-size tasks
SIZES = ((1.5, 10), (3, 25), (6, 20), (10, 15), (14, 12), (28, 8), (40, 6), (60, 4))
MEM_PER_CPU = 3.5


@dataclass(frozen=True)
class Req:
    id: int
    t: float
    cpu: float
    mem: float
    dur: float


@dataclass(frozen=True)
class Params:
    tick: float = 2.0          # reconcile interval (matches the live controller)
    start_seconds: float = 1.0  # pod start on a ready node (image assumed cached)
    max_warm: int = 8
    quantile: float = 0.9
    buckets: int = 20


def gen_trace(seed=1, hours=2.0, rate_per_min=6.0, burst_prob=0.15):
    rng = random.Random(seed)
    cpus = [c for c, _ in SIZES]
    weights = [w for _, w in SIZES]
    horizon = hours * 3600

    def duration():
        return min(14400.0, max(60.0, rng.lognormvariate(math.log(600), 1.0)))

    raw, t = [], 0.0
    while True:
        t += rng.expovariate(rate_per_min / 60.0)
        if t >= horizon:
            break
        cpu = rng.choices(cpus, weights)[0]
        raw.append((t, cpu, duration()))
        if rng.random() < burst_prob:   # e.g. a fleet deploy: same-size tasks together
            for _ in range(rng.randint(2, 4)):
                raw.append((t + rng.uniform(0, 5), cpu, duration()))
    raw.sort()
    return [Req(i, t, c, round(c * MEM_PER_CPU, 1), d)
            for i, (t, c, d) in enumerate(raw)]


def fine_shapes():
    return tuple(Shape(f"f{c}", c, c * 4) for c in FINE_CPUS)


class Node:
    __slots__ = ("shape", "launched", "ready", "state", "idle_since")

    def __init__(self, shape, launched, ready):
        self.shape, self.launched, self.ready = shape, launched, ready
        self.state, self.idle_since = "reserved", None


def adaptive_target(arr, now, bucket, p):
    """p-quantile of arrivals per boot-window over recent history."""
    lo = now - bucket * p.buckets
    counts = []
    for i in range(p.buckets):
        a = lo + i * bucket
        counts.append(bisect.bisect_left(arr, a + bucket) - bisect.bisect_left(arr, a))
    counts.sort()
    idx = min(len(counts) - 1, max(0, math.ceil(p.quantile * len(counts)) - 1))
    return min(p.max_warm, counts[idx])


def _pct(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    i = min(len(sorted_vals) - 1, max(0, math.ceil(q * len(sorted_vals)) - 1))
    return sorted_vals[i]


def simulate(trace, cfg, strategy, p=None):
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown strategy {strategy!r}")
    p = p or Params()
    cat = replace(cfg, shapes=fine_shapes()) if strategy == "none" else cfg
    boot, ttl = cfg.boot_seconds, cfg.idle_ttl_seconds
    bucket = max(boot, 1.0)

    seq = itertools.count()
    ev = []

    def push(t, kind, obj=None):
        heapq.heappush(ev, (t, next(seq), kind, obj))

    idle = defaultdict(list)       # shape -> booted, unused nodes
    booting = defaultdict(list)    # shape -> booting nodes nobody has claimed
    arrivals = defaultdict(list)   # shape -> arrival times (for adaptive)
    alive = set()
    st = dict(launches=0, hits=0, node_s=0.0, cpu_s=0.0,
              busy_s=0.0, req_cpu_s=0.0, run_cpu_s=0.0)
    lats = []
    done = 0

    def new_node(shape, now):
        n = Node(shape, now, now + boot)
        alive.add(n)
        st["launches"] += 1
        push(n.ready, "ready", n)
        return n

    def retire(n, now):
        alive.discard(n)
        st["node_s"] += now - n.launched
        st["cpu_s"] += n.shape.cpu * (now - n.launched)

    def start(req, n, now):
        wait = max(0.0, n.ready - now)
        lat = wait + p.start_seconds
        lats.append(lat)
        if wait == 0:
            st["hits"] += 1
        n.state = "busy"
        push(now + lat + req.dur, "end", n)
        st["busy_s"] += req.dur
        st["req_cpu_s"] += req.cpu * req.dur
        st["run_cpu_s"] += n.shape.cpu * req.dur

    def on_arrival(req, now):
        shape = pick_shape(cat, req.cpu, req.mem)
        arrivals[shape.name].append(now)
        if idle[shape.name]:
            n = idle[shape.name].pop()            # most recently idle
        elif booting[shape.name]:
            n = booting[shape.name].pop(0)        # earliest to finish booting
        else:
            n = new_node(shape, now)              # cold launch
        start(req, n, now)

    def target_for(shape, now):
        if strategy == "adaptive":
            return adaptive_target(arrivals[shape.name], now, bucket, p)
        if strategy == "fixed":
            return shape.warm
        return 0

    def reconcile(now):
        for shape in cat.shapes:
            name = shape.name
            target = target_for(shape, now)
            supply = len(idle[name]) + len(booting[name])
            for _ in range(max(0, target - supply)):
                n = new_node(shape, now)
                n.state = "free"
                booting[name].append(n)
            surplus = len(idle[name]) - target
            if surplus > 0:
                idle[name].sort(key=lambda n: n.idle_since)   # oldest first
                keep = []
                for n in idle[name]:
                    if surplus > 0 and now - n.idle_since > ttl:
                        retire(n, now)
                        surplus -= 1
                    else:
                        keep.append(n)
                idle[name] = keep

    for r in trace:
        push(r.t, "arrive", r)
    push(0.0, "tick")
    total, now, end_t = len(trace), 0.0, 0.0

    while ev:
        now, _, kind, obj = heapq.heappop(ev)
        if kind == "arrive":
            on_arrival(obj, now)
        elif kind == "ready":
            if obj.state == "free":
                booting[obj.shape.name].remove(obj)
                obj.state, obj.idle_since = "idle", now
                idle[obj.shape.name].append(obj)
        elif kind == "end":
            done += 1
            obj.state, obj.idle_since = "idle", now
            idle[obj.shape.name].append(obj)
            if done == total:
                end_t = now
                break
        elif kind == "tick":
            reconcile(now)
            if done < total:
                push(now + p.tick, "tick")

    for n in list(alive):          # charge surviving nodes up to the last task end
        retire(n, max(end_t, n.launched))

    lats.sort()
    n_tasks = max(1, total)
    return dict(
        strategy=strategy,
        tasks=total,
        hit_rate=st["hits"] / n_tasks,
        p50=_pct(lats, 0.50), p95=_pct(lats, 0.95), p99=_pct(lats, 0.99),
        cpu_hours=st["cpu_s"] / 3600,
        idle_share=1 - st["busy_s"] / st["node_s"] if st["node_s"] else 0.0,
        unused_cpu=1 - st["req_cpu_s"] / st["run_cpu_s"] if st["run_cpu_s"] else 0.0,
        launches=st["launches"],
    )


METRICS = ("tasks", "hit_rate", "p50", "p95", "p99",
           "cpu_hours", "idle_share", "unused_cpu", "launches")


def _load_trace(path):
    with open(path) as f:
        return [Req(**d) for d in json.load(f)]


def cmd_trace(a):
    trace = gen_trace(a.seed, a.hours, a.rate, a.burst_prob)
    with open(a.out, "w") as f:
        json.dump([r.__dict__ for r in trace], f)
    print(f"wrote {len(trace)} requests to {a.out}")


def cmd_replay(a):
    cfg = load_config(a.config)
    if a.boot is not None:
        cfg = replace(cfg, boot_seconds=a.boot)
    p = Params(max_warm=a.max_warm)
    if a.trace_file:
        traces = [_load_trace(a.trace_file)]
    else:
        traces = [gen_trace(s, a.hours, a.rate, a.burst_prob)
                  for s in range(1, a.seeds + 1)]
    strategies = [s.strip() for s in a.strategies.split(",")]

    print(f"{len(traces)} trace(s), ~{sum(len(t) for t in traces) // len(traces)} tasks each | "
          f"boot={cfg.boot_seconds}s  idle-ttl={cfg.idle_ttl_seconds}s  "
          f"start={p.start_seconds}s  max-warm={p.max_warm}")
    print(f"{'strategy':<10}{'hit%':>7}{'p50 s':>8}{'p95 s':>8}{'p99 s':>8}"
          f"{'cpu-hrs':>10}{'idle%':>8}{'unused%':>9}{'launches':>10}")
    for s in strategies:
        runs = [simulate(t, cfg, s, p) for t in traces]
        m = {k: sum(r[k] for r in runs) / len(runs) for k in METRICS}
        print(f"{s:<10}{m['hit_rate'] * 100:>7.1f}{m['p50']:>8.1f}{m['p95']:>8.1f}"
              f"{m['p99']:>8.1f}{m['cpu_hours']:>10.1f}{m['idle_share'] * 100:>8.1f}"
              f"{m['unused_cpu'] * 100:>9.1f}{m['launches']:>10.0f}")
    print("\nhit% = tasks that started without waiting for a boot | "
          "idle% = node time not running a task (incl. boot) | "
          "unused% = requested-vs-node CPU gap while running")


def register(sub):
    def common(sp):
        sp.add_argument("--hours", type=float, default=2.0)
        sp.add_argument("--rate", type=float, default=6.0, help="mean arrivals per minute")
        sp.add_argument("--burst-prob", type=float, default=0.15)

    r = sub.add_parser("replay", help="compare pool strategies on synthetic traces")
    common(r)
    r.add_argument("--seeds", type=int, default=3)
    r.add_argument("--strategies", default=",".join(STRATEGIES))
    r.add_argument("--boot", type=float, default=None, help="override boot seconds")
    r.add_argument("--max-warm", type=int, default=8)
    r.add_argument("--trace-file", default=None)
    r.set_defaults(fn=cmd_replay)

    t = sub.add_parser("trace", help="write a synthetic trace to a JSON file")
    common(t)
    t.add_argument("--seed", type=int, default=1)
    t.add_argument("--out", default="trace.json")
    t.set_defaults(fn=cmd_trace)
