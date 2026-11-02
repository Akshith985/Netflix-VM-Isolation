"""Replay a synthetic trace against the live cluster at accelerated speed."""
import threading
import time

from kubernetes.client.rest import ApiException

from .cluster import connect, pod_body
from .core import load_config, pick_shape
from .sim import gen_trace


def cmd_live(a):
    cfg, v1 = load_config(a.config), connect(a.context)
    trace = gen_trace(a.seed, a.hours, a.rate, a.burst_prob)
    waits, lock = [], threading.Lock()
    cold_cut = cfg.boot_seconds / 2
    t0 = time.monotonic()
    print(f"replaying {len(trace)} tasks at {a.speed:g}x speed "
          f"(~{trace[-1].t / a.speed:.0f}s)\n")

    def task(req):
        name = f"live-{req.id}"
        shape = pick_shape(cfg, req.cpu, req.mem)
        sub = time.monotonic()
        try:
            v1.create_namespaced_pod(a.namespace,
                                     pod_body(name, shape, req.cpu, req.mem))
        except ApiException as e:
            print(f"[submit] {name} failed: {e.reason}")
            return
        deadline = sub + 120
        while time.monotonic() < deadline:
            try:
                if v1.read_namespaced_pod(name, a.namespace).status.phase == "Running":
                    break
            except ApiException:
                pass
            time.sleep(0.2)
        else:
            print(f"[timeout] {name} never reached Running")
            return
        wait = time.monotonic() - sub
        with lock:
            waits.append(wait)
        kind = "warm" if wait < cold_cut else "COLD"
        print(f"t+{time.monotonic() - t0:5.1f}s {name:<10}{req.cpu:>5g} cpu -> "
              f"{shape.name:<4} started in {wait:4.1f}s  {kind}")
        time.sleep(req.dur / a.speed)
        try:
            v1.delete_namespaced_pod(name, a.namespace)
        except ApiException:
            pass

    threads = []
    for req in trace:
        delay = req.t / a.speed - (time.monotonic() - t0)
        if delay > 0:
            time.sleep(delay)
        th = threading.Thread(target=task, args=(req,), daemon=True)
        th.start()
        threads.append(th)
    for th in threads:
        th.join()

    if waits:
        waits.sort()
        cold = sum(w >= cold_cut for w in waits)
        print(f"\n{len(waits)} tasks | warm {len(waits) - cold} | cold {cold} | "
              f"p50 {waits[len(waits) // 2]:.1f}s | max {waits[-1]:.1f}s")


def register(sub):
    p = sub.add_parser("live", help="replay a trace against the live cluster")
    p.add_argument("--hours", type=float, default=0.25)
    p.add_argument("--rate", type=float, default=6.0)
    p.add_argument("--burst-prob", type=float, default=0.15)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--speed", type=float, default=20.0,
                   help="time compression for arrivals and task durations")
    p.set_defaults(fn=cmd_live)
