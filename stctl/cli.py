import argparse
import time
import uuid

from kubernetes.client.rest import ApiException

from .cluster import connect, pod_body, POOL
from .controller import Controller
from .core import load_config, pick_shape
from .sim import register as register_sim
from .live import register as register_live
from .dash import register as register_dash


def cmd_controller(a):
    Controller(connect(a.context), load_config(a.config)).run()


def cmd_submit(a):
    cfg, v1 = load_config(a.config), connect(a.context)
    shape = pick_shape(cfg, a.cpu, a.memory_gi)
    v1.create_namespaced_pod(a.namespace, pod_body(a.name, shape, a.cpu, a.memory_gi))
    print(f"{a.name}: {a.cpu} cpu / {a.memory_gi} Gi -> shape {shape.name}")


def cmd_status(a):
    v1 = connect(a.context)
    nodes = v1.list_node(label_selector=f"pool={POOL}").items
    pods = v1.list_pod_for_all_namespaces(label_selector="tenancy=single").items
    on_node = {p.spec.node_name: p.metadata.name for p in pods if p.spec.node_name}
    pending = [p.metadata.name for p in pods if not p.spec.node_name]
    print(f"{'NODE':<16}{'SHAPE':<8}TASK")
    for n in sorted(nodes, key=lambda n: n.metadata.name):
        print(f"{n.metadata.name:<16}{n.metadata.labels['shape']:<8}"
              f"{on_node.get(n.metadata.name, '(idle / warm)')}")
    for name in pending:
        print(f"{'-':<16}{'-':<8}{name} (pending)")


def cmd_bench(a):
    cfg, v1 = load_config(a.config), connect(a.context)
    shape = pick_shape(cfg, a.cpu, a.memory_gi)
    names = []
    for _ in range(a.count):
        name = f"bench-{uuid.uuid4().hex[:5]}"
        names.append(name)
        t0 = time.monotonic()
        v1.create_namespaced_pod(a.namespace, pod_body(name, shape, a.cpu, a.memory_gi))
        while time.monotonic() - t0 < 180:
            if v1.read_namespaced_pod(name, a.namespace).status.phase == "Running":
                break
            time.sleep(0.2)
        print(f"{name}: running after {time.monotonic() - t0:.1f}s (shape {shape.name})")
    for name in names:
        try:
            v1.delete_namespaced_pod(name, a.namespace)
        except ApiException:
            pass


def main():
    p = argparse.ArgumentParser(prog="stctl")
    p.add_argument("--config", default="config/shapes.yaml")
    p.add_argument("--context", default="kwok-st")
    p.add_argument("--namespace", default="default")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("controller").set_defaults(fn=cmd_controller)
    sub.add_parser("status").set_defaults(fn=cmd_status)

    s = sub.add_parser("submit")
    s.add_argument("--name", required=True)
    s.add_argument("--cpu", type=float, required=True)
    s.add_argument("--memory-gi", type=float, required=True)
    s.set_defaults(fn=cmd_submit)

    b = sub.add_parser("bench")
    b.add_argument("--cpu", type=float, required=True)
    b.add_argument("--memory-gi", type=float, required=True)
    b.add_argument("--count", type=int, default=1)
    b.set_defaults(fn=cmd_bench)

    register_sim(sub)
    register_live(sub)
    register_dash(sub)

    a = p.parse_args()
    a.fn(a)
