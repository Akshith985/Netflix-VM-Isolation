"""Local dashboard: serves one page and a JSON view of the node pool."""
import json
import threading
from collections import Counter, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .cluster import POOL, connect
from .core import load_config

PAGE = Path(__file__).parent / "web" / "index.html"


def _cpu(quantity):
    q = str(quantity or "0")
    return float(q[:-1]) / 1000 if q.endswith("m") else float(q)


def _requested(pod):
    res = pod.spec.containers[0].resources
    return _cpu((res.requests or {}).get("cpu")) if res else 0.0


class Pool:
    def __init__(self, v1, cfg, context):
        self.v1, self.cfg, self.context = v1, cfg, context
        self.lock = threading.Lock()
        self.seen = set()
        self.counts = Counter()
        self.recent = deque(maxlen=12)
        self.warm_cut = cfg.boot_seconds / 2   # same rule as the live replay

    def snapshot(self):
        with self.lock:
            return self._build()

    def _build(self):
        now = datetime.now(timezone.utc)
        nodes = self.v1.list_node(label_selector=f"pool={POOL}").items
        pods = [p for p in self.v1.list_pod_for_all_namespaces(
                    label_selector="tenancy=single").items
                if p.status.phase not in ("Succeeded", "Failed")]
        by_node = {p.spec.node_name: p for p in pods if p.spec.node_name}

        for p in pods:
            if p.status.phase == "Running" and p.metadata.uid not in self.seen:
                self.seen.add(p.metadata.uid)
                started = p.status.start_time or now
                wait = max(0.0, (started - p.metadata.creation_timestamp).total_seconds())
                if wait > 300:      # started before the dashboard; wait unknown
                    continue
                kind = "warm" if wait < self.warm_cut else "cold"
                self.counts[kind] += 1
                self.recent.appendleft({
                    "task": p.metadata.name,
                    "shape": (p.spec.node_selector or {}).get("shape", "?"),
                    "wait_s": wait, "kind": kind})

        shapes = []
        for s in self.cfg.shapes:
            rows = []
            for n in sorted((n for n in nodes
                             if n.metadata.labels.get("shape") == s.name),
                            key=lambda n: n.metadata.name):
                p = by_node.get(n.metadata.name)
                rows.append({"name": n.metadata.name,
                             "state": "busy" if p else "idle",
                             "task": p.metadata.name if p else None,
                             "requested_cpu": _requested(p) if p else 0.0})
            waiting = sorted(
                (p for p in pods if not p.spec.node_name
                 and (p.spec.node_selector or {}).get("shape") == s.name),
                key=lambda p: p.metadata.creation_timestamp)
            shapes.append({
                "name": s.name, "cpu": s.cpu, "memory_gi": s.memory_gi,
                "warm_target": s.warm, "nodes": rows,
                "waiting": [{"task": p.metadata.name,
                             "requested_cpu": _requested(p),
                             "waited_s": (now - p.metadata.creation_timestamp).total_seconds()}
                            for p in waiting]})

        warm, cold = self.counts["warm"], self.counts["cold"]
        return {
            "context": self.context, "now": now.timestamp(),
            "boot_seconds": self.cfg.boot_seconds,
            "shapes": shapes,
            "totals": {
                "nodes": len(nodes),
                "busy": sum(1 for sh in shapes for n in sh["nodes"] if n["state"] == "busy"),
                "waiting": sum(len(sh["waiting"]) for sh in shapes),
                "started": warm + cold, "warm": warm, "cold": cold},
            "recent": list(self.recent)}


def make_handler(pool):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body, ctype):
            data = body if isinstance(body, bytes) else body.encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path == "/":
                self._send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
            elif path == "/api/state":
                try:
                    self._send(200, json.dumps(pool.snapshot()), "application/json")
                except Exception as e:   # tell the page what failed
                    self._send(500, json.dumps({"error": f"{type(e).__name__}: {e}"}),
                               "application/json")
            else:
                self._send(404, "Not found", "text/plain; charset=utf-8")

        def log_message(self, *args):
            pass
    return Handler


def cmd_dash(a):
    pool = Pool(connect(a.context), load_config(a.config), a.context)
    server = ThreadingHTTPServer((a.host, a.port), make_handler(pool))
    print(f"Dashboard at http://{a.host}:{a.port}  (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print()


def register(sub):
    p = sub.add_parser("dash", help="serve the node pool dashboard")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.set_defaults(fn=cmd_dash)
