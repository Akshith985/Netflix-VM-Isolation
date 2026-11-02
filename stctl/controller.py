"""Warm pool controller: keeps booted, idle nodes ready per shape."""
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

from kubernetes.client.rest import ApiException

from .cluster import POOL, node_body


class Controller:
    def __init__(self, v1, cfg):
        self.v1, self.cfg = v1, cfg
        self.booting = Counter()
        self.lock = threading.Lock()

    def _launch(self, shape):
        """Simulate a cold boot, then register the node."""
        with self.lock:
            self.booting[shape.name] += 1

        def boot():
            try:
                time.sleep(self.cfg.boot_seconds)
                name = f"{shape.name}-{uuid.uuid4().hex[:6]}"
                self.v1.create_node(body=node_body(name, shape, self.cfg))
                print(f"[launch] {name} ready after {self.cfg.boot_seconds}s")
            except Exception as e:
                print(f"[launch] failed: {type(e).__name__}: {e}")
            finally:
                with self.lock:
                    self.booting[shape.name] -= 1

        threading.Thread(target=boot, daemon=True).start()

    def reconcile(self):
        nodes = self.v1.list_node(label_selector=f"pool={POOL}").items
        pods = self.v1.list_pod_for_all_namespaces(label_selector="tenancy=single").items
        live = [p for p in pods if p.status.phase not in ("Succeeded", "Failed")]
        busy = {p.spec.node_name for p in live if p.spec.node_name}
        pending = Counter((p.spec.node_selector or {}).get("shape")
                          for p in live if not p.spec.node_name)
        now = datetime.now(timezone.utc)

        for shape in self.cfg.shapes:
            idle = [n for n in nodes
                    if n.metadata.labels.get("shape") == shape.name
                    and n.metadata.name not in busy]
            with self.lock:
                booting = self.booting[shape.name]

            # Scale up: one node per pending pod, plus the warm target.
            required = pending[shape.name] + shape.warm
            for _ in range(max(0, required - len(idle) - booting)):
                self._launch(shape)

            # Scale down: trim surplus idle nodes past the TTL.
            if pending[shape.name] == 0:
                surplus = len(idle) - shape.warm
                old = [n for n in idle if (now - n.metadata.creation_timestamp)
                       .total_seconds() > self.cfg.idle_ttl_seconds]
                for n in old[:max(0, surplus)]:
                    self.v1.delete_node(n.metadata.name)
                    print(f"[trim] removed idle {n.metadata.name}")

    def run(self, interval=2):
        print("controller running; warm targets:",
              {s.name: s.warm for s in self.cfg.shapes})
        while True:
            try:
                self.reconcile()
            except ApiException as e:
                print(f"[reconcile] API error: {e.reason}")
            time.sleep(interval)
