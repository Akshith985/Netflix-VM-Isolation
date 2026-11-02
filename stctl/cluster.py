"""Kubernetes helpers: client setup and object bodies."""
from kubernetes import client, config

POOL = "single-tenant"


def connect(context):
    config.load_kube_config(context=context)
    return client.CoreV1Api()


def _cpu(v):
    return f"{int(v * 1000)}m"


def _mem(gi):
    return f"{int(gi * 1024)}Mi"


def node_body(name, shape, cfg):
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": name,
            "annotations": {"kwok.x-k8s.io/node": "fake",
                            "node.alpha.kubernetes.io/ttl": "0"},
            "labels": {
                "type": "kwok", "pool": POOL, "shape": shape.name,
                "kubernetes.io/hostname": name,
                "kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64",
            },
        },
        "spec": {"taints": [{"key": "dedicated", "value": POOL,
                             "effect": "NoSchedule"}]},
        "status": {
            "capacity": {"cpu": _cpu(shape.cpu), "memory": _mem(shape.memory_gi),
                         "pods": "110"},
            "allocatable": {"cpu": _cpu(shape.cpu - cfg.overhead_cpu),
                            "memory": _mem(shape.memory_gi - cfg.overhead_memory_gi),
                            "pods": "110"},
            "nodeInfo": {
                "architecture": "amd64", "operatingSystem": "linux",
                "bootID": "kwok", "containerRuntimeVersion": "kwok",
                "kernelVersion": "kwok", "kubeProxyVersion": "kwok",
                "kubeletVersion": "kwok", "machineID": "kwok",
                "osImage": "kwok", "systemUUID": "kwok",
            },
        },
    }


def pod_body(name, shape, cpu, memory_gi):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "labels": {"tenancy": "single", "app": name}},
        "spec": {
            "priorityClassName": "single-tenant",
            "nodeSelector": {"pool": POOL, "shape": shape.name},
            "tolerations": [{"key": "dedicated", "operator": "Equal",
                             "value": POOL, "effect": "NoSchedule"}],
            "affinity": {"podAntiAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": [{
                    "labelSelector": {"matchLabels": {"tenancy": "single"}},
                    "topologyKey": "kubernetes.io/hostname"}]}},
            "containers": [{"name": "app", "image": "fake",
                            "resources": {"requests": {
                                "cpu": _cpu(cpu), "memory": _mem(memory_gi)}}}],
        },
    }
