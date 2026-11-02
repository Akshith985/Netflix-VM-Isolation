#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
CLUSTER="${CLUSTER:-st}"
if kwokctl get clusters | grep -qx "$CLUSTER"; then
  echo "Cluster $CLUSTER already exists"
else
  kwokctl create cluster --name "$CLUSTER"
fi
kubectl --context "kwok-$CLUSTER" apply -f manifests/priorityclass.yaml
echo "Ready. kubectl context: kwok-$CLUSTER"
