#!/usr/bin/env sh
set -eu
kwokctl delete cluster --name "${CLUSTER:-st}"
