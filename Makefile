CLUSTER ?= st
export CLUSTER

check:
	sh scripts/preflight.sh
up:
	sh scripts/up.sh
down:
	sh scripts/down.sh
test:
	python -m pytest -q
controller:
	python -m stctl controller
