IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
DEV   ?= dev
ANSWER ?= dev-answer
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures native test ref-fixtures dev run-dev check-dev difftest

## Build the Docker image exactly as the scorer will
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 fixtures with the scorer's docker flags
fixtures: build
	python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh

## Native (no Docker) build, unit tests, and fixtures
native:
	cargo build --release
test: native
	cargo test --release
	python3 scripts/run_fixtures.py fixtures -- target/release/shopping-sessions

## The Python reference on the fixtures (sanity check of the oracle itself)
ref-fixtures:
	python3 scripts/run_fixtures.py fixtures -- python3 reference/reference.py

## Dev set: generate, run in Docker with scorer flags (timed), compare with dev-answer/
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out
check-dev:
	python3 scripts/compare.py out $(ANSWER)

## Rust vs the Python reference on fresh seeds: make difftest SEEDS="2 3 4"
SEEDS ?= 2 3 4 5 6
difftest: native
	scripts/difftest.sh 200000 60000 $(SEEDS)
