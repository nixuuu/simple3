.PHONY: build test lint chaos chaos-loop chaos-disk-full

build:
	cargo build --release

test:
	cargo test

lint:
	cargo lint

# Runs the chaos in-process suite plus the random-kill shell loop.
# Disk-full is opt-in via `make chaos-disk-full` because it needs root + Linux.
chaos: chaos-stress chaos-loop

chaos-stress:
	cargo test --test chaos_test -- --nocapture

chaos-loop: build
	./chaos/kill-loop.sh

chaos-disk-full: build
	sudo ./chaos/disk-full.sh
