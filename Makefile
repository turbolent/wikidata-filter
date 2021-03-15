.PHONY: lint
lint:
	cargo clippy -- -D warnings

fmt:
	cargo fmt --all
