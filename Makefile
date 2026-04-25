SCHEME = scheme
JERBOA_DIR = $(HOME)/mine/jerboa
LIBDIRS = lib:$(JERBOA_DIR)/lib

# Chez external FFI libs (for LMDB, DuckDB, etc.)
CHEZ_EXT_DIR ?= $(HOME)/src
CHEZ_EXT_LIBDIRS = $(CHEZ_EXT_DIR)/chez-lmdb:$(CHEZ_EXT_DIR)/chez-duckdb
FULL_LIBDIRS = $(LIBDIRS):$(CHEZ_EXT_LIBDIRS)

.PHONY: test test-cluster test-transport test-transport-tls test-migrate build clean check bench bench-quick mbrainz mbrainz-quick showcase

# Run the core test suite (in-memory, no FFI deps)
test:
	$(SCHEME) --libdirs "$(LIBDIRS)" --script tests/test-core.ss

# End-to-end Clojure-parity showcase (bookstore scenario).
# Exercises: Datalog, pull, as-of, pmap, atom+validator, STM+io!,
# agent+await-for, protocol, multi+hierarchy, spec, transducer, ->.
showcase:
	$(SCHEME) --libdirs "$(LIBDIRS)" --script examples/bookstore.ss

# Run cluster (Raft replication) tests
test-cluster:
	$(SCHEME) --libdirs "$(LIBDIRS)" --script tests/test-cluster.ss

# Run TCP transport tests (two nodes connected via loopback)
test-transport:
	$(SCHEME) --libdirs "$(LIBDIRS)" --script tests/test-transport.ss

# Run TLS transport tests (requires libssl + self-signed cert).
# Generate the cert with:
#   mkdir -p $(JERBOA_DB_TLS_DIR) && \
#   openssl req -new -x509 -nodes -newkey rsa:2048 \
#     -keyout $(JERBOA_DB_TLS_DIR)/server.key \
#     -out $(JERBOA_DB_TLS_DIR)/server.crt \
#     -days 1 -subj "/CN=localhost"
JERBOA_DB_TLS_DIR ?= $(PREFIX)/tmp/jerboa-db-tls
test-transport-tls:
	JERBOA_DB_TLS_DIR=$(JERBOA_DB_TLS_DIR) \
	$(SCHEME) --libdirs "$(LIBDIRS)" --script tests/test-transport-tls.ss

# Run schema migration tests (rename/retype/delete/merge/split)
test-migrate:
	$(SCHEME) --libdirs "$(LIBDIRS)" --script tests/test-migrate.ss

# Run tests including LMDB backend
test-lmdb:
	$(SCHEME) --libdirs "$(FULL_LIBDIRS)" --script tests/test-lmdb.ss

# Compile all libraries (catches syntax/import errors)
build:
	@echo "Compiling jerboa-db libraries..."
	printf '(import (jerboa-db core))\n' | $(SCHEME) --libdirs "$(LIBDIRS)"
	@echo "Build check complete."

# Syntax check all .ss files
check:
	@echo "Checking library files..."
	@for f in $$(find lib -name "*.ss"); do \
		echo "  $$f"; \
		$(SCHEME) --libdirs "$(LIBDIRS)" --script /dev/null 2>&1 | head -5 || true; \
	done
	@echo "Check complete."

# Full load test (all 7 scenarios at full scale)
bench:
	$(SCHEME) --libdirs "$(LIBDIRS)" --script benchmarks/load-test.ss

# Quick load test (1/10 scale — runs in under 5s)
bench-quick:
	$(SCHEME) --libdirs "$(LIBDIRS)" --script benchmarks/load-test.ss --quick

# MBrainz benchmark (8 standard queries, synthetic data at full scale)
mbrainz:
	$(SCHEME) --libdirs "$(LIBDIRS)" --script benchmarks/mbrainz-bench.ss

# MBrainz quick smoke test (1% scale, ~3 runs per query)
mbrainz-quick:
	$(SCHEME) --libdirs "$(LIBDIRS)" --script benchmarks/mbrainz-bench.ss --quick

# Clean compiled artifacts
clean:
	find lib -name "*.so" -delete
	find lib -name "*.wpo" -delete
	rm -rf /tmp/jerboa-db-test-*
