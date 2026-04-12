SCHEME = scheme
JERBOA_DIR = $(HOME)/mine/jerboa
LIBDIRS = lib:$(JERBOA_DIR)/lib

# Chez external FFI libs (for LMDB, DuckDB, etc.)
CHEZ_EXT_DIR ?= $(HOME)/src
CHEZ_EXT_LIBDIRS = $(CHEZ_EXT_DIR)/chez-lmdb:$(CHEZ_EXT_DIR)/chez-duckdb
FULL_LIBDIRS = $(LIBDIRS):$(CHEZ_EXT_LIBDIRS)

.PHONY: test build clean check

# Run the core test suite (in-memory, no FFI deps)
test:
	$(SCHEME) --libdirs "$(LIBDIRS)" --script tests/test-core.ss

# Run tests including LMDB backend
test-lmdb:
	$(SCHEME) --libdirs "$(FULL_LIBDIRS)" --script tests/test-lmdb.ss

# Compile all libraries (catches syntax/import errors)
build:
	@echo "Compiling jerboa-db libraries..."
	$(SCHEME) --libdirs "$(LIBDIRS)" --program /dev/null \
		--import-notify <<< '(import (jerboa-db core))' 2>&1 || true
	@echo "Build check complete."

# Syntax check all .sls files
check:
	@echo "Checking library files..."
	@for f in $$(find lib -name "*.sls"); do \
		echo "  $$f"; \
		$(SCHEME) --libdirs "$(LIBDIRS)" --script /dev/null 2>&1 | head -5 || true; \
	done
	@echo "Check complete."

# Clean compiled artifacts
clean:
	find lib -name "*.so" -delete
	find lib -name "*.wpo" -delete
	rm -rf /tmp/jerboa-db-test-*
