.PHONY: docs docs-check

## Generate documentation using helm-docs
docs:
	@./scripts/helm-docs.sh

## Verify documentation is up to date (used by CI)
docs-check: docs
	@git diff --exit-code helm/olake/OLAKE_README.md
