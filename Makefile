.DEFAULT_GOAL := help
.PHONY: help docs docs-check lint

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

docs: ## Regenerate helm/olake/OLAKE_README.md from values.yaml
	@./scripts/helm-docs.sh

docs-check: docs ## Fail if helm/olake/OLAKE_README.md is out of date (run by CI)
	@git diff --exit-code helm/olake/OLAKE_README.md || \
		{ echo "helm/olake/OLAKE_README.md is out of date. Run 'make docs' and commit the result." >&2; exit 1; }

lint: ## Lint the chart with helm lint
	@helm lint helm/olake
