.DEFAULT_GOAL := help
.PHONY: help docs docs-check lint test dist docker

# Comma-separated, same format as buildx --platform. Defaults to this machine's arch;
# the release workflow sets linux/amd64,linux/arm64.
PLATFORM ?= linux/$(shell go env GOARCH)
IMAGE    ?= olakego/k8s-worker
TAG      ?= local
PUSH     ?=

comma := ,

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

docs: ## Regenerate helm/olake/OLAKE_README.md from values.yaml
	@./scripts/helm-docs.sh

docs-check: docs ## Fail if helm/olake/OLAKE_README.md is out of date (run by CI)
	@git diff --exit-code helm/olake/OLAKE_README.md || \
		{ echo "helm/olake/OLAKE_README.md is out of date. Run 'make docs' and commit the result." >&2; exit 1; }

lint: ## Lint the chart with helm lint
	@helm lint helm/olake

test: ## Run chart unit tests (needs the helm-unittest plugin)
	@helm unittest helm/olake

# One worker binary per PLATFORM in worker/dist/<os>/<arch>/. The worker Dockerfile only
# copies these, so the image build never compiles Go (and never runs under QEMU).
# worker/dist is wiped first, so a binary from an older build never ends up in the image.
dist: ## Cross-compile the worker to worker/dist/<os>/<arch>/olake-worker for PLATFORM
	@for p in $(subst $(comma), ,$(PLATFORM)); do \
		case "$$p" in */*/*) echo "dist: $$p: platform variants are not supported" >&2; exit 1;; esac; \
	done
	@rm -rf worker/dist
	@for p in $(subst $(comma), ,$(PLATFORM)); do \
		echo "go build $$p"; \
		(cd worker && CGO_ENABLED=0 GOOS=$${p%/*} GOARCH=$${p#*/} \
			go build -trimpath -ldflags="-s -w" -o dist/$$p/olake-worker .) || exit 1; \
	done

docker: dist ## Build the worker image for PLATFORM (PUSH=--push to push it)
	docker buildx build --platform $(PLATFORM) -t $(IMAGE):$(TAG) $(PUSH) worker
