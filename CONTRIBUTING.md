# Contributing to the OLake Helm Chart

Thanks for helping improve the chart. This guide covers how we branch, write commits, open pull requests and document chart values.

For changes to OLake itself, see the [OLake repository](https://github.com/datazip-inc/olake). For documentation on olake.io, see [olake-docs](https://github.com/datazip-inc/olake-docs).

## Prerequisites

- [Helm](https://helm.sh/docs/intro/install/) 3 or later
- `make`, `git`, `curl` and `tar`

`helm-docs` is downloaded automatically when needed (see [Chart values and documentation](#chart-values-and-documentation)).

Run `make` from the repository root to list the available targets.

## Branching

- Branch from `staging` and open your pull request against `staging`. Never target `master`.
- `master` holds released code. Maintainers promote `staging` to `master` through a release pull request.
- Name branches `<type>/<short-description>`, in lowercase kebab-case, using the types below:

| Type | Use for |
|------|---------|
| `feat/` | New chart features or values |
| `fix/` | Bug fixes |
| `chore/` | Maintenance, dependency bumps, releases |
| `docs/` | Documentation only |
| `test/` | Tests only |
| `refactor/` | Code or template changes with no behavior change |
| `ci/` | GitHub Actions workflows and CI scripts |

Examples: `feat/job-pod-labels`, `fix/nfs-storage-class`, `docs/contributing`.

## Commits and pull request titles

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<optional scope>): <short summary in imperative mood>
```

- **Types:** `feat`, `fix`, `chore`, `docs`, `test`, `refactor`, `ci`, the same set as branch names.
- **Scopes** name the area changed, for example `helm`, `worker`, `fusion`, `postgresql`, `ci`.
- **Breaking changes:** add `!` after the type or scope, for example `feat(helm)!: rename olakeWorker.config`, and explain the migration in the pull request description.

Examples:

```
feat(helm): add global.jobPodLabels for activity pods
fix(worker): cross-compile release image
docs: add contributing guide
```

Pull requests are squash merged, so the **pull request title becomes the commit on `staging`**. Make sure it follows the same format.

## Pull requests

- Keep one change per pull request.
- Open a [draft pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests#draft-pull-requests) while the work is in progress, and mark it ready when you want a review.
- Fill in the pull request template, including how you tested the change.
- Sign the Contributor License Agreement when the CLA Assistant asks.

### During review

- Reply to every review comment, either with what you changed or with why you disagree.
- Push follow-up commits to the same branch. Do not force push during review, and do not close the pull request and open a new one. Reviewers need to see what changed since their last review, and the history is squashed on merge anyway.
- Request another review only after every comment has been handled.

## Chart values and documentation

`helm/olake/values.yaml` is the source of truth for chart configuration. The values reference in [`helm/olake/OLAKE_README.md`](./helm/olake/OLAKE_README.md) is generated from its comments by [helm-docs](https://github.com/norwoodj/helm-docs). Do not edit that file by hand.

### Comment format

Document each value with a `# --` comment directly above the key:

```yaml
global:
  # -- Global storage class for all persistent volumes.
  # Set to "" to use the cluster's default storage class.
  storageClass: ""

  # -- Service account settings for job pods created by olake-workers
  # @default -- creates a service account named after the release
  jobServiceAccount:
    create: true
    name: ""
```

- `# --` starts the description. The comment lines that follow are part of the same description.
- Plain `#` comments are ignored. A key without a `# --` comment still appears in the table, but with an empty description.
- Document nested keys with their own `# --` comment.
- Use `# @default -- <text>` when the literal default is unclear, for example an empty value that the chart fills in.
- Descriptions are rendered on a single line in a table, so keep them short. Put long YAML examples in [`helm/olake/README.md`](./helm/olake/README.md) instead.

### When you add or change a value

1. Add or update the `# --` comment in `values.yaml`.
2. Update `helm/olake/values.schema.json` to match.
3. For a new top-level key, add it to a category in `helm/olake/OLAKE_README.md.gotmpl`. Otherwise it is listed under "Other Configurations".
4. Run the checks and commit the regenerated `OLAKE_README.md`:

```bash
make lint
make docs
```

`make docs` uses `helm-docs` from your `PATH` if it is the pinned version. Otherwise it downloads that version into `.bin/` and verifies its checksum first.

CI runs `make docs-check` on every pull request that touches the chart values, template or docs tooling. It fails if `OLAKE_README.md` is out of date.

## Testing your changes

Lint the chart and render it with your values before opening a pull request:

```bash
make lint
helm template olake helm/olake -f my-values.yaml
```

For template or behavior changes, install the chart on a local cluster (kind, minikube or k3d) and describe what you checked in the pull request.
