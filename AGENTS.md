# Repository Guidelines

## Design Philosophy
Keep abstractions minimal. Favor the simplest interface that satisfies current lessons and resist speculative hooks; students should extend code incrementally as the curriculum demands. Avoid staging or committing changes unless explicitly requested.

## Project Structure & Module Organization
Follow idiomatic Go layout to keep components discoverable. Application entry points live under `cmd/<service>`; shared domain logic belongs in `internal/`, and reusable libraries in `pkg/`. Store documentation in `docs/` so automation can publish it without touching code packages.

## Environment Setup
Use Go 1.21 or newer and enable modules (`GO111MODULE=on`). Run `go mod tidy` whenever dependencies change to keep `go.sum` deterministic. Local tooling such as `gofmt`, `goimports`, and `staticcheck` should be available on your PATH.

## Build, Test, and Development Commands
- `go build ./...` compiles every package to surface type errors early.
- `go run ./cmd/<service>` executes the selected entry point for rapid iteration.
- `go test ./...` runs the full test suite; append `-run <regex>` for focused checks.
- `go test -coverprofile=coverage.out ./...` followed by `go tool cover -html=coverage.out` reviews coverage hotspots before submitting a PR.

## Coding Style & Naming Conventions
Run `gofmt -w` or your editor’s format-on-save before committing; tabs are expected for indentation. Organize imports with `goimports` to enforce standard grouping. Package names stay lowercase without underscores. Exported identifiers use PascalCase with concise nouns, while unexported helpers stay camelCase. Test functions follow `TestFeature` patterns that mirror the code under test.

## Testing Guidelines
Prefer table-driven tests in `*_test.go` files and keep fixtures in `testdata/`. Mock external services at package boundaries to avoid brittle network calls. Aim for ≥80% package-level coverage and ensure parallel-safe tests (`t.Parallel()`) when possible.

## Commit & Pull Request Guidelines
Write commit messages in the imperative mood ("Add parser"), keeping the subject ≤72 characters with optional bullet details in the body. Reference issues with `Refs #ID` when relevant. Pull requests should summarize intent, list major code paths touched, and describe testing evidence (command output or screenshots). Include rollout or migration notes if deployment steps differ from the default.
