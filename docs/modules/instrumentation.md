# Instrumentation Module

## Purpose
Collect metrics and tracing hooks that help students observe system behavior during experiments. The instrumentation layer is intentionally lightweight so it can plug into logging-based dashboards or more advanced exporters later.

## Key Types
- `Counters` and `Histograms` interfaces abstract the backend (in-memory, Prometheus, stdout).
- `Registry` coordinates metric creation and finds existing instruments by name.
- `Scope` provides contextual tagging (e.g., by level number or file ID).

## Integration Points
- WAL and compaction modules emit latency and queue length metrics to visualize backpressure.
- Memtable and iterator modules record mutation counts and cache hit ratios.
- API layer aggregates per-operation traces for quick feedback in teaching labs.

## Extension Hooks
- Allow swapping registries to integrate with popular telemetry stacks without touching core code.
- Introduce sampling controls so students can study the impact of high-cardinality labels.

## Suggested Exercises
- Build a CLI status page that renders key metrics each second.
- Add invariant checks that fire when metrics exceed guardrails (e.g., too many immutable memtables).
