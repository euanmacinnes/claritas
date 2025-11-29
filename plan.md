# ClaritasAI — Working Plan and TODOs

Date: 2025-11-29 20:56 (local)

This document captures the current state of implementation for the Rust-based ClaritasAI suite and lists remaining actions, TODOs, and items to re-implement later.

## 1) Scope and goals (recap)
- Rust multi-crate workspace with agentic runtime and MCP-driven tools.
- Default local Rust MCP hosts (stdio JSON-RPC), overridable by YAML/CLI to use remote MCP.
- Single user-facing web UI: server-rendered HTMX chat at /chat with live progress.
- Persistence in Clarium (Postgres at postgres://localhost:5433/claritas).
- Two-agent gate (Planner -> Verifier -> Executor), A/B testing, notifications, and self-learning (global + project memories).

## 2) Current status (implemented)
- Workspace and crates: core, runtime, mcp, lua, plan, verify, abtest, web, notify, app; MCP hosts: claritas_mcp_python, claritas_mcp_rust, claritas_mcp_clarium.
- Config loader and YAML (configs/claritasai.yaml).
- Local MCP supervision (spawn/monitor/backoff), persistent stdio JSON-RPC clients, ToolRegistry with prefix routing.
- Web server (Axum) with /chat, /chat/stream (SSE), /health, and GET /runs/:id/metrics.
- DB bootstrap/migrations (runs, plans, plan_verdicts, executions, steps, metrics, global_memories, project_memories). Auto-create DB if missing.
- Planner -> Verifier -> Executor MVP wired to /chat; per-step and total runtime metrics persisted and linked.
- Clarium MCP: clarium.validate_sql, clarium.parse, clarium.execute, db.schema.inspect, db.schema.diff, db.query.explain with normalized errors.
- Python MCP: python.lint, python.format, python.test.unit, python.test.integration, python.coverage.report, python.sec.check, python.warn.
- Rust MCP: rust.cargo.build.debug, rust.cargo.test, rust.cargo.check, rust.clippy, rust.fmt, rust.doc, rust.coverage.report.
- Planner drafts a multi-step plan: meta.ping, python.lint, rust.cargo.build.debug, clarium.validate_sql.
- Build is green across workspace.

## 3) Remaining actions (near-term)
1. UI timeline and streaming enhancements
   - Live step cards with status, timestamps, stdout/stderr panes via SSE. [UI]
   - Metrics summary block (total runtime, per-step durations) on /chat. [UI]
   - DB connectivity/migration status banner. [UI]
2. Executor robustness and persistence
   - Finer-grained event streaming (start/finish per step, partial outputs). [Runtime]
   - Persist artifact pointers per step (stdout/stderr snippets, file outputs). [DB]
3. Clarium DB helpers expansion
   - Include indexes/constraints in db.schema.diff; human-readable diff summaries. [Clarium MCP]
   - Implement db.index.suggest (basic heuristics). [Clarium MCP]
   - Ensure execution timing surfaced consistently (db.query.explain with analyze=true). [Clarium MCP]
4. Notifications (YAML-driven)
   - Implement Telegram, Email, WhatsApp providers with queue + retry and templating. [Notify]
   - Triggers: user_input_required, run completion/failure (deep links to /chat). [Notify]
5. A/B experiment runner (MVP)
   - Variants across planner/verifier models; metrics: valid SQL rate, tests passed, runtime. [ABTest]
   - CSV/JSON export and minimal UI summary. [Web]
6. Self-learning (global + project memories)
   - Write step summaries, diffs, outcomes; tag by task/tool/model/project. [Runtime]
   - Retrieval to seed planner/verifier context. [Plan/Verify]
7. MCP supervision & client hardening
   - Health probes surfaced to UI; reconnect on host restart without user impact. [App/MCP]
   - Backpressure, timeouts, backoff tuning; telemetry counters. [MCP]
8. Persistence schema growth
   - New tables: artifacts, step_attachments (checksums, paths, sizes, mime). [DB]
   - Indexes for metrics/runs lookups; maintenance notes. [DB]
9. Cross-platform polish and detection
   - External tool detection (ruff, black, pytest, mypy, cargo llvm-cov); graceful fallbacks. [Hosts]
   - Windows/Linux/macOS path/encoding quirks. [All]
10. Testing and CI
   - Unit tests for MCP framing/timeouts/reconnect. [MCP]
   - Integration tests per tool (python, rust, clarium). [Hosts]
   - E2E smoke test (objective -> plan -> verify -> execute). [App/Web]
   - CI workflows for multi-OS build/test. [Repo]
11. CLI polish & docs
   - CLI: --mcp.disable, --mcp.remote=id=url, claritasai mcp start --tool ... --serve http. [App]
   - Config validation with clear diagnostics; precedence rules (CLI vs YAML). [App]
   - README/Quickstart; ops guide for remote MCP mode. [Docs]

## 4) Deferred / re-implement later
- Replace stub Verifier with LLM-assisted verifier via configured MCP chat tools.
- Advanced planner using retrieval-augmented prompts and tool discovery from capabilities.
- Rich A/B/N benchmarking and report visualization beyond MVP.
- Artifact repository integration and downloadable bundles.
- Web auth integration (proxied in front); add RBAC later if needed.
- Lua scripting orchestration and hot-reload hooks beyond current stubs.

## 5) Open TODOs (checklist)
- [ ] /chat timeline UI with step cards and SSE-bound logs.
- [ ] Surface per-step stdout/stderr and store trimmed copies in DB (or artifacts table when added).
- [ ] Implement db.index.suggest in claritas_mcp_clarium.
- [ ] Enhance db.schema.diff to include indexes and constraints.
- [ ] Add notifications: Telegram, Email, WhatsApp (config keys already present).
- [ ] Implement A/B runner execution path and UI summary route.
- [ ] Implement memory write/retrieval APIs and seed planner/verifier with top-k relevant memories.
- [ ] Add artifacts, step_attachments tables and wire writes from executor.
- [ ] Tool detection helpers and user-friendly errors when tools are missing.
- [ ] CI workflows for Windows/Linux/macOS; smoke test job.
- [ ] CLI help texts and config validation errors; document CLI vs YAML precedence.

## 6) Risks and notes
- External tools may be unavailable; provide clear guidance and fallbacks.
- Long-running commands (coverage, doc builds, explain analyze) need generous timeouts and cancellation.
- Ensure Postgres DSN correctness; auto-create DB may require permissions.
- Maintain stable error schema for Clarium tools to avoid breaking UI/metrics.

## 7) How to run (current)
1. Ensure Postgres at postgres://localhost:5433 (DB claritas will be created/migrated).
2. Adjust configs/claritasai.yaml (paths and DSN).
3. Start app:
   cargo run -p claritasai-app -- --config configs/claritasai.yaml --bind 0.0.0.0:7040
4. Open http://localhost:7040/chat and submit an objective. Metrics at /runs/{id}/metrics.

## 8) Ownership / next milestone
- Next milestone: UI timeline, executor event granularity, Clarium diff improvements, and notifications.
- Keep build green; prioritize robustness of MCP supervision and client reconnect.
