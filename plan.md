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

## 3) Priority focus: Self‑development via Rust API and multi‑role LLM planning
Goals: Enable Claritas to improve itself using its Rust API and MCP tools, guided by an LLM planning stack with three coordinated roles.

Roles and loop (dual‑personality + manager):
- Professional Developer (Dev): proposes designs/architectures and detailed implementation plans; selects tools and produces patches/specs.
- Professional Systems Tester / QA (QA): expands/validates the plan; defines acceptance tests, edge cases, and risk checks; blocks unsafe/underspecified work.
- Professional Software Engineering Manager (Mgr): enforces scope, milestones, and quality bars; arbitrates Dev↔QA disagreements; keeps execution on track.

Key capabilities to build:
- Rust self‑development API
  - High‑level operations: code.generate, code.edit (structured patches), code.refactor.*, fs.* with safety (dry‑run, diff, rollback).
  - Project graph & impact analysis (Rust and Python): parse Cargo workspace, map crates/files/tests; for Python, infer modules/tests.
  - Test scaffolding: create unit/integration tests and harness wiring; link to metrics collection.
- Enhanced planning via LLM+MCP
  - Planner uses MCP chat tools (OpenAI/Ollama) with retrieval from memories; auto‑discovers tools via meta.capabilities and builds executable Plans with assertions and rollback.
  - Verifier (QA) performs structured critique; returns PlanVerdict with required_changes; can auto‑author tests.
  - Manager role manages OKRs: objective → milestones → steps, enforces budgets/timeouts, and resumes runs on interruptions.
- Memory‑driven learning
  - Write outcome summaries, diffs, and test artifacts to global/project memories; retrieve top‑k relevant memories into prompts for Dev/QA/Mgr.
- Safety & governance
  - Guardrails: dry‑run first, require QA approval before write operations, and Mgr approval for scope/budget changes.
  - Audit trail: persist decisions, diffs, tests, and verdicts.

Deliverables for this focus:
- New agents: DevAgent, QAAgent, ManagerAgent (Rust), orchestrated in runtime; optional Lua hooks later.
- Planning protocol JSON schemas: Plan{assumptions, constraints, risks, steps, assertions, rollback}, PlanVerdict{status, rationale, required_changes[]}.
- Patch application toolchain: unified diff/structured edits with validation (compiles/tests before commit), rollback on failure.
- UI: role‑tagged messages and approvals, milestone progress, and prompts for user input when blocking.

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

12. Multi‑role planning orchestration (Dev/QA/Mgr)
    - Implement DevAgent, QAAgent, ManagerAgent with explicit contracts and prompts; wire to MCP chat providers. [Runtime]
    - Add planning loop: Dev drafts → QA critiques (NeedsChanges with diffs/tests) → repeat until Approved → Mgr signs off per milestone. [Runtime]
    - Persist role messages, decisions, and change requests; expose in UI. [Web/DB]

13. Self‑development patch pipeline
    - Add structured patch model (unified diff and AST‑aware edits where available). [Core]
    - Pre‑commit checks: compile/tests/lints; auto‑rollback on failures; metrics emitted. [Runtime]
    - Safe write gates: require QA Approve and Mgr OK for scope/risk changes. [Runtime]

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
- [ ] Implement DevAgent, QAAgent, ManagerAgent with MCP chat connectors and prompts.
- [ ] Implement planning loop with NeedsChanges cycle and milestone sign‑off.
- [ ] Introduce structured patch model and guarded apply (dry‑run, diff preview, rollback).
- [ ] Add project graph/impact analysis (Cargo workspaces; Python modules/tests discovery).
- [ ] Persist role transcripts and decisions; render role timeline in UI.

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
