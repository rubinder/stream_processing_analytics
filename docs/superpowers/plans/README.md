# Implementation plans

One Markdown file per execution. Filename: `YYYY-MM-DD-<topic>-plan.md`.

Plans are derived from a spec in `../specs/` and broken into:

- **Phases** — independently demonstrable chunks (e.g., "Phase 4 — Pinot serving").
- **Tasks** within each phase — bite-sized with exact file paths, code, commands, and TDD steps where applicable.

These docs are consumed by subagent-driven execution and become stale once the work is done. They're kept around for traceability — useful when future work asks "why is this structured the way it is?"

Don't edit a plan after execution; if requirements change, write a follow-up plan.
