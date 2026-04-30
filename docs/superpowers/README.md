# docs/superpowers/

Brainstorming and planning artifacts. Two flavors:

- `specs/YYYY-MM-DD-<topic>-design.md` — the **design**. Locked decisions, architecture, schemas, late-data policy, observability, testing strategy. Written during the brainstorming phase; updated only when the underlying design choice changes.
- `plans/YYYY-MM-DD-<topic>-plan.md` — the **implementation plan**. Phase-by-phase, task-by-task, with exact code/commands. The execution artifact; subagents work from this file.

A spec lives, a plan is consumed and (typically) becomes stale once executed — useful for archaeology, not for guiding future work.
