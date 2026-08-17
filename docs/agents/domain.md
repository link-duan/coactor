# Domain Docs

This is a single-context repository. Engineering skills must use the project domain documentation when exploring, specifying, or modifying the codebase.

## Before exploring

- Read `CONTEXT.md` at the repository root.
- Read ADRs under `docs/adr/` that affect the area being changed.
- If either location does not exist, proceed silently rather than proposing it pre-emptively.

## Vocabulary

Use the canonical terms defined in `CONTEXT.md` in issue titles, specs, implementation plans, test names, and code review feedback. Avoid synonyms explicitly listed under `_Avoid_`.

If a needed concept is absent, reconsider whether existing vocabulary already covers it. If it represents a genuine domain gap, record it through the domain-modeling workflow.

## ADRs

Explicitly flag any proposal that contradicts an accepted ADR rather than silently overriding it. Architectural decisions that supersede earlier decisions must identify the superseded ADR and preserve unaffected portions.

## Layout

```text
/
├── CONTEXT.md
└── docs/
    └── adr/
```
