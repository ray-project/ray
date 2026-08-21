---
paths:
  - "doc/source/**/*.md"
  - "doc/source/**/*.rst"
---
<!-- Prose style for Ray documentation. -->
<!-- Source of truth: doc/source/ray-contribute/writing-style.md — read it before writing or editing docs prose. -->
<!-- These bullets are the highest-frequency corrections; the full guide is broader. -->

- Read `doc/source/ray-contribute/writing-style.md` before writing or editing docs prose. It's the source of truth and supersedes prior style guidance.
- Active voice with a named actor. Present tense. Second person ("you"). Imperative for instructions.
- No first-person plural. Rewrite to remove "we" and "our"; use "The Ray project" only when a subject is unavoidable.
- Use contractions (don't, isn't, can't), except in warnings and error messages.
- "such as," not "like," for examples. "ID," not "id," in prose. Plain words: "use" (not utilize/leverage), "through" (not via), "before" (not prior to).
- Cut filler: simply, just, basically, actually, really, very. Cut time-relative words: currently, now, recently, new.
- Sentence-case headings; imperative for tasks, questions for concepts.
- Short sentences. No dashes, parentheticals, or semicolons in prose. A colon only introduces a list or code block.
- List items that are sentences or contain verbs get periods. Lead-ins are complete sentences ending in a colon.
- MyST admonitions (`:::{note}`, `:::{warning}`), not bold inline labels. Tag code blocks with a language; no `$` prompt.
- Cross-references: `{doc}` for whole pages (required when the target source is `.rst`), `{ref}` for labeled targets, `[text](page.md)` between `.md` pages.
- Never fabricate technical details. Every technical claim needs a verifiable source: the docs, the source code, or a config file. When you can't verify a detail, leave it out.
