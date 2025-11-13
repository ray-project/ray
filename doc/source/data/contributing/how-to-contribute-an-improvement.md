# How to contribute an improvement

If you want your changes to be reviewed and merged quickly, following a few key 
practices makes a big difference. Clear, focused, and well-structured contributions help 
reviewers understand your intent and ensure your improvements land smoothly.

This guide outlines how to write, structure, and submit changes to Ray Data in a way 
that maximizes clarity and minimizes friction.

## Find something to work On

Most contributions begin with solving a problem you personally encounter, like fixing a 
bug or adding a missing feature.

If you're unsure where to start:
* Check the issue tracker for problems you understand or want to explore. Issues 
  confirmed by other contributors are good candidates.
* Look for labels like “good first issue” for smaller, approachable tasks.
* Join the Ray Slack and message @richardliaw.

## Get early feedback

If you’re adding a new public API or making a substantial refactor, 
**share your plan early**. Discussing changes before you invest a lot of work can save 
time and align your work with the project’s direction.

You can open a draft PR or post in Slack for early feedback. It won’t affect acceptance 
and often improves the final design.

## Write a clear pull request description

A good PR description explains the problem and why your change matters. It helps 
reviewers understand your intent, reduces back-and-forth questions, and increases the 
likelihood of a fast merge.

When writing a PR description, **focus on why the change exists** and what it achieves.

## Keep pull requests small

Review difficulty scales nonlinearly with PR size. Smaller PRs are easier to review, 
merge, and revert if needed.

For fast reviews, do the following:
* **Keep PRs under ~200 lines** of change when possible.
* **Split large PRs** into multiple incremental PRs.
* Avoid mixing refactors and new features in the same PR.

## Write simple, clear code

Ray Data code should be **simple to read, reason about, and extend** — not just fast or 
"clever." 

### Minimize complexity

Every line of code has a long-term maintenance cost. Add new code only when it clearly 
reduces overall complexity or improves clarity.

Avoid adding layers, abstractions, or feature-specific flags unless they simplify the 
system as a whole. For specialized behavior, prefer mixins or 
small extension classes instead of expanding core interfaces (for example, 
`PhysicalOperator`). Avoid adding feature-specific flags to general-purpose interfaces.

### Design deep modules

Good modules are **deep, not shallow** — they have small, powerful interfaces that hide 
meaningful complexity behind clear boundaries. 

Avoid “pass-through” layers that only forward calls. They increase surface area without 
reducing complexity.

Each module should have a **strong contract** and **clear separation of concerns**. 
Favor a few well-defined entry points over many narrow ones.

### Keep abstractions clean

Avoid overlapping concepts or abstractions. Each layer should have a clear purpose, and 
responsibilities shouldn’t bleed across boundaries. Don’t break abstraction barriers or 
rely on hidden internal behavior.

When adding new features, **refactor first** if it leads to a cleaner, simpler design. 
Improving structure before extending functionality pays off long-term.

### Write good tests

For tips on how to write good tests, see {ref}`How to write tests <how-to-write-tests>`.