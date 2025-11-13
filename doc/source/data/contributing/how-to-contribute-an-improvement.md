# How to contribute an improvement

If you want your changes to be reviewed and merged quickly, following a few key 
practices makes a big difference. Clear, focused, and well-structured contributions help 
reviewers understand your intent and ensure your improvements land smoothly.

## Find something to work On

Start by solving a problem you encounter, like fixing a bug or adding a missing feature. 
If you're unsure where to start:
* Browse the issue tracker for problems you understand.
* Look for labels like "good first issue" for approachable tasks.
* Join the Ray Slack and message @richardliaw.

## Get early feedback

If you’re adding a new public API or making a substantial refactor, 
**share your plan early**. Discussing changes before you invest a lot of work can save 
time and align your work with the project’s direction.

You can open a draft PR, discuss on an Issue, or post in Slack for early feedback. It 
won’t affect acceptance and often improves the final design.

## Write a clear pull request description

Explain **why the change exists and what it achieves**. Clear descriptions reduce 
back-and-forth and speed up reviews.

## Keep pull requests small

Review difficulty scales nonlinearly with PR size.

For fast reviews, do the following:
* **Keep PRs under ~200 lines** of change when possible.
* **Split large PRs** into multiple incremental PRs.
* Avoid mixing refactors and new features in the same PR.

## Write simple, clear code

Ray Data values **readable, maintainable, and extendable** code over clever tricks.

### Minimize complexity

Add new code only when it clearly reduces overall complexity or improves clarity.

* Avoid adding layers, abstractions, or feature-specific flags unless they simplify the 
  system as a whole. 
* Use mixins or small extension classes for specialized behavior instead of bloating 
  core interfaces like `PhysicalOperator`.

### Design deep modules

Good modules are **deep, not shallow**.

* Prefer small, powerful interfaces.
* Avoid "pass-through" layers.
* Aim for clear separation of concerns.

### Keep abstractions clean

* Avoid overlapping concepts or abstractions. 
* Don't break abstraction barriers or rely on hidden behavior.
* Refactor first if it simplifies design before adding new features.

### Write good tests

For tips on how to write good tests, see {ref}`How to write tests <how-to-write-tests>`.