---
orphan: true
myst:
  html_meta:
    description: "Proposal for a Ray Core concept page on single-controller and SPMD programming models, opened for maintainer input on scope and placement."
---

(ray-single-controller)=

# Single-controller and SPMD programming models

:::{warning}
This page is a proposal, not finished documentation. It's open for maintainer input on whether the Ray docs should cover this topic, and if so, what the page should claim. Sections marked **Needs input** are deliberately unwritten. Don't merge this page as it stands.
:::

## Why propose this page

The Ray docs describe tasks, actors, and objects thoroughly, and they describe how Ray schedules them. They don't describe the program shape those primitives let you build, or how that shape differs from the one most distributed training frameworks assume.

A search of `doc/source` for SPMD, single-controller, or multi-controller returns no prose coverage. Frameworks in the post-training and reinforcement learning space describe themselves in these terms, so users arrive at the Ray docs already carrying the vocabulary. Those users have to infer the connection between that vocabulary and the Ray API themselves.

The concrete gap: a user who knows their training stack runs a single-program-multiple-data model, and who has been told Ray enables a different arrangement, has nowhere in the Ray docs to confirm what that arrangement is or how to express it.

## What the page would cover

The intent is a short conceptual page, not a survey or a comparison against named third-party frameworks. It would cover four things:

- **The two arrangements, defined.** What it means for every worker to run the same program and coordinate through collective operations, versus a single coordinating process that drives several differently-configured groups of workers.
- **How you express the coordinating process in Ray.** The driver holds the handles, calls methods on them, and passes references between them. This maps onto documented behavior and would link to {doc}`./actors` and {doc}`./objects` rather than restate them.
- **Why the arrangement matters for heterogeneous workloads.** When roles need different runtimes, different resource shapes, or different degrees of parallelism, one uniform program per worker stops being a good fit.
- **Where the boundary sits.** A coordinating driver and same-program worker groups aren't mutually exclusive. A group of workers can run a uniform program internally while the driver coordinates across groups.

## Open questions for maintainers

**Needs input: does this belong in the Ray docs at all?** It's arguably framework education rather than Ray documentation. A reasonable position is that Ray should document its API and let framework authors explain the models they build on it. The counter-position is that Ray Core gets chosen because of this property, and the docs never say so.

**Needs input: if yes, where?** Candidates are a new page under `ray-core/` near {doc}`./key-concepts`, a section within an existing conceptual page, or somewhere in the Ray Train or RLlib documentation where the audience already has the context.

**Needs input: what can the page claim about fault tolerance?** The interesting property is the blast radius of a single worker failure under each arrangement. Any claim here needs to come from someone who knows Ray's actual failure semantics, and should be reconciled with {doc}`./fault-tolerance`. This page doesn't speculate.

**Needs input: should it name third-party frameworks?** Naming them makes the page concrete and matches how users arrive at the topic. It also dates the page and implies endorsement.

**Needs input: is there an authoritative internal description already?** If a design doc, talk, or blog post states Ray's position on this, the page should follow it rather than invent a framing.

## Deliberately unwritten

No code sample appears on this page. A sample would need to show a driver coordinating multiple worker groups, and writing one before the questions above are settled risks encoding a pattern the maintainers wouldn't endorse. Once the framing is agreed, the sample should be a tested `doc_code` snippet like the rest of the scheduling documentation.

No performance, scalability, or fault-tolerance numbers appear here either, and none should be added without a source.
