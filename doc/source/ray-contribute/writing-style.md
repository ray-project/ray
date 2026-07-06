---
myst:
  html_meta:
    description: "The Ray documentation style and grammar guide: voice, word choice, sentence structure, headings, lists, links, admonitions, and MyST formatting conventions. Read this before writing or editing Ray documentation, whether you're a human contributor or an AI coding agent."
---

(documentation-style)=

# Ray documentation style guide

This page is the style and grammar standard for the Ray documentation. It covers how to write Ray docs: voice, word choice, sentence structure, headings, lists, links, admonitions, and MyST formatting. Read it before you write or edit a page, then apply it as you go.

The guidance here is for everyone who writes Ray docs, including AI coding agents. When an agent edits documentation under `doc/source/`, it follows this guide. See {ref}`agent-development` for how the repository configures AI coding agents.

## How to use this guide

When two rules conflict, follow this order:

1. This guide.
1. The [Google developer documentation style guide](https://developers.google.com/style), as the general fallback for anything this guide doesn't cover.

Vale enforces an automated subset of the Google style guide in CI, currently on the Ray Data docs and the example gallery. Passing Vale is the baseline, not the whole standard. This guide is broader than what Vale checks, so a page can pass Vale and still need edits to meet the standard here. For how to run Vale, see {ref}`vale`.

New pages are MyST Markdown (`.md`). A lint check rejects newly added reStructuredText (`.rst`) files, though edits to existing `.rst` files are fine. The examples in this guide use MyST. The prose rules apply to both formats. Only the formatting and cross-reference syntax differs.

## Voice and grammar

### Write in active voice

Name the actor and put it in front of the verb. Passive voice hides who does what.

- Use: "The scheduler retries failed tasks."
- Not: "Failed tasks are retried by the scheduler."

Keep passive voice when the object is the real focus, when the actor is unknown, or when naming the actor adds nothing. "The object is spilled to disk" is fine when the point is what happens to the object, not what spills it.

### Write in present tense

Describe current behavior in the present tense. Avoid "will" for things that are always true.

- Use: "Ray Serve routes the request to a replica."
- Not: "Ray Serve will route the request to a replica."

### Address the reader as "you"

Write in second person. Don't write about "the user" or "developers" in the abstract.

- Use: "You can configure the number of replicas."
- Not: "Users can configure the number of replicas."

### Use the imperative for instructions

Write steps and commands as direct instructions.

- Use: "Set `num_cpus` to reserve cores for the task."
- Not: "You should set `num_cpus` to reserve cores for the task."

### Avoid first-person plural

Don't write "we," "our," or "let's." Rewrite the sentence to remove the first person. Address the reader with "you" or the imperative, or make the feature the subject. When a sentence genuinely needs a subject that acts on behalf of the project, use "The Ray project" or the relevant component.

- Use: "Ray tests every code snippet in the documentation." Not: "We test every code snippet."
- Use: "To build the docs, follow these steps:" Not: "Let's build the docs."
- Use: "The Ray project welcomes contributions of all kinds." Not: "We welcome all contributions."

### Capitalize Ray components and product names

Capitalize Ray and its libraries: Ray, Ray Core, Ray Data, Ray Serve, Ray Train, Ray Tune, RLlib. Capitalize other proper nouns: Python, Sphinx, AWS, GCP, Kubernetes. Lowercase generic nouns even when they name a Ray concept: task, actor, object, cluster, worker, node, placement group.

### Define acronyms on first use

Spell out the full term the first time, with the acronym in parentheses: "reinforcement learning (RL)." Use the acronym consistently afterward. Don't redefine it later on the same page.

### Keep comparisons precise

State what's better and by what measure. Vague comparisons read as marketing.

- Use: "Streaming execution uses less memory than materializing the full dataset."
- Not: "Streaming execution is better."

### Keep parallel structure

Match the grammatical form of items in a series and avoid redundant conjunctions.

- Use: "This preserves throughput while reducing memory usage."
- Not: "This preserves throughput and can also reduce memory usage."

## Word choice

### Use contractions

Contractions read naturally: don't, doesn't, can't, won't, it's, you're, isn't, aren't, wouldn't, shouldn't. Avoid contractions in a warning or an error message, where the extra weight of the full form helps.

Never use "would've," "could've," "should've," "ain't," or "y'all."

### Use "such as," not "like," for examples

Reserve "like" for genuine comparisons.

- Use: "distributed frameworks such as Ray"
- Not: "distributed frameworks like Ray"
- Fine: "The API behaves like a Python dictionary." (a real comparison)

### Cut qualifiers and filler

Delete words that add no information: simply, just, basically, actually, really, very, quite, easily, of course, obviously, clearly, note that. Also drop the sentiment adverbs "luckily," "fortunately," and "unfortunately."

- Use: "Call `ray.get` to retrieve the result."
- Not: "You can simply just call `ray.get` to retrieve the result."

### Prefer plain words

Choose the simpler word.

- "utilize" or "leverage" → "use"
- "in order to" → "to"
- "prior to" → "before"
- "due to the fact that" → "because"
- "via" → "through" or "with"

### Turn hedging into direct advice

Recommend directly. Don't soften real guidance into a suggestion.

- "Prefer X" → "Use X"
- "Consider using X" → "Use X"
- "You might want to enable caching" → "Enable caching."

Keep "might," "can," or "may" when they express genuine possibility, as in "The build might fail if the network is unstable."

### Focus on what the reader does

Avoid "lets," "allows," and "enables." They describe what the product permits instead of what the reader accomplishes.

- Use: "Scale a deployment by adding replicas."
- Not: "Ray Serve lets you scale a deployment."

### Streamline references

- "Please refer to" → "See"
- "Refer to the configuration guide" → "See the configuration guide"
- "Check the API documentation" → "See the API documentation"

### Write "ID," not "id"

In prose, write "ID" (or "IDs"). Reserve `id` for code, where it's a literal identifier.

- Use: "Ray assigns each task a task ID."
- Not: "Ray assigns each task a task id."

### Use words for symbols in prose

Outside code, spell out operators and separators.

- "X + Y" → "X and Y"
- "X vs. Y" → "X versus Y"
- "X/Y" → "X or Y" or "X and Y"

### Don't write "etc." after "such as," "for example," or "including"

Those phrases already signal a partial list. Give specific examples, or end with "and more" if you must.

### Avoid time-relative words

Words such as "currently," "recently," "new," "now," and "at this time" go stale as Ray evolves. Remove them, or replace them with a specific version when the timing matters.

- Use: "Ray Serve supports synchronous handlers." Or: "As of Ray 2.9, Ray Serve supports asynchronous handlers."
- Not: "Ray Serve currently supports synchronous handlers."

### Use angle brackets for placeholders

Mark values the reader replaces with angle brackets, not uppercase.

- Use: `ray start --address=<head-node-ip>:6379`
- Not: `ray start --address=HEAD_NODE_IP:6379`

Uppercase placeholders are ambiguous with real constants and environment variables.

## Sentence structure

Prefer short, direct sentences. Split a long sentence into two rather than joining clauses with punctuation.

Avoid dashes (`--`, em dashes, en dashes), parentheticals, and semicolons in prose. Restructure instead. Use a colon only to introduce a list or a code block.

- Use: "Ray splits the nodes into two groups: spot and on-demand. Ray ranks on-demand above spot."
- Not: "Ray splits the nodes into two groups -- spot and on-demand -- and ranks them (on-demand first)."

## Headings

Use sentence case for every heading. Capitalize only the first word and proper nouns.

- Use: "Configure the runtime environment"
- Not: "Configure the Runtime Environment"

Write conceptual headings as questions and task headings as imperatives. Keep them short.

- Conceptual: "What is a placement group?"
- Task: "Launch a cluster on AWS"
- Too long: "Understanding the fundamentals of placement groups" → "Placement group basics"

Don't stack heading levels without prose between them. A section heading that introduces subsections needs a lead-in paragraph first. Avoid nesting beyond the fourth level; reaching that depth usually means the page should be split.

## Lists and punctuation

Use a list to enumerate things, map topics to pages, or present a scannable reference. Don't use a list to explain how something works or to sell benefits. Fold that into prose.

End a list item with a period when it's a complete sentence or contains a verb. Leave brief noun phrases unpunctuated. Be consistent within a single list.

Write list lead-ins as complete sentences, and end them with a colon.

- Use: "To invite users, do the following:"
- Not: "To invite users:"

Use the Oxford comma: "tasks, actors, and objects."

When you name a fixed set, state the count: "Ray offers two scheduling strategies:" rather than "Ray offers several scheduling strategies:".

In Markdown, number every ordered-list item `1.`. MyST renders them in source order, so writing `1.` throughout means inserting or reordering a step is a one-line edit instead of a renumber pass.

## Formatting

### Admonitions

Use MyST admonitions for asides, not bold inline labels. Ray uses `note`, `tip`, `warning`, `caution`, and `important`:

````markdown
:::{note}
Ray caches the object in the local object store.
:::
````

Choose the level by severity. Use `tip` for optional advice, `note` for supporting information, `caution` for something that needs care, and `warning` for an action that can lose data or break a workload. Convert a standalone caveat or limitation into the matching admonition so it stands out instead of hiding in a paragraph.

### Code blocks

Tag every fenced code block with a language: `python`, `bash`, `yaml`, `json`, `text`, or `dockerfile`. Don't prefix shell commands with `$` or `#`, so readers can copy and paste them.

Every runnable snippet in the docs is tested. Write examples that run out of the box, and use `literalinclude` or a testable code cell rather than pasting untested code. See {ref}`How to write code snippets <writing-code-snippets_ref>`.

In code comments, start with a capital letter and use the imperative.

- Use: `# Reserve two GPUs for the actor.`
- Not: `# this reserves two gpus`

### Bold and italics

Italicize a term when you first define it. Reserve bold for three uses: the name of a UI element (a button, menu, tab, or field), the short lead-in label of an admonition, and the term in a bold definition list (`**Term**: explanation`).

Don't bold ordinary prose for emphasis, and don't bold inline code. Code styling already sets it apart. Pervasive bold reads as shouting.

- Use: "supports capacity reservations, spot instances, and on-demand instances"
- Not: "supports **capacity reservations**, **spot instances**, and **on-demand instances**"

### Tables

Use a Markdown table for simple rows with single-line cells. Always include a header row, and introduce the table with a sentence of prose. For cells that hold code blocks, multiple lines, or other complex content, use the `list-table` directive:

````markdown
:::{list-table} Scheduling strategies
:header-rows: 1

* - Strategy
  - Behavior
* - `DEFAULT`
  - Packs tasks onto the fewest nodes.
* - `SPREAD`
  - Spreads tasks across available nodes.
:::
````

## Links and cross-references

Ray docs use Sphinx cross-references, which resolve at build time and survive page moves. Prefer them over hardcoded URLs to other docs pages.

- Link to a whole page with the `{doc}` role: `` {doc}`writing-code-snippets` ``. Use `{doc}` when the target's source is `.rst`. A bare `[text](path.md)` link to an `.rst` source emits `myst.xref_missing`, which fails the build.
- Link to a labeled target with the `{ref}` role: `` {ref}`vale` `` or `` {ref}`custom text <vale>` ``. Define the target with a MyST label on the line above the heading:

  ```markdown
  (my-label)=
  ## The section to link
  ```

- Link between `.md` pages with a standard Markdown link: `[text](other-page.md)`.
- Link to the API reference with autodoc cross-reference roles, such as `` {py:func}`ray.init` `` or `` {py:class}`ray.data.Dataset` ``.

Write descriptive link text that says where the link goes. Don't write `click [here](path).` Wrap external URLs in Markdown; don't paste a bare URL into prose.

Link to a given page once per section. Repeating a link in a later section is fine when readers might enter the page at different points. For a cluster of related links, gather them under a "See also" list at the end of the section.

## MyST and Markdown specifics

### Soft-wrap prose

Write one logical line per paragraph and list item, and let your editor wrap the display. Don't hard-wrap prose at a fixed column. One paragraph is one source line, however long. Hard wrapping makes diffs noisy and edits awkward.

### Front matter

Give every page a `description` in its MyST `html_meta` front matter. Search engines and the page's social preview use it:

```markdown
---
myst:
  html_meta:
    description: "One or two sentences describing what the page covers."
---
```

### Numbers and units

Spell out zero through nine in prose. Use numerals for 10 and above. Don't wrap a single-digit number in backticks when it carries a unit: write "1 GiB," not "`1` GiB." Use backticks for configuration values, as in `num_replicas: 1`.

### Alt text

Give every image descriptive alt text that says what the image shows. Don't write "screenshot of..." or "image of...".

### HTML comments

Preserve existing HTML comments that read as author notes. They're working notes contributors leave for each other across revisions. Don't remove or reword them as part of an unrelated edit.

## Write for humans

Documentation should read as though a person wrote it, because a person should have.

- Vary your sentence and list structure. Don't produce perfectly symmetrical bullet lists where every item has the same shape and length.
- Explain how something works in prose, not a numbered list. Reserve numbered lists for sequential steps.
- Cut benefits lists. If a feature is worth recommending, recommend it directly. Don't follow "This approach provides:" with a list of adjectives.
- Skip opening previews. Don't open a section with a bullet list that restates the headings below it. Start with substance.
- Open a page with one or two sentences that say what it covers, then get into the content.

## Don't fabricate technical details

Every technical claim needs a verifiable source: existing Ray documentation, the source code, a configuration file, or something the contributor gives you. Don't invent behavior from what seems plausible, extrapolate implementation details from a feature name, or guess at metrics, defaults, or version cutoffs.

When you can't verify a detail, leave it out. Incomplete but accurate documentation beats complete but partly fabricated documentation. This matters most for AI agents, which can produce fluent, confident prose that's wrong. Fabrication is the most damaging error in documentation.

## Choosing a format

Pick the format that makes the information easiest to extract:

- Enumerating distinct things, such as options or components, is a list.
- Explaining what something is or how it works is prose.
- Mapping topics to pages, or comparing options side by side, is a table.
- Listing benefits is usually a cut. Fold the one point that matters into prose.
