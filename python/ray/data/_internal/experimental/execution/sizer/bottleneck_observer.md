# Bottleneck observer: slot accounting

How the observer turns an operator's `SlotUsage` (what the op reports at a
sample) into an `_OpSample` (one interval's slot time, split into disjoint
parts). See `bottleneck_observer.py` for the code and
`actor_pool_map_operator.py` for `SlotUsage`.

## Notation

One interval runs from `t0` to the sample at `t1`; `dt = t1 - t0`.

- `S(t)`: serving (IDLE or ACTIVE) actors at time `t`. Pending, DRAINING and
  RESTARTING actors don't serve.
- `O(t) ⊆ S(t)`: the output-backpressured ones (pull budget exhausted).
- `c`: slots per actor (`slots_per_actor()`, `max_concurrency` and at least 1).
- `h_a(t)`: actor `a`'s held slots, `min(c, tasks in flight)`; 0 when IDLE.
- `|S|`: the number of actors in `S`.
- `Σ_{a ∈ S}`: the sum over the actors in `S`.

## `SlotUsage`: the op's reading at `t1`

A field without a prefix covers all serving actors; its `obp_` counterpart
covers the output-backpressured subset.

| Field | Math | Kind |
|---|---|---|
| `serving_actors` | `\|S(t1)\|` | count at `t1` |
| `slots` | `c · \|S(t1)\|` | count at `t1` |
| `busy_slots` | `Σ_{a ∈ S(t1)} h_a(t1)` | count at `t1` |
| `obp_slots` | `c · \|O(t1)\|` | count at `t1` |
| `obp_busy_slots` | `Σ_{a ∈ O(t1)} h_a(t1)` | count at `t1` |
| `non_obp_busy_slots` (property) | `busy_slots - obp_busy_slots` | count at `t1` |
| `idle_slots` (property) | `slots - obp_slots - non_obp_busy_slots` | count at `t1` |
| `slot_seconds` | `∫ c · \|S(t)\| dt`, from pool start to `t1` | running total |
| `busy_slot_seconds` | `∫ Σ_{a ∈ S(t)} h_a(t) dt`, from pool start to `t1` | running total |

At any moment the slots split into three disjoint groups:

```
slots = obp_slots + non_obp_busy_slots + idle_slots
```

That is: slots on output-backpressured actors, slots holding a task on the
other actors, and free slots on the other actors.

The two running totals are kept by the pool, which advances them on every
task and status change, so tasks shorter than a sizer tick and actors that
join or leave between samples still count. Output backpressure changes on
output pulls the pool does not see, so the `obp_*` fields are sampled.

## `_OpSample`: one interval

Built in `BottleneckObserver._record_op` and `OpShareWindow.record`. Each field
integrates one of the slot groups above over `[t0, t1]`:

| Field | Integrates over `[t0, t1]` | How it is measured |
|---|---|---|
| `slot_s` | `slots` | Exact: `Δ slot_seconds`. |
| `obp_s` | `obp_slots` | Value at `t1` held for `dt`, capped at `slot_s`. |
| `non_obp_busy_s` | `non_obp_busy_slots` | `Δ busy_slot_seconds - obp_busy_slots · dt`, capped at `slot_s - obp_s`. |
| `starved_s` | `idle_slots`, if no bundle is ready at `t1` | Remainder `slot_s - obp_s - non_obp_busy_s`; 0 if a bundle is ready. |
| (not stored) | `idle_slots`, if a bundle is ready at `t1` | The same remainder: the dispatch gap. |
| `unstaffed_s` | Nothing; a rule | `dt` when no actor is serving at `t1`, input is queued and `slot_s = 0`. Also added to `slot_s` and `non_obp_busy_s`. |

Integrating the slot split gives the sample's split:

```
slot_s = obp_s + non_obp_busy_s + (starved_s or the dispatch gap)
```

Whether the idle remainder is starved or a dispatch gap is decided once, at
`t1`: starved when the op has no bundle ready to dispatch (its queue is empty
or holds only a partial bundle), so it waits on its producer; a dispatch gap
when a bundle is ready, so more input would only lengthen the queue.

### Notes

- **Unqualified `busy` covers all serving actors**, as in
  `SlotUsage.busy_slots` and `busy_slot_seconds`. `_OpSample.non_obp_busy_s`
  integrates `non_obp_busy_slots`: an output-backpressured actor's held slots
  count in `obp_s` instead, since the breakdown needs disjoint parts. The one
  exception is the public share `OpShareBreakdown.busy`, which is
  `Σ non_obp_busy_s / Σ slot_s`: busy on actors not output-backpressured.
- **Only `slot_s` and the busy integral are exact.** Anything involving output
  backpressure holds its `t1` value for the whole interval.
- **Unstaffed** means an op with queued input and no serving actor at `t1`.
  With no slot time it would drop out of the snapshot, and its producer,
  output-backpressured on it, would be named instead. Pending actors don't
  count as serving here, unlike the pool's `serving_size()`.

## `OpShareBreakdown`: an op's shares over the window

`OpShareWindow` keeps the op's recent `_OpSample`s and drops the oldest once
the rest still span `window_s` seconds. `shares()` sums each field over the
kept samples and divides by their total slot time:

| Share | Math |
|---|---|
| `busy` | `Σ non_obp_busy_s / Σ slot_s` (includes `unstaffed_s`) |
| `obp` | `Σ obp_s / Σ slot_s` |
| `starved` | `Σ starved_s / Σ slot_s` |
| `unstaffed` | `Σ unstaffed_s / Σ slot_s` (part of `busy`, not on top of it) |
| `actors` | `serving_actors` at the latest sample, not a share |

So `busy + obp + starved <= 1`; the rest is the window's dispatch gap.
Summing slot-seconds before dividing weights each interval by its capacity:
an interval with 8 slots counts four times one with 2, which averaging
per-interval fractions would not.

`shares()` returns `None` until the samples span a full `window_s`, or if the
window had no slot time (no serving actor and no queued input throughout).
An op without shares drops out of the snapshot.

## Edges: `_EdgeSample` and `EdgeShareBreakdown`

For each edge `producer -> consumer` between actor-pool ops (the consumer not
completed), `observe` splits the producer's output-backpressured time by what
the consumer was doing at `t1`:

```
saturated(consumer) = idle_slots <= 0     if the consumer has slots
                    = queued > 0          otherwise (unstaffed)
```

| `_EdgeSample` field | Math |
|---|---|
| `slot_s` | the producer's `slot_s` |
| `blocked_s` | the producer's `obp_s` if the consumer is saturated, else 0 |
| `self_limited_s` | the producer's `obp_s` if the consumer is not saturated, else 0 |

So `blocked_s + self_limited_s = obp_s` (the producer's), and each interval's
output-backpressured time goes entirely to one side. Blocked means the
consumer had no room, so it holds the producer back. Self-limited means the
consumer had room, so the producer's own output budget (blocks still in
flight, counted against it until consumed) held it back.

`EdgeShareWindow` keeps and evicts samples the same way as `OpShareWindow`,
and `shares()` gives:

| `EdgeShareBreakdown` field | Math |
|---|---|
| `producer_blocked` | `Σ blocked_s / Σ slot_s` |
| `producer_self_limited` | `Σ self_limited_s / Σ slot_s` |

Both are fractions of the producer's slot time. Their sum is the producer's
output-backpressured fraction over the edge's own samples, close to but not
always equal to `OpShareBreakdown.obp`: the edge window starts once the
consumer is active, and its `slot_s` has no unstaffed credit. Without a full window of edge
samples, `get_edge_share_breakdown_or_default` falls back to
`producer_blocked = obp` (the producer's) and `producer_self_limited = 0`.

`classify_edge_direction` then points the edge's arrow:

```
diff = producer_blocked - (consumer_starved + producer_self_limited)
diff >  margin  ->  downstream (the consumer limits the edge)
diff < -margin  ->  upstream   (the producer limits the edge)
otherwise       ->  tie
```
