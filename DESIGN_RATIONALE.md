# Design Rationale — Distributed Mode Explorer

**Theme 1: Exploration & Understanding** · Live demo: https://haixinli1031.github.io/cluster_mode/ · Code: https://github.com/haixinli1031/cluster_mode · **Time spent: ~2 hours** *(one session)*

An interactive explorer for a distributed algorithm: `k` workers compute the mode of a sharded integer dataset by hash-partitioned frequency shuffle, with no worker ever seeing another's raw data. You set the workers and the data, then step through all six phases and watch every message move.

## Why this theme, and why this approach

Distributed aggregation is the archetypal artifact that static explanation fails. The code is ~150 lines, the output is a single integer, and everything that makes it correct — who owns which key, which message ends which phase, why a worker knows it can stop waiting — lives in the *ordering of messages over time*, which appears in neither the source nor the result. Diagrams flatten the time dimension; prose asks you to simulate the cluster in your head.

So rather than explain a system, I made the system itself drivable. The starting artifact was real: my own Python solution to a production-shaped interview problem. The tool exposes its two levers (`k`, the dataset), replays the actual execution phase by phase, and lets you inspect each worker's mailbox as literal payload strings with a per-phase read cursor.

It fits the self-contained requirement structurally, not by bolting on a demo mode: there is no data to source (three generators, one click each), no backend, no build step, no install. It is a static page on GitHub Pages, and CI runs both test suites before every deploy.

## What makes it non-obvious

**The visualization is a trace, not a drawing.** `cluster.js` is a line-for-line port of `cluster_mode.py` with a trace hook; the UI replays the real message log. Both implementations run the same fixed corner cases and 3,000 randomized cases against brute force, in CI. Most explainers animate a cartoon of an algorithm and can quietly diverge from it. This one is wired so it cannot.

**The presets are adversarial on purpose.** *Skewed* emits only multiples of `k`, so `value mod k` routes every key to worker 0 and the balance bars collapse. The tool's most instructive view is the one where its own partitioner looks worst.

**The comparison panel refuses the flattering baseline.** The obvious foil — "ship all raw data to W0" — would make the shuffle look brilliant, and it is deliberately absent, because it violates the problem's constraint and therefore isn't a real alternative. Against the only legitimate competitor (every worker sends its full count table to W0), the shuffle sends *more* messages and roughly the same bytes. Its actual win is that the busiest worker's merge work and memory shrink by a factor of `k`. The panel states the loss as plainly as the gain — and because at n ≤ 1,000 none of this is visible, the *At scale* sliders project the run's own value mix out to n = 10⁹ and k = 1,000, with the modelling assumptions printed on screen and verified to reproduce the live run exactly.

## Key decisions and trade-offs

- **Static site, two implementations in lockstep.** No backend means "open the file" is the whole deployment; the price is reimplementing the algorithm in JS. Paid down with a line-for-line port plus a shared test corpus, so the browser cannot silently drift from the reference.
- **Batched scatter with distinct phase markers.** One `FREQ` message per worker→owner pair — *including empty batches* — makes "k batches received" a sound termination condition and removes a separate broadcast. `MODE_END` is distinct from `FREQ` so each phase can only be ended by its own signal. This corrected the original solution, which reused one `"END"` tag and leaned on strict scheduling for correctness.
- **Explicit Run instead of live updates.** Live recompute swapped the visualization under you mid-walkthrough. Now edits mark the page stale, dim it, and lock navigation until you press Run. Slightly more friction, bought in exchange for never showing a screen that mixes two runs.
- **`value mod k` kept as the partitioner.** A mixing hash would be better production code and a worse teaching artifact; its failure mode *is* the lesson, so the weakness stays and *Skewed* points at it.
- **Adaptive detail over virtualization.** Panels collapse behind "+N more" past readable limits, keeping a 1,000-value run to a few hundred DOM nodes. Going far beyond that needs virtualized lists — a different tier of work, declined on purpose.
- **Scoped out: real asynchrony.** Phases run strictly in order, so every message a phase waits on is already in the mailbox. That is the honest simplification — the tool teaches the protocol's *structure*, not its behaviour under adversarial scheduling.

## With more time

1. **Adversarial scheduling** — interleave, delay, drop, duplicate, and crash-restart workers. The termination conditions are exactly where this class of system breaks, the UI already surfaces per-mailbox read cursors, and it would turn the explainer into a falsification tool.
2. **Pluggable partitioners** — `mod k` vs. multiplicative vs. consistent hashing, side by side against the balance bars, plus skew mitigations (salting hot keys, a local combine pass).
3. **Protocol diff mode** — two algorithms on one dataset with a synced timeline, so the network trade-off is watched rather than tabulated.
4. **Approximate mode** — Count-Min / heavy hitters, showing the memory-vs-accuracy curve against the exact answer.
5. **Measured, not projected, scale** — run the workers as real Web Workers to replace the At-scale model with observation.
