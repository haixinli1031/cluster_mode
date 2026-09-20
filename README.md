# Distributed Mode Explorer

An interactive, dependency-free web app for exploring how `k` workers compute
the **mode** (most frequent integer, smallest on a tie) of a dataset that is
sharded across them — without any worker ever seeing another worker's raw data.

The algorithm is a hash-partitioned frequency shuffle:

1. **Partition** — the dataset is split evenly across `k` workers.
2. **Local count** — each worker counts its own slice into `{value: count}`.
3. **Scatter by hash** — every `(value, count)` pair is sent to the value's
   *owner*, `owner = value mod k`, as `FREQ value count`. Each worker then
   broadcasts `SCATTER_END` to all `k` workers.
4. **Aggregate** — each owner reads its mailbox until it has seen `k`
   `SCATTER_END`s, summing counts. Because every occurrence of a value lands on
   the same owner, the owner now holds that value's exact global count. It
   picks its local mode (smallest value on a tie).
5. **Report** — workers `1..k-1` send `MODE value count` and `MODE_END` to
   worker 0.
6. **Result** — worker 0 waits for `k-1` `MODE_END`s and keeps the highest
   count / smallest value. That is the global mode.

## Run it

No build step, no server-side code.

- **Open the file:** double-click `index.html`.
- **Local server:** `python3 -m http.server 8000` then open
  <http://localhost:8000>.
- **GitHub Pages:** `.github/workflows/pages.yml` deploys on every push to
  `main`. One-time setup: repo *Settings → Pages → Source: "GitHub Actions"*.

## Use it

- **Workers** slider: 1–10.
- **Dataset** box: up to 50 integers (negatives allowed), separated by commas
  or spaces. The run updates as you type.
- **Generate**: *Random* (default, 20 values in 1–9), *Skewed* (every value is
  a multiple of `k`, so worker 0 owns all of them — watch the shuffle-balance
  bars), *Ties* (several values share the top count — watch the tie-breaks).
- Step through the six phases with *Prev / Next*, the phase pills, or the
  arrow keys. *Run all* jumps to the result.
- **Mailboxes** shows each worker's raw payload strings and how far it has
  read at the end of the current phase.
- **Network** compares this run's traffic to the naive "ship every slice to
  worker 0" approach. At ≤ 50 values the per-message overhead dominates; the
  point is that shuffle traffic scales with the number of *distinct* values,
  not with `n`, and raw data never leaves its worker.

## Files

| File | Purpose |
|---|---|
| `cluster_mode.py` | Reference implementation (`Worker` / `Cluster`). `python3 cluster_mode.py <k> <nums…>` runs it from the CLI. |
| `cluster.js` | Line-for-line port used by the browser, instrumented to record a message trace. Loads in the browser and in Node. |
| `app.js`, `index.html`, `styles.css` | The explorer UI. |
| `test_cluster_mode.py` | `python3 test_cluster_mode.py` — fixed cases + 3,000 randomized cases vs. a brute force. |
| `cluster.test.js` | `node cluster.test.js` — the same checks for the JS port, plus trace sanity checks. |
| `.github/workflows/pages.yml` | Runs both test files, then deploys to GitHub Pages. |

## Notes on the protocol

- Payloads are plain strings: `FREQ <value> <count>`, `SCATTER_END`,
  `MODE <value> <count>`, `MODE_END`. The two end markers are distinct so each
  phase can only be terminated by its own signal.
- `Cluster.findMode` runs the phases strictly in order (all scatters, then all
  aggregates, …), so every message a phase waits for is already in the mailbox
  when it starts. The wait loops treat an empty `receive()` (`""`) as
  "nothing yet".
- `value mod k` uses Python-style modulo in both implementations so negative
  values still map into `[0, k)`.
- `value mod k` is a deliberately simple partitioner; the *Skewed* preset shows
  how badly it can balance. A real deployment would use a mixing hash.
