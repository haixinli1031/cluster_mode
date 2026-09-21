// Randomized check of cluster.js against a brute-force mode, mirroring
// test_cluster_mode.py so the JS port cannot drift from the Python reference.
//
// Run:  node cluster.test.js
'use strict';
const assert = require('assert');
const { Cluster, bruteMode } = require('./cluster.js');

function check(data, k) {
  const got = new Cluster(data.slice(), k).findMode();
  const exp = bruteMode(data);
  assert.strictEqual(got, exp, `k=${k} data=[${data}] got=${got} expected=${exp}`);
}

// Fixed cases (same as the Python test).
check([3, 1, 3, 2, 1, 3, 2, 2], 3);
check([5, 5, 3], 7);
check([-4, -4, -9, -9, -1], 3);
check([1, 2, 3, 4], 1);
check(Array.from({ length: 49 }, (_, i) => (i + 1) * 4).concat(Array.from({ length: 49 }, (_, i) => (i + 1) * 4), [8]), 4);
check([7], 10);

// Hand-designed corner cases (mirrors test_corner_cases in the Python test);
// expected values are fixed, not brute-forced.
function expect(data, k, want) {
  const got = new Cluster(data.slice(), k).findMode();
  assert.strictEqual(got, want, `k=${k} data=[${data}] got=${got} expected=${want}`);
}
const rep = (v, n) => Array(n).fill(v);
expect([1, 2, 2, 3, 3, 3, 4, 4, 4, 4], 3, 4);                          // 1. plain
expect([1, 2, 3, 1, 2, 3], 2, 1);                                      // 2. three-way tie -> smallest
expect([].concat(rep(7, 100), rep(3, 50), rep(11, 30), rep(5, 20)), 10, 7); // 3. larger dataset, k=10
expect([5, -5, 0, 5, -5, 0, 5, -5], 2, -5);                            // 4. negatives & tie-breaker
expect([8, 8, 8, 8, 8], 5, 8);                                         // 5. empty workers (4 owners get no FREQ)
expect([1, 9, 2, 9, 3, 9, 4, 9, 1, 2, 3, 4], 4, 9);                    // 6. fragmented global mode
expect([42], 10, 42);                                                  // 7. high k, low data

// Trace sanity: every message sent is eventually received, in phase order.
{
  const c = new Cluster([3, 1, 3, 2, 1, 3, 2, 2], 3);
  c.findMode();
  const sends = c.trace.filter(e => e.type === 'send').length;
  const recvs = c.trace.filter(e => e.type === 'recv' && e.payload !== '').length;
  assert.strictEqual(sends, recvs, 'every sent message is read exactly once');
  assert.deepStrictEqual(c.trace.filter(e => e.type === 'phase').map(e => e.phase),
    ['scatter', 'aggregate', 'report', 'result', 'done']);
  // Batched scatter: every worker receives exactly k FREQ messages, one from each worker.
  for (let w = 0; w < 3; w++) {
    const freq = c.trace.filter(e => e.type === 'send' && e.to === w && e.payload.startsWith('FREQ'));
    assert.strictEqual(freq.length, 3, `W${w} should receive k FREQ batches`);
    assert.deepStrictEqual(freq.map(e => e.from).sort(), [0, 1, 2]);
  }
}

// Deterministic PRNG so failures are reproducible.
let seed = 7;
function rand() { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; }
function randint(lo, hi) { return lo + Math.floor(rand() * (hi - lo + 1)); }

for (let t = 0; t < 3000; t++) {
  const n = randint(1, 50);
  const k = randint(1, 10);
  const data = Array.from({ length: n }, () => randint(-15, 15));
  check(data, k);
}
console.log('ok');
