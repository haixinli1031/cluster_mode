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

// Trace sanity: every message sent is eventually received, in phase order.
{
  const c = new Cluster([3, 1, 3, 2, 1, 3, 2, 2], 3);
  c.findMode();
  const sends = c.trace.filter(e => e.type === 'send').length;
  const recvs = c.trace.filter(e => e.type === 'recv' && e.payload !== '').length;
  assert.strictEqual(sends, recvs, 'every sent message is read exactly once');
  assert.deepStrictEqual(c.trace.filter(e => e.type === 'phase').map(e => e.phase),
    ['scatter', 'aggregate', 'report', 'result', 'done']);
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
