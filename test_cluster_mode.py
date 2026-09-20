"""Randomized check of cluster_mode.Cluster against a brute-force mode.

Run:  python3 test_cluster_mode.py
"""
import random
from collections import Counter

from cluster_mode import Cluster


def brute_mode(data):
    counts = Counter(data)
    best = max(counts.values())
    return min(n for n, c in counts.items() if c == best)


def check(data, k):
    got = Cluster(list(data), k).findMode()
    exp = brute_mode(data)
    assert got == exp, f"k={k} data={data} got={got} expected={exp}"


def test_fixed_cases():
    check([3, 1, 3, 2, 1, 3, 2, 2], 3)      # plain
    check([5, 5, 3], 7)                     # more workers than elements
    check([-4, -4, -9, -9, -1], 3)          # negative keys, tie -> -9
    check([1, 2, 3, 4], 1)                  # single worker
    check([i * 4 for i in range(1, 50)] * 2 + [8], 4)  # every key owned by W0
    check([7], 10)                          # only worker 0 has data


def test_corner_cases():
    """Hand-designed corner cases; expected values are fixed, not brute-forced."""
    def expect(data, k, want):
        got = Cluster(list(data), k).findMode()
        assert got == want, f"k={k} data={data} got={got} expected={want}"

    # 1. plain
    expect([1, 2, 2, 3, 3, 3, 4, 4, 4, 4], 3, 4)
    # 2. three-way tie -> smallest
    expect([1, 2, 3, 1, 2, 3], 2, 1)
    # 3. larger dataset, k=10
    expect([7] * 100 + [3] * 50 + [11] * 30 + [5] * 20, 10, 7)
    # 4. negative numbers & tie-breaker: -5 and 5 both appear 3 times
    expect([5, -5, 0, 5, -5, 0, 5, -5], 2, -5)
    # 5. empty workers: k=5, one unique value, so 4 owners receive zero FREQ
    #    messages and must still terminate on SCATTER_END / report MODE_END
    expect([8, 8, 8, 8, 8], 5, 8)
    # 6. fragmented global mode: no shard holds a majority of the 9s
    expect([1, 9, 2, 9, 3, 9, 4, 9, 1, 2, 3, 4], 4, 9)
    # 7. high k, low data: most shards empty from the start
    expect([42], 10, 42)


def test_random(trials=3000, seed=7):
    rng = random.Random(seed)
    for _ in range(trials):
        n = rng.randint(1, 50)
        k = rng.randint(1, 10)
        data = [rng.randint(-15, 15) for _ in range(n)]
        check(data, k)


if __name__ == "__main__":
    test_fixed_cases()
    test_corner_cases()
    test_random()
    print("ok")
