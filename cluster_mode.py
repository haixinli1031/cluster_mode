"""Distributed mode via hash-partitioned frequency shuffle.

Reference implementation. ``cluster.js`` is a line-for-line port used by the
browser explorer; ``test_cluster_mode.py`` / ``cluster.test.js`` keep the two in
sync by checking both against a brute force.

Protocol (all payloads are plain strings):

    FREQ <num> <count> [<num> <count> ...]
                         scatter: every (num, count) pair a worker holds for one
                         owner (owner = num % k), batched into a single message.
                         Every worker sends exactly one FREQ message to every
                         owner, empty batches included ("FREQ "), so the k-th
                         FREQ message an owner receives is its end-of-scatter
                         marker.
    MODE <num> <count>   report: an owner's best (num, count) sent to worker 0
    MODE_END             sent to worker 0 by every worker after its report
"""


class Worker:
    def __init__(self, workerId, k, localData, cluster):
        self.workerId = workerId
        self.k = k
        self.localData = localData
        self.cluster = cluster
        self.localModeInfo = None

    # Sends a string-represented payload data to the specified worker
    # asynchronously. You should NOT modify this method.
    def sendAsyncMessage(self, targetWorkerId, payload):
        self.cluster.sendAsyncMessage(targetWorkerId, payload)

    # Receives a string-represented payload data from the worker's mailbox. You
    # should NOT modify this method.
    def receive(self):
        return self.cluster.receive(self.workerId)

    # Scatter the freq counts of each num to its owner.
    def scatterFreq(self):
        localFreq = {}
        for num in self.localData:
            localFreq[num] = localFreq.get(num, 0) + 1

        # Hash partition: every occurrence of `num`, on every worker, routes to
        # the same owner, so the owner ends up with the exact global count.
        batches = [[] for _ in range(self.k)]
        for num, count in localFreq.items():
            owner = num % self.k
            batches[owner].append(f"{num} {count}")

        # One batched message per owner, sent even when the batch is empty:
        # that is what lets the owner count k of them and know the scatter
        # is finished.
        for owner in range(self.k):
            self.sendAsyncMessage(owner, "FREQ " + " ".join(batches[owner]))

    # Sum the counts for the nums this worker owns and pick its local mode.
    def aggregateFreq(self):
        aggFreq = {}
        complete = 0

        while complete < self.k:
            msg = self.receive()
            if msg.startswith("FREQ"):
                data = msg.split()
                for i in range(1, len(data), 2):
                    num = int(data[i])
                    count = int(data[i + 1])
                    aggFreq[num] = aggFreq.get(num, 0) + count
                complete += 1

        if not aggFreq:
            return

        # Local mode; the smaller num wins a tie.
        localMode = float('inf')
        maxCount = 0
        for num, count in aggFreq.items():
            if count > maxCount or (count == maxCount and num < localMode):
                maxCount = count
                localMode = num

        self.localModeInfo = [localMode, maxCount]

    # Report the local mode to worker 0.
    def reportLocalMode(self):
        if self.localModeInfo is not None:
            self.sendAsyncMessage(0, f"MODE {int(self.localModeInfo[0])} {int(self.localModeInfo[1])}")
        self.sendAsyncMessage(0, "MODE_END")

    # Worker 0 folds every report (and its own result) into the global mode.
    def getGlobalMode(self):
        globalMode = float('inf')
        maxCount = 0

        if self.localModeInfo is not None:
            globalMode = int(self.localModeInfo[0])
            maxCount = int(self.localModeInfo[1])

        complete = 0
        while complete < self.k - 1:
            msg = self.receive()
            if msg.startswith("MODE "):
                data = msg.split(" ")
                num = int(data[1])
                count = int(data[2])
                if count > maxCount or (count == maxCount and num < globalMode):
                    maxCount = count
                    globalMode = num
            elif msg == "MODE_END":
                complete += 1

        return globalMode


class Cluster:
    def __init__(self, data, k):
        self.k = k

        # Distribute data evenly across workers: the first `remainder` workers
        # get one extra element.
        self.shards = [[] for _ in range(k)]
        totalSize = len(data)
        baseSize = totalSize // k
        remainder = totalSize % k

        index = 0
        for w in range(k):
            chunkSize = baseSize + (1 if w < remainder else 0)
            for _ in range(chunkSize):
                self.shards[w].append(data[index])
                index += 1

        self.mailboxes = {i: [] for i in range(k)}
        # Per-worker cursor: index of the next unread message in its mailbox.
        self.readIndices = {i: 0 for i in range(k)}

        self.workers = [Worker(i, k, self.shards[i], self) for i in range(k)]

    def sendAsyncMessage(self, targetWorkerId, payload):
        self.mailboxes[targetWorkerId].append(payload)

    def receive(self, workerId):
        myMailbox = self.mailboxes[workerId]
        idx = self.readIndices[workerId]
        if idx < len(myMailbox):
            self.readIndices[workerId] = idx + 1
            return myMailbox[idx]
        return ""

    def findMode(self):
        # Phases run strictly in order, so every message a phase waits for has
        # already been delivered when it starts.
        for i in range(self.k):
            self.workers[i].scatterFreq()

        for i in range(self.k):
            self.workers[i].aggregateFreq()

        for i in range(1, self.k):
            self.workers[i].reportLocalMode()

        return self.workers[0].getGlobalMode()


if __name__ == "__main__":
    import sys
    nums = [int(x) for x in sys.argv[2:]] or [3, 1, 3, 2, 1, 3, 2, 2]
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    print(Cluster(nums, k).findMode())
