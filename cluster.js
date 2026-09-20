/* Distributed mode via hash-partitioned frequency shuffle.
 *
 * Line-for-line port of cluster_mode.py. The algorithm is unchanged; the only
 * additions are the `trace` hooks in Worker.sendAsyncMessage / receive and
 * Cluster.findMode, which record every message and phase boundary so the UI
 * can replay a run step by step.
 *
 * Plain script (no modules) so index.html works over file://. Also loadable
 * from node via `require` for cluster.test.js.
 */
(function (root) {
  'use strict';

  var PHASES = ['partition', 'count', 'scatter', 'aggregate', 'report', 'result'];

  function Worker(workerId, k, localData, cluster) {
    this.workerId = workerId;
    this.k = k;
    this.localData = localData;
    this.cluster = cluster;
    this.localModeInfo = null;
    // Kept only for the explorer; the algorithm never reads these back.
    this.localFreq = null;
    this.aggFreq = null;
  }

  // Sends a string-represented payload to the specified worker asynchronously.
  Worker.prototype.sendAsyncMessage = function (targetWorkerId, payload) {
    this.cluster.trace.push({ type: 'send', phase: this.cluster.phase, from: this.workerId, to: targetWorkerId, payload: payload });
    this.cluster.sendAsyncMessage(targetWorkerId, payload);
  };

  // Receives a string-represented payload from this worker's mailbox.
  Worker.prototype.receive = function () {
    var payload = this.cluster.receive(this.workerId);
    this.cluster.trace.push({ type: 'recv', phase: this.cluster.phase, worker: this.workerId, payload: payload });
    return payload;
  };

  // Scatter the freq counts of each num to its owner.
  Worker.prototype.scatterFreq = function () {
    var localFreq = new Map();
    for (var i = 0; i < this.localData.length; i++) {
      var num = this.localData[i];
      localFreq.set(num, (localFreq.get(num) || 0) + 1);
    }
    this.localFreq = localFreq;

    // Hash partition: every occurrence of `num`, on every worker, routes to
    // the same owner, so the owner ends up with the exact global count.
    var self = this;
    localFreq.forEach(function (count, num) {
      var owner = mod(num, self.k);
      self.sendAsyncMessage(owner, 'FREQ ' + num + ' ' + count);
    });

    // Tell every worker this scatter is finished.
    for (var w = 0; w < this.k; w++) {
      this.sendAsyncMessage(w, 'SCATTER_END');
    }
  };

  // Sum the counts for the nums this worker owns and pick its local mode.
  Worker.prototype.aggregateFreq = function () {
    var aggFreq = new Map();
    var complete = 0;

    while (complete < this.k) {
      var msg = this.receive();
      if (msg.indexOf('FREQ') === 0) {
        var data = msg.split(' ');
        var num = parseInt(data[1], 10);
        var count = parseInt(data[2], 10);
        aggFreq.set(num, (aggFreq.get(num) || 0) + count);
      } else if (msg === 'SCATTER_END') {
        complete += 1;
      }
    }
    this.aggFreq = aggFreq;

    if (aggFreq.size === 0) return;

    // Local mode; the smaller num wins a tie.
    var localMode = Infinity;
    var maxCount = 0;
    aggFreq.forEach(function (count, num) {
      if (count > maxCount || (count === maxCount && num < localMode)) {
        maxCount = count;
        localMode = num;
      }
    });

    this.localModeInfo = [localMode, maxCount];
  };

  // Report the local mode to worker 0.
  Worker.prototype.reportLocalMode = function () {
    if (this.localModeInfo !== null) {
      this.sendAsyncMessage(0, 'MODE ' + this.localModeInfo[0] + ' ' + this.localModeInfo[1]);
    }
    this.sendAsyncMessage(0, 'MODE_END');
  };

  // Worker 0 folds every report (and its own result) into the global mode.
  Worker.prototype.getGlobalMode = function () {
    var globalMode = Infinity;
    var maxCount = 0;

    if (this.localModeInfo !== null) {
      globalMode = this.localModeInfo[0];
      maxCount = this.localModeInfo[1];
    }

    var complete = 0;
    while (complete < this.k - 1) {
      var msg = this.receive();
      if (msg.indexOf('MODE ') === 0) {
        var data = msg.split(' ');
        var num = parseInt(data[1], 10);
        var count = parseInt(data[2], 10);
        if (count > maxCount || (count === maxCount && num < globalMode)) {
          maxCount = count;
          globalMode = num;
        }
      } else if (msg === 'MODE_END') {
        complete += 1;
      }
    }

    return globalMode;
  };

  function Cluster(data, k) {
    this.k = k;
    this.trace = [];
    this.phase = null;

    // Distribute data evenly across workers: the first `remainder` workers
    // get one extra element.
    this.shards = [];
    var totalSize = data.length;
    var baseSize = Math.floor(totalSize / k);
    var remainder = totalSize % k;

    var index = 0;
    for (var w = 0; w < k; w++) {
      var chunkSize = baseSize + (w < remainder ? 1 : 0);
      var shard = [];
      for (var j = 0; j < chunkSize; j++) {
        shard.push(data[index]);
        index += 1;
      }
      this.shards.push(shard);
    }

    this.mailboxes = {};
    // Per-worker cursor: index of the next unread message in its mailbox.
    this.readIndices = {};
    for (var i = 0; i < k; i++) {
      this.mailboxes[i] = [];
      this.readIndices[i] = 0;
    }

    this.workers = [];
    for (var m = 0; m < k; m++) {
      this.workers.push(new Worker(m, k, this.shards[m], this));
    }
  }

  Cluster.prototype.sendAsyncMessage = function (targetWorkerId, payload) {
    this.mailboxes[targetWorkerId].push(payload);
  };

  Cluster.prototype.receive = function (workerId) {
    var myMailbox = this.mailboxes[workerId];
    var idx = this.readIndices[workerId];
    if (idx < myMailbox.length) {
      this.readIndices[workerId] = idx + 1;
      return myMailbox[idx];
    }
    return '';
  };

  Cluster.prototype.markPhase = function (name) {
    this.phase = name;
    this.trace.push({ type: 'phase', phase: name, readIndices: Object.assign({}, this.readIndices) });
  };

  Cluster.prototype.findMode = function () {
    // Phases run strictly in order, so every message a phase waits for has
    // already been delivered when it starts.
    this.markPhase('scatter');
    for (var i = 0; i < this.k; i++) this.workers[i].scatterFreq();

    this.markPhase('aggregate');
    for (var a = 0; a < this.k; a++) this.workers[a].aggregateFreq();

    this.markPhase('report');
    for (var r = 1; r < this.k; r++) this.workers[r].reportLocalMode();

    this.markPhase('result');
    var mode = this.workers[0].getGlobalMode();
    this.markPhase('done');
    return mode;
  };

  // Python-style modulo: result has the sign of the divisor, so negative keys
  // still land on a worker in [0, k).
  function mod(n, k) {
    return ((n % k) + k) % k;
  }

  function bruteMode(data) {
    var counts = new Map();
    data.forEach(function (n) { counts.set(n, (counts.get(n) || 0) + 1); });
    var best = 0;
    counts.forEach(function (c) { if (c > best) best = c; });
    var mode = Infinity;
    counts.forEach(function (c, n) { if (c === best && n < mode) mode = n; });
    return mode;
  }

  var api = { Worker: Worker, Cluster: Cluster, mod: mod, bruteMode: bruteMode, PHASES: PHASES };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.ClusterMode = api;
})(typeof self !== 'undefined' ? self : this);
