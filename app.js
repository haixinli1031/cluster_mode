/* UI for the distributed-mode explorer. Runs cluster.js once per input change
 * and renders one phase of the recorded trace at a time. */
(function () {
  'use strict';

  var CM = window.ClusterMode;
  var MAX_K = 10, MAX_N = 1000, DEFAULT_N = 20;
  // Detail limits: panels collapse past these so large runs stay readable.
  var CHIP_LIMIT = 20;      // values shown per shard before "+N more"
  var CHIP_COLLAPSE = 30;   // shards larger than this are collapsed
  var ROW_LIMIT = 10;       // rows shown per count table
  var LEGEND_LIMIT = 30;    // distinct values shown as chips in the scatter legend
  var PAIR_LIMIT = 5;       // pairs shown per batch line
  var PAYLOAD_LIMIT = 80;   // characters of a mailbox payload shown before "…"
  var DECODE_PAIRS = 6;     // pairs named in the decoder line

  var PHASES = [
    { id: 'partition', title: 'Partition', blurb: 'The dataset is split evenly across workers. Each worker can only see its own slice.' },
    { id: 'count', title: 'Local count', blurb: 'Each worker counts its own values. Raw data never leaves the worker — only (value, count) pairs will.' },
    { id: 'scatter', title: 'Scatter by hash', blurb: 'Each (value, count) pair belongs to the value’s owner, owner = value mod k, so every occurrence of a value, on every worker, lands on the same owner. Pairs are grouped by owner and sent as one FREQ message per owner — sent even when empty, so the k-th FREQ an owner receives tells it the scatter is finished.' },
    { id: 'aggregate', title: 'Aggregate', blurb: 'Each owner reads its mailbox until it has received k FREQ batches, summing the counts inside them. It now holds the exact global count of every value it owns and picks its local mode (smallest value on a tie).' },
    { id: 'report', title: 'Report', blurb: 'Workers 1..k−1 send their local mode to worker 0 as MODE value count, then MODE_END. Worker 0 uses its own result directly.' },
    { id: 'result', title: 'Result', blurb: 'Worker 0 waits for k−1 MODE_END messages and keeps the highest count, smallest value. That is the global mode.' }
  ];

  var state = { k: 4, data: [], phase: 0, mailboxWorker: 0, run: null, stale: false };

  var $ = function (id) { return document.getElementById(id); };
  var els = {
    kRange: $('kRange'), kOut: $('kOut'), dataInput: $('dataInput'), dataError: $('dataError'),
    sizeInput: $('sizeInput'), sizeError: $('sizeError'), dataCount: $('dataCount'), genRandom: $('genRandom'), genSkew: $('genSkew'), genTies: $('genTies'),
    prevBtn: $('prevBtn'), nextBtn: $('nextBtn'), skipBtn: $('skipBtn'), phaseList: $('phaseList'),
    runBtn: $('runBtn'), bannerRun: $('bannerRun'), revertBtn: $('revertBtn'), staleBanner: $('staleBanner'), staleText: $('staleText'), layout: $('layout'),
    phaseTitle: $('phaseTitle'), phaseBlurb: $('phaseBlurb'), stage: $('stage'),
    mailboxTabs: $('mailboxTabs'), mailbox: $('mailbox'), mailboxDecode: $('mailboxDecode'), netStats: $('netStats')
  };

  // ---- tiny DOM helper -----------------------------------------------------
  function h(tag, attrs, children) {
    var el = document.createElement(tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (key) {
        var v = attrs[key];
        if (v === null || v === undefined || v === false) return;
        if (key === 'class') el.className = v;
        else if (key === 'style') el.setAttribute('style', v);
        else if (key === 'text') el.textContent = v;
        else if (key.indexOf('on') === 0) el.addEventListener(key.slice(2), v);
        else el.setAttribute(key, v === true ? '' : v);
      });
    }
    (children || []).forEach(function (c) {
      if (c === null || c === undefined || c === false) return;
      el.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
    });
    return el;
  }
  function wStyle(w) { return '--wc: var(--w' + w + ')'; }
  function badge(w) { return h('span', { class: 'badge', style: wStyle(w), text: 'W' + w }); }
  function tag(text) { return h('span', { class: 'tag', text: text }); }
  function chip(num, owner) {
    return h('span', { class: 'chip' + (owner !== undefined ? ' owned' : ''), style: owner !== undefined ? wStyle(owner) : null, text: String(num) });
  }
  // Renders the first `limit` items; past that, a "+N more" link expands to all.
  function truncated(items, limit, renderItem, wrapClass, noun) {
    var wrap = h('div', { class: wrapClass });
    function fill(all) {
      wrap.innerHTML = '';
      var shown = all ? items : items.slice(0, limit);
      shown.forEach(function (it) { wrap.appendChild(renderItem(it)); });
      if (items.length > limit) {
        wrap.appendChild(h('button', {
          type: 'button', class: 'link more',
          text: all ? 'show fewer' : '+' + fmtNum(items.length - limit) + ' more ' + noun,
          onclick: function () { fill(!all); }
        }));
      }
    }
    fill(false);
    return wrap;
  }
  function minMax(arr) {
    var lo = Infinity, hi = -Infinity;
    arr.forEach(function (v) { if (v < lo) lo = v; if (v > hi) hi = v; });
    return [lo, hi];
  }

  function sortedEntries(map) {
    return Array.from(map.entries()).sort(function (a, b) { return a[0] - b[0]; });
  }
  function plural(n, word, suffix) { return n + ' ' + word + (n === 1 ? '' : (suffix || 's')); }

  // ---- run the algorithm ---------------------------------------------------
  function runCluster() {
    var cluster = new CM.Cluster(state.data.slice(), state.k);
    var mode = cluster.findMode();
    var markers = {};
    cluster.trace.forEach(function (e) { if (e.type === 'phase') markers[e.phase] = e.readIndices; });
    state.run = { cluster: cluster, mode: mode, markers: markers, brute: CM.bruteMode(state.data) };
  }

  // Messages delivered up to and including the given UI phase, plus the read
  // cursor of each worker at the end of that phase.
  function mailboxView(phaseId) {
    var run = state.run, includePhases, cursorMarker;
    switch (phaseId) {
      case 'scatter': includePhases = ['scatter']; cursorMarker = 'aggregate'; break;
      case 'aggregate': includePhases = ['scatter']; cursorMarker = 'report'; break;
      case 'report': includePhases = ['scatter', 'report']; cursorMarker = 'result'; break;
      case 'result': includePhases = ['scatter', 'report']; cursorMarker = 'done'; break;
      default: includePhases = []; cursorMarker = 'scatter';
    }
    var boxes = {};
    for (var w = 0; w < state.k; w++) boxes[w] = [];
    run.cluster.trace.forEach(function (e) {
      if (e.type === 'send' && includePhases.indexOf(e.phase) !== -1) boxes[e.to].push(e);
    });
    return { boxes: boxes, cursors: run.markers[cursorMarker] };
  }

  // 'FREQ 3 2 6 1' -> [[3, 2], [6, 1]]; 'FREQ ' -> []
  function parsePairs(payload) {
    var t = payload.trim().split(/\s+/), out = [];
    for (var i = 1; i + 1 < t.length; i += 2) out.push([parseInt(t[i], 10), parseInt(t[i + 1], 10)]);
    return out;
  }
  function fmtNum(x) { return Math.round(x).toLocaleString('en-US'); }

  function sendsIn(phaseId) {
    return state.run.cluster.trace.filter(function (e) { return e.type === 'send' && e.phase === phaseId; });
  }

  // ---- phase renderers -----------------------------------------------------
  function workerCard(w, meta, body) {
    var shard = state.run.cluster.shards[w];
    return h('div', { class: 'worker', style: wStyle(w) }, [
      h('header', null, [h('span', { class: 'wname', text: 'W' + w }), h('span', { class: 'muted', text: meta || plural(shard.length, 'value') })])
    ].concat(body));
  }

  function renderPartition() {
    var n = state.data.length, k = state.k;
    var base = Math.floor(n / k), rem = n % k;
    var note = h('p', { class: 'muted small' }, [
      'n = ' + n + ', k = ' + k + ' → base ' + base + ' per worker',
      rem ? ', first ' + rem + ' worker' + (rem === 1 ? '' : 's') + ' get one extra' : ''
    ]);
    var cards = state.run.cluster.shards.map(function (shard, w) {
      if (!shard.length) return workerCard(w, null, [h('p', { class: 'empty', text: 'no data' })]);
      var big = shard.length > CHIP_COLLAPSE, mm = minMax(shard);
      var meta = big ? fmtNum(shard.length) + ' values · ' + new Set(shard).size + ' distinct · min ' + mm[0] + ' · max ' + mm[1] : null;
      return workerCard(w, meta, [truncated(shard, big ? CHIP_LIMIT : Infinity, function (v) { return chip(v); }, 'chips', 'values')]);
    });
    return [note, h('div', { class: 'workers' }, cards)];
  }

  function freqTable(entries, best, ownerOf) {
    var big = entries.length > ROW_LIMIT;
    // Large tables sort by count so the rows that matter are the visible ones.
    var rows = big ? entries.slice().sort(function (a, b) { return b[1] - a[1] || a[0] - b[0]; }) : entries;
    var cols = ownerOf ? 3 : 2;
    var table = h('table', null, [
      h('thead', null, [h('tr', null, [h('th', { text: 'value' }), h('th', { text: 'count', class: 'num' }), ownerOf ? h('th', { text: 'owner' }) : null])])
    ]);
    var tbody = h('tbody');
    function fill(all) {
      tbody.innerHTML = '';
      (all ? rows : rows.slice(0, ROW_LIMIT)).forEach(function (e) {
        tbody.appendChild(h('tr', { class: best && best.indexOf(e[0]) !== -1 ? 'best' : null }, [
          h('td', null, [chip(e[0], ownerOf ? ownerOf(e[0]) : undefined)]),
          h('td', { class: 'num', text: e[1] }),
          ownerOf ? h('td', null, [badge(ownerOf(e[0]))]) : null
        ]));
      });
      if (big) {
        tbody.appendChild(h('tr', null, [h('td', { colspan: cols }, [h('button', {
          type: 'button', class: 'link more',
          text: all ? 'show top ' + ROW_LIMIT : '+' + fmtNum(rows.length - ROW_LIMIT) + ' more values',
          onclick: function () { fill(!all); }
        })])]));
      }
    }
    fill(false);
    table.appendChild(tbody);
    if (big) table.appendChild(h('caption', { class: 'muted small', text: 'sorted by count' }));
    return table;
  }

  function renderCount() {
    var cards = state.run.cluster.workers.map(function (worker, w) {
      var entries = sortedEntries(worker.localFreq);
      return workerCard(w, plural(entries.length, 'distinct value'), [entries.length ? freqTable(entries) : h('p', { class: 'empty', text: 'nothing to count' })]);
    });
    return [h('div', { class: 'workers' }, cards)];
  }

  function renderScatter() {
    var k = state.k;
    var ownerOf = function (v) { return CM.mod(v, k); };
    var distinct = Array.from(new Set(state.data)).sort(function (a, b) { return a - b; });
    var legend = distinct.length > LEGEND_LIMIT
      ? h('p', { class: 'muted small', text: fmtNum(distinct.length) + ' distinct values; owner = value mod ' + k + '. Per-owner totals are in the balance bars below.' })
      : h('div', { class: 'legend' }, [h('span', { class: 'muted small', text: 'owner = value mod ' + k + ':' })].concat(distinct.map(function (v) {
          return h('span', null, [chip(v, ownerOf(v)), ' ', badge(ownerOf(v)), ' ']);
        })));

    var sends = sendsIn('scatter');
    var cards = state.run.cluster.workers.map(function (worker, w) {
      var mine = sends.filter(function (e) { return e.from === w; });
      var pairTotal = 0;
      var items = mine.map(function (e) {
        var pairs = parsePairs(e.payload);
        pairTotal += pairs.length;
        var parts = [h('code', { text: 'FREQ' }), h('span', { class: 'arrow', text: '→' }), badge(e.to)];
        if (pairs.length) parts.push(truncated(pairs, PAIR_LIMIT, function (pr) { return h('span', { class: 'pair', text: pr[0] + ' ' + pr[1] }); }, 'pairs', 'pairs'));
        else parts.push(h('span', { class: 'muted', text: '(empty)' }));
        return h('li', null, parts);
      });
      return workerCard(w, 'sends ' + plural(mine.length, 'message') + ' · ' + plural(pairTotal, 'pair'), [h('ul', { class: 'msgs' }, items)]);
    });

    // Every owner receives exactly k messages now, so balance is measured in
    // pairs (and bytes) received, which is where hash skew actually shows.
    var received = [];
    for (var w = 0; w < k; w++) received.push({ w: w, pairs: 0, bytes: 0 });
    sends.forEach(function (e) { received[e.to].pairs += parsePairs(e.payload).length; received[e.to].bytes += e.payload.length; });
    var maxPairs = Math.max(1, Math.max.apply(null, received.map(function (r) { return r.pairs; })));
    var bars = h('div', { class: 'bars' }, [].concat.apply([], received.map(function (r) {
      return [
        badge(r.w),
        h('div', null, [h('div', { class: 'bar', style: wStyle(r.w) + '; width: ' + (100 * r.pairs / maxPairs) + '%' })]),
        h('span', { class: 'val', text: plural(r.pairs, 'pair') + ' · ' + r.bytes + ' B' })
      ];
    })));
    var skewNote = h('p', { class: 'muted small', text: '(value, count) pairs received per owner — each owner gets exactly ' + k + ' messages, so this is where hash skew shows. A lopsided chart means the partition is unbalanced; try the “Skewed” preset.' });

    return [legend, h('div', { class: 'workers' }, cards), h('div', { class: 'section' }, [h('h4', { text: 'Shuffle balance' }), bars, skewNote])];
  }

  function tiedKeys(map, maxCount) {
    return sortedEntries(map).filter(function (e) { return e[1] === maxCount; }).map(function (e) { return e[0]; });
  }

  function renderAggregate() {
    var k = state.k;
    var cards = state.run.cluster.workers.map(function (worker, w) {
      var entries = sortedEntries(worker.aggFreq);
      var freqCount = entries.length;
      var meta = 'in: ' + plural(k, 'batch', 'es') + ' · ' + plural(freqCount, 'pair');
      if (!entries.length) {
        return workerCard(w, meta, [h('p', { class: 'empty', text: 'owns no values — will send only MODE_END' })]);
      }
      var info = worker.localModeInfo, ties = tiedKeys(worker.aggFreq, info[1]);
      var body = [
        freqTable(entries, ties),
        h('p', { class: 'verdict' }, ['local mode ', h('strong', { text: info[0] }), ' × ' + info[1]]),
        ties.length > 1 ? h('p', { class: 'tie', text: 'tie between ' + ties.join(', ') + ' → smallest wins' }) : null
      ];
      return workerCard(w, meta, body);
    });
    return [h('div', { class: 'workers' }, cards)];
  }

  function renderReport() {
    var k = state.k, workers = state.run.cluster.workers;
    var w0 = workers[0];
    var seed = h('p', null, [badge(0), ' keeps its own result: ', w0.localModeInfo ? h('strong', { text: w0.localModeInfo[0] + ' × ' + w0.localModeInfo[1] }) : h('span', { class: 'empty', text: 'nothing (owns no values)' })]);
    var items = [];
    for (var w = 1; w < k; w++) {
      var info = workers[w].localModeInfo;
      items.push(h('li', null, [badge(w), h('span', { class: 'arrow', text: '→' }), badge(0),
        info ? h('code', { text: 'MODE ' + info[0] + ' ' + info[1] }) : h('span', { class: 'empty', text: '(no MODE — owns no values)' }),
        h('code', { text: 'MODE_END' })]));
    }
    var list = k > 1 ? h('ul', { class: 'msgs' }, items) : h('p', { class: 'empty', text: 'single worker — nothing to report' });
    return [seed, h('div', { class: 'section' }, [h('h4', { text: 'Reports into worker 0’s mailbox' }), list])];
  }

  function renderResult() {
    var run = state.run, k = state.k, workers = run.cluster.workers;
    var candidates = [];
    workers.forEach(function (worker, w) { if (worker.localModeInfo) candidates.push({ w: w, num: worker.localModeInfo[0], count: worker.localModeInfo[1] }); });
    var best = Math.max.apply(null, candidates.map(function (c) { return c.count; }));
    var atBest = candidates.filter(function (c) { return c.count === best; }).sort(function (a, b) { return a.num - b.num; });
    var owner = CM.mod(run.mode, k);

    var hero = h('div', { class: 'hero' }, [
      h('div', null, [h('div', { class: 'big', text: run.mode }), h('div', { class: 'sub', text: 'global mode' })]),
      h('div', null, [h('div', { class: 'big', text: best }), h('div', { class: 'sub', text: 'occurrences' })]),
      h('div', null, [h('div', null, ['owned by ', badge(owner)]), h('div', { class: 'sub', text: run.mode + ' mod ' + k + ' = ' + owner })])
    ]);
    var checkOk = run.mode === run.brute;
    var check = h('p', { class: 'check ' + (checkOk ? 'ok' : 'bad'), text: (checkOk ? '✓ matches' : '✗ differs from') + ' a single-machine brute force (' + run.brute + ')' });

    var rows = candidates.sort(function (a, b) { return a.w - b.w; }).map(function (c) {
      return h('tr', { class: c.num === run.mode && c.count === best ? 'best' : null }, [
        h('td', null, [badge(c.w)]), h('td', null, [chip(c.num, c.w)]), h('td', { class: 'num', text: c.count })
      ]);
    });
    var table = h('table', null, [h('thead', null, [h('tr', null, [h('th', { text: 'from' }), h('th', { text: 'local mode' }), h('th', { text: 'count', class: 'num' })])]), h('tbody', null, rows)]);
    var explain = atBest.length > 1
      ? h('p', { class: 'tie' }, ['Tie at ' + best + ' between ' + atBest.map(function (c) { return c.num + ' (W' + c.w + ')'; }).join(', ') + ' → smallest value, ' + run.mode + ', wins.'])
      : h('p', { class: 'muted small', text: 'Only one candidate reached ' + best + '; no tie-break needed.' });
    var why = h('p', { class: 'muted small', text: 'Why this is correct: each value has exactly one owner, so every count worker 0 compares is already a global count. The max over owners is the global max, and each owner reports its smallest value at that count.' });

    return [hero, check, h('div', { class: 'section' }, [h('h4', { text: 'What worker 0 compared' }), table, explain, why])];
  }

  var RENDER = { partition: renderPartition, count: renderCount, scatter: renderScatter, aggregate: renderAggregate, report: renderReport, result: renderResult };

  // ---- side panels ---------------------------------------------------------
  // Plain-English reading of one raw payload, for the decoder line.
  function decodeMessage(e, to) {
    var k = state.k;
    if (e.payload.indexOf('FREQ') === 0) {
      var pairs = parsePairs(e.payload);
      if (!pairs.length) return 'Empty batch from W' + e.from + ' \u2192 W' + to + ': W' + e.from + ' holds no values that W' + to + ' owns. Still sent, and still one of the ' + k + ' batches W' + to + ' waits for.';
      var shown = pairs.length > DECODE_PAIRS ? pairs.slice().sort(function (a, b) { return b[1] - a[1] || a[0] - b[0]; }).slice(0, DECODE_PAIRS) : pairs;
      var list = shown.map(function (pr) { return pr[0] + ' \u00d7' + pr[1]; }).join(', ');
      if (pairs.length > DECODE_PAIRS) list = plural(pairs.length, 'pair') + ' (largest: ' + list + ', \u2026)';
      return 'Batch from W' + e.from + ' \u2192 W' + to + ': ' + list + ' \u2014 W' + e.from + '\u2019s local counts for the ' + plural(pairs.length, 'value') + ' that W' + to + ' owns (value mod ' + k + ' = ' + to + ').';
    }
    if (e.payload.indexOf('MODE ') === 0) {
      var d = e.payload.split(' ');
      return 'W' + e.from + ' reports its local mode to W0: value ' + d[1] + ' with global count ' + d[2] + '. W0 keeps the highest count, smallest value.';
    }
    if (e.payload === 'MODE_END') return 'W' + e.from + ' has finished reporting. W0 waits for k \u2212 1 = ' + (k - 1) + ' of these before deciding.';
    return e.payload;
  }
  // Long payloads show a prefix with a "+N chars" toggle; bytes are always counted in full.
  function payloadSpan(payload) {
    if (payload.length <= PAYLOAD_LIMIT) return h('span', { text: payload });
    var span = h('span'), full = false;
    function fill() {
      span.innerHTML = '';
      span.appendChild(document.createTextNode(full ? payload : payload.slice(0, PAYLOAD_LIMIT) + '…'));
      span.appendChild(h('button', {
        type: 'button', class: 'link more', text: full ? 'less' : '+' + fmtNum(payload.length - PAYLOAD_LIMIT) + ' chars',
        onclick: function (ev) { ev.stopPropagation(); full = !full; fill(); }
      }));
    }
    fill();
    return span;
  }
  function setDecode(text, active) {
    els.mailboxDecode.textContent = text;
    els.mailboxDecode.classList.toggle('active', !!active);
  }

  function renderMailbox() {
    var phaseId = PHASES[state.phase].id;
    var view = mailboxView(phaseId);
    if (state.mailboxWorker >= state.k) state.mailboxWorker = 0;

    els.mailboxTabs.innerHTML = '';
    for (var w = 0; w < state.k; w++) {
      (function (w) {
        els.mailboxTabs.appendChild(h('button', {
          type: 'button', role: 'tab', style: wStyle(w), 'aria-selected': w === state.mailboxWorker ? 'true' : 'false',
          text: 'W' + w + ' (' + view.boxes[w].length + ')',
          onclick: function () { state.mailboxWorker = w; renderMailbox(); }
        }));
      })(w);
    }

    var box = view.boxes[state.mailboxWorker], cursor = view.cursors[state.mailboxWorker];
    var to = state.mailboxWorker;
    els.mailbox.innerHTML = '';
    setDecode(box.length ? 'Hover or tap a message to decode it.' : 'No messages yet in this phase.', false);
    if (!box.length) { els.mailbox.appendChild(h('li', { class: 'empty', text: 'empty' })); return; }
    if (cursor === 0) els.mailbox.appendChild(h('li', { class: 'cursor-label', text: '▼ unread from here' }));
    box.forEach(function (e, i) {
      var explain = function () { setDecode(decodeMessage(e, to), true); };
      var li = h('li', {
        class: (i >= cursor ? 'unread' : '') + (i === cursor - 1 ? ' cursor' : ''), tabindex: '0',
        onmouseenter: explain, onfocus: explain, onclick: explain
      }, [
        h('span', { class: 'idx', text: i }), badge(e.from), payloadSpan(e.payload),
        e.payload === 'FREQ ' ? h('span', { class: 'muted', text: '(empty batch)' }) : null
      ]);
      els.mailbox.appendChild(li);
      if (i === cursor - 1 && cursor < box.length) els.mailbox.appendChild(h('li', { class: 'cursor-label', text: '▲ read so far · ▼ unread' }));
    });
  }

  // Model this dataset's value mix at a larger n: every worker holds every
  // distinct value in proportion, batches one FREQ per owner, owners report
  // to W0. Returns projected message and byte totals.
  function project(N) {
    var k = state.k, n = state.data.length;
    var counts = new Map();
    state.data.forEach(function (v) { counts.set(v, (counts.get(v) || 0) + 1); });
    var perWorker = new Map();   // value -> count held by ONE worker at scale N
    counts.forEach(function (c, v) { perWorker.set(v, Math.max(1, Math.round(c * N / n / k))); });

    var bytes = 0, msgs = 0;
    for (var w = 0; w < k; w++) {
      for (var o = 0; o < k; o++) {
        var parts = [];
        perWorker.forEach(function (c, v) { if (CM.mod(v, k) === o) parts.push(v + ' ' + c); });
        bytes += ('FREQ ' + parts.join(' ')).length; msgs++;
      }
    }
    // Report phase: each owner with values sends its (mode, global count).
    var ownerBest = new Map();
    perWorker.forEach(function (c, v) {
      var o = CM.mod(v, k), g = c * k, cur = ownerBest.get(o);
      if (!cur || g > cur[1] || (g === cur[1] && v < cur[0])) ownerBest.set(o, [v, g]);
    });
    for (var r = 1; r < k; r++) {
      var b = ownerBest.get(r);
      if (b) { bytes += ('MODE ' + b[0] + ' ' + b[1]).length; msgs++; }
      bytes += 'MODE_END'.length; msgs++;
    }
    return { msgs: msgs, bytes: bytes };
  }

  function renderStats() {
    var run = state.run, k = state.k, n = state.data.length;
    var sends = run.cluster.trace.filter(function (e) { return e.type === 'send'; });
    var scatter = sends.filter(function (e) { return e.phase === 'scatter'; });
    var bytes = sends.reduce(function (s, e) { return s + e.payload.length; }, 0);

    var pairs = 0, pairsPerOwner = {};
    scatter.forEach(function (e) {
      var ps = parsePairs(e.payload);
      pairs += ps.length;
      pairsPerOwner[e.to] = (pairsPerOwner[e.to] || 0) + ps.length;
    });
    var maxPairs = Math.max.apply(null, [0].concat(Object.keys(pairsPerOwner).map(function (w) { return pairsPerOwner[w]; })));
    var rows = [
      ['messages', fmtNum(sends.length)],
      ['payload bytes', fmtNum(bytes)],
      ['(value, count) pairs shuffled', fmtNum(pairs)],
      ['raw values leaving their worker', '0'],
      ['max pairs into one owner', fmtNum(maxPairs)]
    ];
    els.netStats.innerHTML = '';
    var grid = h('div', { class: 'stats stats-2' });
    rows.forEach(function (r) {
      grid.appendChild(h('div', { class: 'h', text: r[0] }));
      grid.appendChild(h('div', { class: 'r', text: r[1] }));
    });
    els.netStats.appendChild(grid);
    els.netStats.appendChild(h('p', { class: 'muted small', text: 'One FREQ message per worker \u2192 owner, so the scatter is always ' + k + '\u00b2 = ' + (k * k) + ' messages however large the data, plus the reports to W0. Only (value, count) pairs travel: no worker ever receives another worker\u2019s raw data. Max pairs into one owner shows how evenly the hash spreads the load.' }));

    // Projection block
    var sizes = [1000, 100000, 1000000];
    var head = h('tr', null, [h('th', { text: 'n' }), h('th', { text: 'messages', class: 'num' }), h('th', { text: 'payload bytes', class: 'num' }), h('th', { text: 'bytes / value', class: 'num' })]);
    var body = [h('tr', { class: 'best' }, [h('td', { class: 'nowrap', text: fmtNum(n) + ' (this run)' }), h('td', { class: 'num', text: fmtNum(sends.length) }), h('td', { class: 'num', text: fmtNum(bytes) }), h('td', { class: 'num', text: (bytes / n).toFixed(2) })])];
    sizes.forEach(function (N) {
      if (N <= n) return;
      var pj = project(N);
      body.push(h('tr', null, [h('td', { text: fmtNum(N) }), h('td', { class: 'num', text: fmtNum(pj.msgs) }), h('td', { class: 'num', text: fmtNum(pj.bytes) }), h('td', { class: 'num', text: (pj.bytes / N).toFixed(pj.bytes / N < 0.01 ? 4 : 2) })]));
    });
    var distinct = new Set(state.data).size;
    els.netStats.appendChild(h('div', { class: 'section' }, [
      h('h4', { text: 'Projection — same value mix, more data' }),
      h('table', { class: 'proj' }, [h('thead', null, [head]), h('tbody', null, body)]),
      h('p', { class: 'muted small', text: 'Assumes the ' + plural(distinct, 'distinct value') + ' above keep their proportions as n grows. Messages never change; bytes grow only with the number of distinct values (their counts gain digits), so the cost per value falls toward zero. Data with many rarely repeated values keeps bytes closer to n.' })
    ]));
  }

  // ---- top-level render ----------------------------------------------------
  function render() {
    var phase = PHASES[state.phase];
    els.phaseTitle.textContent = (state.phase + 1) + '. ' + phase.title;
    els.phaseBlurb.textContent = phase.blurb;
    els.stage.innerHTML = '';
    RENDER[phase.id]().forEach(function (node) { els.stage.appendChild(node); });

    els.phaseList.innerHTML = '';
    PHASES.forEach(function (p, i) {
      els.phaseList.appendChild(h('li', null, [h('button', {
        type: 'button', 'aria-current': i === state.phase ? 'step' : null,
        onclick: function () { if (!state.stale) { state.phase = i; render(); } }
      }, [h('span', { class: 'num', text: i + 1 }), p.title])]));
    });
    updateNav();

    renderMailbox();
    renderStats();
  }

  // ---- inputs --------------------------------------------------------------
  // The controls are a draft. Nothing recomputes until Run applies the draft
  // to `state`; until then the page is marked stale and navigation is locked.
  var draft = { k: state.k };

  function parseData(text) {
    var tokens = text.split(/[\s,;]+/).filter(Boolean);
    if (!tokens.length) throw new Error('Enter at least one integer.');
    if (tokens.length > MAX_N) throw new Error('At most ' + MAX_N + ' numbers (got ' + tokens.length + ').');
    return tokens.map(function (t) {
      if (!/^[+-]?\d+$/.test(t)) throw new Error('"' + t + '" is not an integer.');
      var v = parseInt(t, 10);
      if (!Number.isSafeInteger(v)) throw new Error('"' + t + '" is too large.');
      return v;
    });
  }

  // Parses the dataset box, showing or clearing the inline error. null if invalid.
  function validateDraft() {
    try {
      var data = parseData(els.dataInput.value);
      els.dataError.hidden = true;
      els.dataCount.textContent = plural(fmtNum(data.length), 'value') + ' · ' + plural(new Set(data).size, 'distinct value');
      return data;
    } catch (err) {
      els.dataError.textContent = err.message;
      els.dataError.hidden = false;
      els.dataCount.textContent = '';
      return null;
    }
  }

  function sameData(a, b) {
    if (a.length !== b.length) return false;
    for (var i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
    return true;
  }

  // How the draft differs from the applied run, or null when it doesn't.
  function pendingChanges() {
    var changes = [];
    if (draft.k !== state.k) changes.push('workers ' + state.k + ' → ' + draft.k);
    var data = validateDraft();
    if (data === null) changes.push('dataset invalid');
    else if (!sameData(data, state.data)) changes.push('dataset edited');
    return changes.length ? changes : null;
  }

  function updateStale() {
    var changes = pendingChanges();
    state.stale = !!changes;
    els.staleBanner.hidden = !state.stale;
    if (state.stale) {
      els.staleText.textContent = changes.indexOf('dataset invalid') !== -1
        ? 'Dataset is invalid — fix it, then Run.'
        : 'Inputs changed — ' + changes.join(', ') + '. Run to recompute from step 1.';
    }
    els.layout.classList.toggle('stale', state.stale);
    els.runBtn.classList.toggle('attention', state.stale);
    updateNav();
  }

  function updateNav() {
    var lock = state.stale;
    els.prevBtn.disabled = lock || state.phase === 0;
    els.nextBtn.disabled = lock || state.phase === PHASES.length - 1;
    els.skipBtn.disabled = lock || state.phase === PHASES.length - 1;
    Array.prototype.forEach.call(els.phaseList.querySelectorAll('button'), function (b) { b.disabled = lock; });
  }

  function run() {
    var data = validateDraft();
    if (data === null) { els.dataInput.focus(); return; }
    state.k = draft.k;
    state.data = data;
    state.phase = 0;
    runCluster();
    render();
    updateStale();
  }

  function revert() {
    draft.k = state.k;
    els.kRange.value = state.k;
    els.kOut.value = state.k;
    els.dataInput.value = state.data.join(', ');
    updateStale();
  }

  // Generators only fill the dataset box; the user still has to press Run.
  function setDraftData(arr) {
    els.dataInput.value = arr.join(', ');
    updateStale();
  }
  // From a button click, move focus to Run so the next step is obvious.
  function generateFromClick(gen) {
    return function () {
      if (size() === null) { els.sizeInput.focus(); return; }
      gen();
      els.runBtn.focus();
    };
  }

  // The requested size, or null (with an inline error) when it is not an
  // integer between 1 and MAX_N. Nothing is clamped silently.
  function size() {
    var raw = els.sizeInput.value.trim();
    var n = /^\d+$/.test(raw) ? parseInt(raw, 10) : NaN;
    var msg = '';
    if (raw === '') msg = 'Enter how many values to generate.';
    else if (isNaN(n)) msg = 'Size must be a whole number.';
    else if (n < 1) msg = 'Size must be at least 1.';
    else if (n > MAX_N) msg = 'Size is ' + fmtNum(n) + ' \u2014 the maximum is ' + fmtNum(MAX_N) + '.';
    els.sizeError.textContent = msg;
    els.sizeError.hidden = !msg;
    els.sizeInput.setAttribute('aria-invalid', msg ? 'true' : 'false');
    return msg ? null : n;
  }
  function randint(lo, hi) { return lo + Math.floor(Math.random() * (hi - lo + 1)); }

  // Value range widens with size so the shuffle stays interesting: 1–9 up to
  // 50 values, 1–99 up to 500, 1–999 beyond.
  function randomRange(n) { return n <= 50 ? 9 : n <= 500 ? 99 : 999; }
  function genRandom() {
    var n = size(); if (n === null) return;
    var hi = randomRange(n), out = [];
    for (var i = 0; i < n; i++) out.push(randint(1, hi));
    setDraftData(out);
  }
  // Every value is a multiple of k, so value mod k = 0 and worker 0 owns all of them.
  function genSkew() {
    var n = size(); if (n === null) return;
    var out = [];
    for (var i = 0; i < n; i++) out.push(draft.k * randint(1, 5));
    setDraftData(out);
  }
  // Several distinct values, each repeated the same number of times, shuffled.
  function genTies() {
    var n = size(); if (n === null) return;
    var reps = Math.min(n, Math.max(2, Math.min(4, Math.floor(n / 3))));
    var distinct = Math.max(1, Math.floor(n / reps)), out = [];
    for (var v = 1; v <= distinct; v++) for (var r = 0; r < reps; r++) out.push(v);
    while (out.length < n) out.push(randint(distinct + 1, distinct + 9));
    for (var i = out.length - 1; i > 0; i--) { var j = randint(0, i); var t = out[i]; out[i] = out[j]; out[j] = t; }
    setDraftData(out);
  }

  els.kRange.addEventListener('input', function () {
    draft.k = Math.min(MAX_K, Math.max(1, parseInt(els.kRange.value, 10)));
    els.kOut.value = draft.k;
    updateStale();
  });
  els.dataInput.addEventListener('input', updateStale);
  els.sizeInput.addEventListener('input', size);
  els.dataInput.addEventListener('keydown', function (e) {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) { e.preventDefault(); run(); }
  });
  els.runBtn.addEventListener('click', run);
  els.bannerRun.addEventListener('click', run);
  els.revertBtn.addEventListener('click', revert);
  els.genRandom.addEventListener('click', generateFromClick(genRandom));
  els.genSkew.addEventListener('click', generateFromClick(genSkew));
  els.genTies.addEventListener('click', generateFromClick(genTies));
  els.prevBtn.addEventListener('click', function () { if (!state.stale && state.phase > 0) { state.phase--; render(); } });
  els.nextBtn.addEventListener('click', function () { if (!state.stale && state.phase < PHASES.length - 1) { state.phase++; render(); } });
  els.skipBtn.addEventListener('click', function () { if (!state.stale) { state.phase = PHASES.length - 1; render(); } });
  document.addEventListener('keydown', function (e) {
    if (e.target.tagName === 'TEXTAREA' || e.target.tagName === 'INPUT' || state.stale) return;
    if (e.key === 'ArrowLeft') els.prevBtn.click();
    if (e.key === 'ArrowRight') els.nextBtn.click();
  });

  // Boot: k = 4 and a random dataset of DEFAULT_N values, run once so the
  // page is never empty. After this, every run goes through the Run button.
  els.kRange.value = state.k; els.kOut.value = state.k;
  els.sizeInput.value = DEFAULT_N;
  genRandom();
  run();
})();
