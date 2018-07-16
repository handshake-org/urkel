'use strict';

const {performance} = require('perf_hooks');

function mb(num) {
  return Math.floor(num / (1 << 20));
}

function memoryUsage() {
  const mem = process.memoryUsage();

  return {
    total: mb(mem.rss),
    jsHeap: mb(mem.heapUsed),
    jsHeapTotal: mb(mem.heapTotal),
    nativeHeap: mb(mem.rss - mem.heapTotal),
    external: mb(mem.external)
  };
}

function memory(prefix = 'Memory') {
  const mem = memoryUsage();

  console.log(
    '%s: rss=%dmb, js-heap=%d/%dmb native-heap=%dmb',
    prefix,
    mem.total,
    mem.jsHeap,
    mem.jsHeapTotal,
    mem.nativeHeap
  );
}

function logMemory() {
  const gc = global.gc;

  if (typeof gc === 'function') {
    memory('Pre-GC');
    gc();
  }

  memory('Memory');
}

function wait() {
  return new Promise(r => setTimeout(r, 1000));
}

function now() {
  return performance.now() >>> 0;
}

function bench(time) {
  if (time) {
    const [hi, lo] = process.hrtime(time);
    return (hi * 1000 + lo / 1e6).toFixed(2);
  }

  return process.hrtime();
}

exports.memory = memory;
exports.logMemory = logMemory;
exports.wait = wait;
exports.now = now;
exports.bench = bench;
