'use strict';

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

function _logMemory(prefix) {
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
  if (typeof gc === 'function') {
    _logMemory('Pre-GC');
    gc();
  }
  _logMemory('Memory');
}

function wait() {
  return new Promise((r) => setTimeout(r, 1000));
}

function createDB(cacheSize, compression) {
  if (cacheSize == null)
    cacheSize = 8 << 20;

  if (compression == null)
    compression = true;

  const bdb = require('bdb');

  return bdb.create({
    location: __dirname + '/benchdb',
    memory: false,
    compression,
    cacheSize,
    createIfMissing: true,
    errorIfExists: true
  });
}

exports.logMemory = logMemory;
exports.wait = wait;
exports.createDB = createDB;
