/*!
 * db.js - mock db
 * Copyright (c) 2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

class DB {
  constructor(watch = false) {
    this.table = new Map();
    this.batch = new Map();
    this.watch = watch;
    this.items = 0;
    this.size = 0;
  }

  has(key) {
    return this.get(key) !== null;
  }

  get(key) {
    const k = key.toString('hex');
    return this.table.get(k) || null;
  }

  put(key, value) {
    const k = key.toString('hex');

    this.batch.set(k, value);
  }

  reset() {
    this.table.clear();
    this.batch.clear();
  }

  flush() {
    if (this.watch) {
      for (const [k, v] of this.batch) {
        if (!this.table.has(k)) {
          this.items += 1;
          this.size += k.length >> 1;
          this.size += v.length;
        }
      }
    }

    for (const [k, v] of this.batch)
      this.table.set(k, v);

    this.batch.clear();
  }
}

module.exports = DB;
