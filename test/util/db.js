/*!
 * db.js - mock db
 * Copyright (c) 2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

class DB {
  constructor(watch = false) {
    this.map = new Map();
    this.batch = [];
    this.watch = watch;
    this.items = 0;
    this.size = 0;
  }

  has(key) {
    return this.get(key) !== null;
  }

  get(key) {
    const k = key.toString('hex');
    return this.map.get(k) || null;
  }

  put(key, value) {
    const k = key.toString('hex');
    this.batch.push([k, value]);
  }

  del(key) {
    const k = key.toString('hex');
    this.batch.push([k, null]);
  }

  reset() {
    this.map.clear();
    this.batch.length = 0;
  }

  write() {
    this.flush();
  }

  clear() {
    this.reset();
  }

  flush() {
    if (this.watch) {
      for (const [k, v] of this.batch) {
        const c = this.map.get(k);

        if (v) {
          if (c) {
            this.size -= c.length;
            this.size += v.length;
          } else {
            this.items += 1;
            this.size += k.length >> 1;
            this.size += v.length;
          }
        } else {
          if (c) {
            this.items -= 1;
            this.size -= k.length >> 1;
            this.size -= c.length;
          }
        }
      }
    }

    for (const [k, v] of this.batch) {
      if (v)
        this.map.set(k, v);
      else
        this.map.delete(k);
    }

    this.batch.length = 0;
  }
}

module.exports = DB;
