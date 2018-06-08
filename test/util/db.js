/*!
 * db.js - mock db
 * Copyright (c) 2017, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

class DB {
  constructor(watch = false) {
    this.map = new Map();
    this.pending = [];
    this.watch = watch;
    this.items = 0;
    this.size = 0;
  }

  open() {}

  close() {}

  has(key) {
    return this.get(key) !== null;
  }

  get(key) {
    const k = key.toString('hex');
    return this.map.get(k) || null;
  }

  put(key, value) {
    const k = key.toString('hex');
    this.pending.push([k, value]);
  }

  del(key) {
    const k = key.toString('hex');
    this.pending.push([k, null]);
  }

  write() {
    if (this.watch) {
      for (const [k, v] of this.pending) {
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

    for (const [k, v] of this.pending) {
      if (v)
        this.map.set(k, v);
      else
        this.map.delete(k);
    }

    this.pending.length = 0;
  }

  batch() {
    return this;
  }

  clear() {
    this.pending.length = 0;
  }
}

module.exports = DB;
