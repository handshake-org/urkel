/*!
 * hashlist.js - memory optimal hash list
 * Copyright (c) 2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');

/*
 * Constants
 */

const DUMMY32 = Buffer.allocUnsafe(32);

/**
 * HashList
 */

class HashList {
  constructor() {
    this.data = DUMMY32;
    this.pos = 0;
  }

  get length() {
    return this.pos >>> 5;
  }

  set length(len) {
    this.pos = (len << 5) >>> 0;

    if (this.pos > this.data.length)
      this.pos = this.data.length;
  }

  [Symbol.iterator]() {
    return this.values();
  }

  keys() {
    return this.values();
  }

  *values() {
    for (let i = 0; i < this.pos; i += 32)
      yield this.data.slice(i, i + 32);
  }

  clone() {
    const list = new this.constructor();
    list.data = copy(this.data);
    list.pos = this.pos;
    return list;
  }

  push(hash) {
    assert(Buffer.isBuffer(hash) && hash.length === 32);

    if (this.data === DUMMY32)
      this.data = Buffer.allocUnsafe(256 * 32);

    if (this.pos === this.data.length)
      this.data = realloc(this.data, this.pos * 2);

    assert(this.pos + 32 <= this.data.length);

    this.pos += hash.copy(this.data, this.pos);

    return this;
  }

  pop() {
    if (this.pos === 0)
      return null;

    this.pos -= 32;

    return this.data.slice(this.pos, this.pos + 32);
  }

  clear() {
    this.pos = 0;
    return this;
  }

  encode() {
    return this.data.slice(0, this.pos);
  }

  decode(data) {
    assert(Buffer.isBuffer(data));
    assert((data.length & 31) === 0);
    this.data = data;
    this.pos = 0;
    return this;
  }

  static decode(data) {
    return new HashList().decode(data);
  }

  *valuesSafe() {
    for (let i = 0; i < this.pos; i += 32)
      yield copy(this.data.slice(i, i + 32));
  }

  popSafe() {
    const hash = this.pop();

    if (!hash)
      return null;

    return copy(hash);
  }

  encodeSafe() {
    return copy(this.encode());
  }

  decodeSafe(data) {
    return this.decode(copy(data));
  }

  static decodeSafe(data) {
    return new HashList().decodeSafe(data);
  }
}

/*
 * Helpers
 */

function realloc(data, size) {
  assert(Buffer.isBuffer(data));
  assert((size >>> 0) === size);
  assert(size >= data.length);
  const buf = Buffer.allocUnsafe(size);
  data.copy(buf, 0);
  return buf;
}

function copy(buf) {
  return realloc(buf, buf.length);
}

/*
 * Expose
 */

module.exports = HashList;
