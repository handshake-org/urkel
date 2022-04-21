/*!
 * bits.js - bits object
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

/* eslint no-use-before-define: "off" */

'use strict';

const assert = require('bsert');
const common = require('./common');
const {EncodingError} = require('./errors');

const {
  EMPTY,
  hasBit,
  setBit
} = common;

/**
 * Bits
 */

class Bits {
  constructor() {
    this.size = 0;
    this.data = EMPTY;
  }

  clone() {
    if (this.size === 0)
      return this;

    const copy = new this.constructor();
    copy.size = this.size;
    copy.data = Buffer.from(this.data);

    return copy;
  }

  get(index) {
    return hasBit(this.data, index);
  }

  set(index, bit) {
    setBit(this.data, index, bit);
  }

  has(key, depth) {
    return this.count(key, depth) === this.size;
  }

  count(key, depth) {
    return this._count(0, key, depth);
  }

  _count(index, key, depth) {
    const x = this.size - index;
    const y = (key.length * 8) - depth;
    const len = Math.min(x, y);

    let bits = 0;

    for (let i = 0; i < len; i++) {
      if (this.get(index) !== hasBit(key, depth))
        break;

      index += 1;
      depth += 1;
      bits += 1;
    }

    return bits;
  }

  slice(start, end) {
    const size = end - start;

    if (size === 0)
      return Bits.EMPTY;

    const bits = this.constructor.alloc(size);

    for (let i = start, j = 0; i < end; i++, j++)
      bits.set(j, this.get(i));

    return bits;
  }

  split(index) {
    return [
      this.slice(0, index),
      this.slice(index + 1, this.size)
    ];
  }

  collide(key, depth) {
    const size = this._count(depth, key, depth);
    return this.slice(depth, depth + size);
  }

  join(bits, bit) {
    const size = this.size + bits.size + 1;
    const out = this.constructor.alloc(size);

    this.data.copy(out.data, 0);

    out.set(this.size, bit);

    for (let i = 0, j = this.size + 1; i < bits.size; i++, j++)
      out.set(j, bits.get(i));

    return out;
  }

  getSize() {
    let size = 0;

    if (this.size >= 0x80)
      size += 1;

    size += 1;
    size += this.data.length;

    return size;
  }

  write(data, off) {
    checkWrite(off + 2 <= data.length, off);

    if (this.size >= 0x80) {
      data[off] = 0x80 | (this.size >>> 8);
      off += 1;
    }

    data[off] = this.size;
    off += 1;

    checkWrite(off + this.data.length <= data.length, off);

    off += this.data.copy(data, off);

    return off;
  }

  read(data, off) {
    checkRead(off + 2 <= data.length, off);

    let size = data[off];
    off += 1;

    if (size & 0x80) {
      size -= 0x80;
      size *= 0x100;
      size += data[off];
      off += 1;
    }

    const bytes = (size + 7) >>> 3;

    checkRead(off + bytes <= data.length, off);

    this.size = size;
    this.data = Buffer.allocUnsafe(bytes);

    data.copy(this.data, 0, off, off + bytes);

    return this;
  }

  decode(data) {
    return this.read(data, 0);
  }

  alloc(size) {
    assert((size >>> 0) === size);
    this.size = size;
    this.data = Buffer.allocUnsafe((size + 7) >>> 3);
    this.data.fill(0x00);
    return this;
  }

  from(data) {
    assert(Buffer.isBuffer(data));
    this.size = data.length * 8;
    this.data = data;
    return this;
  }

  toString() {
    let str = '';

    for (let i = 0; i < this.size; i++)
      str += this.get(i);

    return str;
  }

  fromString(str) {
    assert(typeof str === 'string');

    this.size = str.length;
    this.data = Buffer.allocUnsafe((this.size + 7) >>> 3);
    this.data.fill(0x00);

    for (let i = 0; i < str.length; i++) {
      const ch = str.charCodeAt(i) - 0x30;

      assert(ch >= 0 && ch <= 1);

      this.set(i, ch);
    }

    return this;
  }

  static read(data, off) {
    return new this().read(data, off);
  }

  static decode(data) {
    return new this().decode(data);
  }

  static alloc(size) {
    return new this().alloc(size);
  }

  static from(data) {
    return new this().from(data);
  }

  static fromString(str) {
    return new this().fromString(str);
  }
}

/*
 * Static
 */

Bits.EMPTY = new Bits();

/*
 * Helpers
 */

function checkWrite(ok, offset, start) {
  if (!ok) {
    throw new EncodingError(offset,
      'Out of bounds write',
      start || checkWrite);
  }
}

function checkRead(ok, offset, start) {
  if (!ok) {
    throw new EncodingError(offset,
      'Out of bounds read',
      start || checkRead);
  }
}

/*
 * Expose
 */

module.exports = Bits;
