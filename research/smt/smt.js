/*!
 * smt.js - sparse merkle tree
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 *
 * Sparse Merkle Trees:
 *   https://eprint.iacr.org/2016/683
 *
 * Parts of this software are based on gosmt:
 *   https://github.com/pylls/gosmt
 *
 * See Also:
 *   https://github.com/google/keytransparency/tree/master/core/tree
 *   https://github.com/google/trillian/blob/master/merkle/hstar2.go
 *   https://github.com/google/trillian/blob/master/merkle/sparse_merkle_tree.go
 */

'use strict';

const assert = require('assert');
const sha256 = require('bcrypto/lib/sha256');

/*
 * Constants
 */

const EMPTY = Buffer.from([0x00]);
const SET = Buffer.from([0x01]);
const TWO = Buffer.allocUnsafe(2);

/**
 * SMT
 */

class SMT {
  constructor(constant, hash) {
    if (constant == null)
      constant = Buffer.from([0x42]);

    if (hash == null)
      hash = sha256;

    assert(Buffer.isBuffer(constant));
    assert(hash);
    assert(typeof hash.digest === 'function');
    assert(typeof hash.root === 'function');

    this.hash = hash;
    this.bytes = hash.digest(EMPTY).length;
    this.bits = this.bytes << 3;
    this.cache = new Cache();

    this.func = this.interiorHash.bind(this);
    this.constant = hash.digest(constant);
    this.zero = Buffer.alloc(this.bytes, 0x00);
    this.defaults = [];
    this.slab = Buffer.allocUnsafe(3 * this.bytes + 2);

    this.set = SET;
    this.empty = EMPTY;

    this.init();
  }

  init() {
    this.defaults.push(this.leafHash(EMPTY, this.zero));

    for (let i = 1; i < this.bits; i++) {
      const last = this.defaults[i - 1];
      this.defaults.push(this.hash.root(last, last));
    }
  }

  isHash(hash) {
    return Buffer.isBuffer(hash) && hash.length === this.bytes;
  }

  isHashes(hashes) {
    if (!Array.isArray(hashes))
      return false;

    if (hashes.length === 0)
      return true;

    return this.isHash(hashes[0]);
  }

  quadHash(a, b, c, d) {
    assert(this.isHash(a));
    assert(this.isHash(b));
    assert(this.isHash(c));
    assert(Buffer.isBuffer(d) && d.length === 2);

    const s = this.slab;
    a.copy(s, this.bytes * 0);
    b.copy(s, this.bytes * 1);
    c.copy(s, this.bytes * 2);
    d.copy(s, this.bytes * 3);

    return this.hash.digest(s);
  }

  defaultHash(height) {
    assert((height & 0xffff) === height);
    assert(height < this.defaults.length);
    return this.defaults[height];
  }

  leafHash(value, base) {
    assert(Buffer.isBuffer(value));
    assert(this.isHash(base));

    if (value.equals(EMPTY))
      return this.hash.digest(this.constant);

    return this.hash.root(this.constant, base);
  }

  interiorHash(left, right, height, base) {
    assert(this.isHash(left));
    assert(this.isHash(right));
    assert((height & 0xffff) === height);
    assert(this.isHash(base));

    if (left.equals(right))
      return this.hash.root(left, right);

    const buf = TWO;
    buf[0] = height >>> 8;
    buf[1] = height & 0xff;

    return this.quadHash(left, right, base, buf);
  }

  async rootHash(db, height, base) {
    assert(this.isHashes(db));
    assert((height & 0xffff) === height);
    assert(this.isHash(base));

    const cache = this.cache.get(height, base);

    if (cache)
      return cache;

    if (db.length === 0)
      return this.defaultHash(height);

    if (db.length === 1 && height === 0)
      return this.leafHash(SET, base);

    if (db.length > 0 && height === 0)
      throw new Error('Unsorted data or broken split.');

    const split = splitBits(base, this.bits - height);
    const [ld, rd] = splitSet(db, split);

    return this.interiorHash(
      await this.rootHash(ld, height - 1, base),
      await this.rootHash(rd, height - 1, split),
      height,
      base
    );
  }

  hashCache(left, right, height, base, split) {
    assert(this.isHash(left));
    assert(this.isHash(right));
    assert((height & 0xffff) === height);
    assert(this.isHash(base));
    assert(this.isHash(split));

    const func = this.func;
    const defs = this.defaults;

    return this.cache.hash(
      left,
      right,
      height,
      base,
      split,
      func,
      defs
    );
  }

  async update(db, keys, height, base, value) {
    assert(this.isHashes(db));
    assert(this.isHashes(keys));
    assert((height & 0xffff) === height);
    assert(this.isHash(base));
    assert(Buffer.isBuffer(value));

    if (height === 0)
      return this.leafHash(value, base);

    const split = splitBits(base, this.bits - height);
    const [ld, rd] = splitSet(db, split);
    const [left, right] = splitSet(keys, split);

    if (left.length === 0 && right.length > 0) {
      return this.hashCache(
        await this.rootHash(ld, height - 1, base),
        await this.update(rd, keys, height - 1, split, value),
        height,
        base,
        split
      );
    }

    if (left.length > 0 && right.length === 0) {
      return this.hashCache(
        await this.update(ld, keys, height - 1, base, value),
        await this.rootHash(rd, height - 1, split),
        height,
        base,
        split
      );
    }

    return this.hashCache(
      await this.update(ld, left, height - 1, base, value),
      await this.update(rd, right, height - 1, split, value),
      height,
      base,
      split
    );
  }

  async auditPath(db, height, base, key) {
    assert(this.isHashes(db));
    assert((height & 0xffff) === height);
    assert(this.isHash(base));
    assert(this.isHash(key));

    if (height === 0)
      return [];

    const split = splitBits(base, this.bits - height);
    const [l, r] = splitSet(db, split);

    if (!hasBit(key, this.bits - height)) {
      const path = await this.auditPath(l, height - 1, base, key);
      const hash = await this.rootHash(r, height - 1, split);
      path.push(hash);
      return path;
    }

    const path = await this.auditPath(r, height - 1, split, key);
    const hash = await this.rootHash(l, height - 1, base);
    path.push(hash);
    return path;
  }

  verifyAuditPath(ap, key, value, root) {
    assert(this.isHashes(ap));
    assert(this.isHash(key));
    assert(Buffer.isBuffer(value));
    assert(this.isHash(root));

    const n = this.bits;
    const zero = this.zero;
    const calc = this.auditPathCalc(ap, n, zero, key, value);

    return root.equals(calc);
  }

  auditPathCalc(ap, height, base, key, value) {
    assert(this.isHashes(ap));
    assert((height & 0xffff) === height);
    assert(this.isHash(base));
    assert(this.isHash(key));
    assert(Buffer.isBuffer(value));

    if (height === 0)
      return this.leafHash(value, base);

    if (!hasBit(key, this.bits - height)) {
      const hash = this.auditPathCalc(ap, height - 1, base, key, value);
      return this.interiorHash(hash, ap[height - 1], height, base);
    }

    const split = splitBits(base, this.bits - height);
    const hash = this.auditPathCalc(ap, height - 1, split, key, value);
    return this.interiorHash(ap[height - 1], hash, height, base);
  }
}

/**
 * Cache
 */

class Cache {
  constructor() {
    this.map = new BufferMap();
    this.mode = 2;
    this.chance = 0.6;
  }

  exists(height, base) {
    return this.map.has(cacheKey(height, base));
  }

  get(height, base) {
    return this.map.get(cacheKey(height, base));
  }

  set(height, base, hash) {
    this.map.set(cacheKey(height, base), hash);
    return this;
  }

  delete(height, base) {
    this.map.delete(cacheKey(height, base));
    return this;
  }

  hash(left, right, height, base, split, func, defaults) {
    assert(Buffer.isBuffer(left));
    assert(Buffer.isBuffer(right));
    assert((height & 0xffff) === height);
    assert(Buffer.isBuffer(base));
    assert(Buffer.isBuffer(split));
    assert(typeof func === 'function');
    assert(Array.isArray(defaults) && defaults.length > 0);
    assert(Buffer.isBuffer(defaults[0]));
    assert(height !== 0);
    assert(height - 1 < defaults.length);

    const def = defaults[height - 1];
    assert(left.length === def.length);
    assert(right.length === def.length);
    assert(base.length === def.length);
    assert(split.length === def.length);

    const hash = func(left, right, height, base);
    assert(def.length === hash.length);

    switch (this.mode) {
      case 0: // Nothing
        break;
      case 1: // Cache Branch
        if (!left.equals(def) && !right.equals(def))
          this.set(height, base, hash);
        else
          this.delete(height, base);
        break;
      case 2: // Cache Branch Plus
        if (!left.equals(def) && !right.equals(def)) {
          this.set(height - 1, base, left);
          this.set(height - 1, split, right);
        } else {
          this.delete(height, base);
        }
        break;
      case 3: // Cache Branch Minus
        if (Math.random() < this.chance
            && !left.equals(def)
            && !right.equals(def)) {
          this.set(height, base, hash);
        } else {
          this.delete(height, base);
        }
        break;
    }

    return hash;
  }

  inspect() {
    const json = {};
    for (const [key, value] of this.map.values()) {
      const height = key.readUInt16BE(0, true);
      const base = key.toString('hex', 2);
      const hash = value.toString('hex');
      json[`${height}/${base}`] = hash;
    }
    return json;
  }
}

/**
 * Buffer Map
 */

class BufferMap {
  constructor() {
    this.map = new Map();
  }

  get size() {
    return this.map.size;
  }

  has(key) {
    assert(Buffer.isBuffer(key));
    return this.map.has(key.toString('hex'));
  }

  get(key) {
    assert(Buffer.isBuffer(key));

    const item = this.map.get(key.toString('hex'));

    if (!item)
      return null;

    return item[1];
  }

  set(key, value) {
    assert(Buffer.isBuffer(key));
    assert(Buffer.isBuffer(value));
    this.map.set(key.toString('hex'), [key, value]);
    return this;
  }

  delete(key) {
    assert(Buffer.isBuffer(key));
    this.map.delete(key.toString('hex'));
    return this;
  }

  [Symbol.iterator]() {
    return this.map[Symbol.iterator]();
  }

  keys() {
    return this.map.keys();
  }

  values() {
    return this.map.values();
  }
}

/*
 * Helpers
 */

function cacheKey(height, base) {
  assert((height & 0xffff) === height);
  assert(Buffer.isBuffer(base));

  const key = Buffer.allocUnsafe(2 + base.length);
  key[0] = height >>> 8;
  key[1] = height & 0xff;
  base.copy(key, 2);
  return key;
}

function hasBit(key, index) {
  assert(Buffer.isBuffer(key));
  assert((index & 0xffff) === index);
  const oct = index >>> 3;
  const bit = index & 7;
  return (key[oct] >>> (7 - bit)) & 1;
}

function setBit(key, index) {
  assert(Buffer.isBuffer(key));
  assert((index & 0xffff) === index);
  const oct = index >>> 3;
  const bit = index & 7;
  key[oct] |= 1 << (7 - bit);
}

function splitBits(key, index) {
  assert(Buffer.isBuffer(key));
  assert((index & 0xffff) === index);
  const split = Buffer.allocUnsafe(key.length);
  key.copy(split, 0);
  setBit(split, index);
  return split;
}

function search(items, key) {
  assert(Array.isArray(items));
  assert(items.length === 0 || Buffer.isBuffer(items[0]));
  assert(Buffer.isBuffer(key));

  let start = 0;
  let end = items.length - 1;

  while (start <= end) {
    const pos = (start + end) >>> 1;
    const cmp = items[pos].compare(key);

    if (cmp === 0)
      return pos;

    if (cmp < 0)
      start = pos + 1;
    else
      end = pos - 1;
  }

  return start;
}

function splitSet(items, item) {
  const index = search(items, item);
  const left = items.slice(0, index);
  const right = items.slice(index);

  if (left.length > 0) {
    const l = left[0];
    assert(l.compare(item) < 0);
  }

  if (right.length > 0) {
    const r = right[0];
    assert(r.compare(item) >= 0);
  }

  return [left, right];
}

/*
 * Expose
 */

module.exports = SMT;
