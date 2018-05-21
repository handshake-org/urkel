/*!
 * smt.js - smt tree
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 *
 * SMT Trees:
 */

'use strict';

const assert = require('assert');
const {ensureHash} = require('./common');

/*
 * Constants
 */

const INTERNAL = Buffer.from([0x00]);
const LEAF = Buffer.from([0x01]);

/*
 * Error Codes
 */

const PROOF_OK = 0;
const PROOF_HASH_MISMATCH = 1;
const PROOF_MALFORMED_NODE = 2;
const PROOF_UNEXPECTED_NODE = 3;
const PROOF_EARLY_END = 4;
const PROOF_NO_RESULT = 5;

const defMap = new WeakMap();

/**
 * SMT
 */

class SMT {
  /**
   * Create a smt tree.
   * @constructor
   * @param {Object} hash
   * @param {Object} [db=null]
   * @param {Number} [limit=100000]
   */

  constructor(hash, db, limit) {
    if (limit == null)
      limit = 500000;

    assert(hash && typeof hash.digest === 'function');
    assert(!db || typeof db === 'object');
    assert((limit >>> 0) === limit);

    this.hash = ensureHash(hash);
    this.db = db || null;
    this.bits = 160;
    this.base = Buffer.alloc(this.bits / 8, 0x00);
    this.defaults = getDefaults(this.hash, this.bits);
    this.originalRoot = this.defaults[0];
    this.cache = new Cache(limit);
    this.context = null;
    this.queue = [];
  }

  ctx() {
    if (!this.context)
      this.context = this.hash.hash();
    return this.context;
  }

  hashInternal(left, right) {
    return hashInternal(this.ctx(), left, right);
  }

  hashLeaf(value) {
    return hashLeaf(this.ctx(), value);
  }

  async open() {
    this.originalRoot = await this.hashRoot();
  }

  async close() {
    this.originalRoot = this.defaults[0];
  }

  async get(key) {
    return this.read(key);
  }

  insertLeaf(key, leaf, value) {
    const item = { key, leaf, value };
    const i = search(this.queue, item.key);

    if (i < this.queue.length) {
      const p = this.queue[i];

      if (p.key.equals(item.key)) {
        p.leaf = item.leaf;
        return;
      }
    }

    switch (i) {
      case 0:
        this.queue.unshift(item);
        break;
      case this.queue.length:
        this.queue.push(item);
        break;
      default:
        this.queue.splice(i, 0, item);
        break;
    }
  }

  insert(key, value) {
    const leaf = this.hashLeaf(value);
    return this.insertLeaf(key, leaf, value);
  }

  remove(key) {
    return this.insertLeaf(key, this.hash.zero, null);
  }

  async read(key) {
    const cache = this.cache.get(key);

    if (cache === undefined)
      return null;

    if (cache)
      return cache;

    if (!this.db)
      return null;

    const value = await this.db.get(key);

    if (!value)
      return null;

    this.cache.set(key, value);

    return value;
  }

  async readNode(depth, base) {
    const key = nodeKey(depth, base);
    const h = await this.read(key);

    if (!h)
      return this.defaults[depth];

    return h;
  }

  writeNode(depth, base, hash) {
    const key = nodeKey(depth, base);

    if (hash.equals(this.defaults[depth])) {
      this.cache.del(key);
      return;
    }

    this.cache.put(key, hash);
  }

  async hashRoot() {
    const root = await this._hashRoot(this.queue, 0, this.base);

    for (const {key, value} of this.queue) {
      if (value)
        this.cache.put(key, value);
      else
        this.cache.del(key);
    }

    this.queue.length = 0;

    return root;
  }

  async _hashRoot(items, depth, base) {
    if (depth === this.bits) {
      if (items.length === 0)
        return this.readNode(depth, base);

      if (items.length === 1) {
        const h = items[0].leaf;
        this.writeNode(depth, base, h);
        return h;
      }

      throw new Error('Unsorted or duplicate items.');
    }

    if (items.length === 0)
      return this.readNode(depth, base);

    const split = splitBits(base, depth);
    const [l, r] = splitSet(items, split);

    const left = await this._hashRoot(l, depth + 1, base);
    const right = await this._hashRoot(r, depth + 1, split);
    const h = this.hashInternal(left, right);

    this.writeNode(depth, base, h);

    return h;
  }

  async rootHash(enc) {
    const root = await this._hashRoot([], 0, this.base);

    if (enc === 'hex')
      return root.toString('hex');

    return root;
  }

  async commit(batch, enc) {
    assert(batch);

    const root = await this.hashRoot();

    // Commit tree.
    this.cache.commit(batch);

    this.originalRoot = root;

    if (enc === 'hex')
      return root.toString('hex');

    return root;
  }

  async prove(key) {
    const root = await this.rootHash();
    const nodes = await this.path(key, 0, this.base);
    const value = await this.read(key);

    return {
      root,
      nodes,
      value
    };
  }

  async path(key, depth, base) {
    if (depth === this.bits)
      return [];

    const split = splitBits(base, depth);

    if (!hasBit(key, depth)) {
      const path = await this.path(key, depth + 1, base);
      const hash = await this.readNode(depth + 1, split);
      // path.unshift(hash);
      path.push(hash);
      return path;
    }

    const path = await this.path(key, depth + 1, split);
    const hash = await this.readNode(depth + 1, base);
    // path.unshift(hash);
    path.push(hash);
    return path;
  }

  verify(key, proof) {
    return proofs.verify(this.hash, key, proof);
  }

  static get proof() {
    return proofs;
  }
}

/**
 * Proofs
 */

const proofs = {};

proofs.prove = async function prove(tree, key) {
  return tree.prove(key);
};

proofs.verify = function verify(hash, key, proof) {
  const bits = key.length * 8;

  if (proof.nodes.length < bits)
    return [PROOF_EARLY_END, null];

  if (proof.nodes.length > bits)
    return [PROOF_UNEXPECTED_NODE, null];

  const ctx = hash.hash();

  let next = proof.value
    ? hashLeaf(ctx, proof.value)
    : hash.zero;

  // Traverse bits right to left.
  let depth = bits - 1;

  for (let i = 0; i < bits; i++) {
    const node = proof.nodes[i];

    if (hasBit(key, depth))
      next = hashInternal(ctx, node, next);
    else
      next = hashInternal(ctx, next, node);

    depth -= 1;
  }

  if (!proof.root.equals(next))
    return [PROOF_HASH_MISMATCH, null];

  return [PROOF_OK, proof.value];
};

proofs.verify = function verify(hash, key, proof) {
  const v = new Verifier(hash, key.length * 8);
  return v.verify(key, proof);
};

/**
 * Verifier
 */

class Verifier {
  constructor(hash, bits) {
    this.hash = ensureHash(hash);
    this.ctx = this.hash.hash();
    this.bits = bits;
    this.base = Buffer.alloc(this.bits / 8, 0x00);
  }

  hashLeaf(value) {
    return hashLeaf(this.ctx, value);
  }

  hashInternal(left, right) {
    return hashInternal(this.ctx, left, right);
  }

  verify(key, proof) {
    const {root, nodes, value} = proof;

    if (nodes.length < this.bits)
      return [PROOF_EARLY_END, null];

    if (nodes.length > this.bits)
      return [PROOF_UNEXPECTED_NODE, null];

    const calc = this.calc(nodes, 0, this.base, key, value);

    if (!root.equals(calc))
      return [PROOF_HASH_MISMATCH, null];

    return [PROOF_OK, proof.value];
  }

  calc(nodes, depth, base, key, value) {
    const height = this.bits - depth;

    if (depth === this.bits)
      return value ? this.hashLeaf(value) : this.hash.zero;

    if (!hasBit(key, depth)) {
      const hash = this.calc(nodes, depth + 1, base, key, value);
      // return this.hashInternal(hash, nodes[depth]);
      return this.hashInternal(hash, nodes[height - 1]);
    }

    const split = splitBits(base, depth);
    const hash = this.calc(nodes, depth + 1, split, key, value);
    // return this.hashInternal(nodes[depth], hash);
    return this.hashInternal(nodes[height - 1], hash);
  }
}

/**
 * Cache
 */

class Cache {
  constructor(size) {
    assert((size >>> 0) === size);
    this.map = new Map();
    this.lru = new Map();
  }

  get(key) {
    const hex = key.toString('hex');
    const pending = this.map.get(hex);

    if (pending)
      return pending[1] || undefined;

    const cached = this.lru.get(hex);

    if (cached)
      return cached;

    return null;
  }

  set(key, value) {
    this.lru.set(key.toString('hex'), value);
    return this;
  }

  remove(key) {
    this.lru.delete(key.toString('hex'));
    return this;
  }

  put(key, value) {
    this.map.set(key.toString('hex'), [key, value]);
    return this;
  }

  del(key) {
    this.map.set(key.toString('hex'), [key, null]);
    return this;
  }

  commit(batch) {
    for (const [key, value] of this.map.values()) {
      if (value)
        batch.put(key, value);
      else
        batch.del(key);
    }

    this.map.clear();
    this.lru.clear();

    return this;
  }
}

/**
 * Assertion Error
 */

class AssertionError extends assert.AssertionError {
  constructor(message) {
    super({ message });
  }
}

/*
 * Helpers
 */

function hasBit(key, index) {
  const oct = index >>> 3;
  const bit = index & 7;
  return (key[oct] >>> (7 - bit)) & 1;
}

function setBit(key, index) {
  const oct = index >>> 3;
  const bit = index & 7;
  key[oct] |= 1 << (7 - bit);
}

function hashInternal(ctx, left, right) {
  ctx.init();
  ctx.update(INTERNAL);
  ctx.update(left);
  ctx.update(right);
  return ctx.final();
}

function hashLeaf(ctx, value) {
  ctx.init();
  ctx.update(LEAF);
  ctx.update(value);
  return ctx.final();
}

function nodeKey(height, base) {
  assert((height & 0xffff) === height);
  assert(Buffer.isBuffer(base));

  const key = Buffer.allocUnsafe(2 + base.length);
  key[0] = height >>> 8;
  key[1] = height & 0xff;
  base.copy(key, 2);
  return key;
}

function splitBits(base, index) {
  assert(Buffer.isBuffer(base));
  assert((index & 0xffff) === index);
  const split = Buffer.allocUnsafe(base.length);
  base.copy(split, 0);
  setBit(split, index);
  return split;
}

function search(items, key) {
  assert(Array.isArray(items));
  assert(items.length === 0 || (items[0] && Buffer.isBuffer(items[0].key)));
  assert(Buffer.isBuffer(key));

  let start = 0;
  let end = items.length - 1;

  while (start <= end) {
    const pos = (start + end) >>> 1;
    const cmp = items[pos].key.compare(key);

    if (cmp === 0)
      return pos;

    if (cmp < 0)
      start = pos + 1;
    else
      end = pos - 1;
  }

  return start;
}

function splitSet(items, split) {
  const index = search(items, split);
  const left = items.slice(0, index);
  const right = items.slice(index);

  if (left.length > 0) {
    const l = left[0];
    assert(l.key.compare(split) < 0);
  }

  if (right.length > 0) {
    const r = right[0];
    assert(r.key.compare(split) >= 0);
  }

  return [left, right];
}

function compare(a, b) {
  return a.key.compare(b.key);
}

function sortSet(items) {
  assert(Array.isArray(items));
  return items.sort(compare);
}

function getDefaults(hash, bits) {
  const cachedBM = defMap.get(hash);

  if (cachedBM) {
    const cachedDefs = cachedBM.get(bits);
    if (cachedDefs)
      return cachedDefs;
  }

  const defs = computeDefaults(hash, bits);

  let bm = cachedBM;

  if (!bm) {
    bm = new Map();
    defMap.set(hash, bm);
  }

  bm.set(bits, defs);

  return defs;
}

function computeDefaults(hash, bits) {
  const nodes = bits + 1;
  const defs = [];

  defs.push(hash.zero);

  for (let i = 1; i < nodes; i++) {
    const h = defs[i - 1];
    defs.push(hash.root(h, h));
  }

  return defs.reverse();
}

/*
 * Expose
 */

module.exports = SMT;

/*
(async () => {
  const sha256 = require('bcrypto/lib/sha256');
  const crypto = require('crypto');
  const smt = new SMT(sha256);
  const items = [];

  for (let i = 0; i < 500; i++)
    items.push([crypto.randomBytes(20), crypto.randomBytes(32)]);

  let now = Date.now();

  for (const [key, value] of items)
    smt.insert(key, value);

  console.log(Date.now() - now);

  now = Date.now();
  const root = await smt.hashRoot();
  console.log(Date.now() - now);

  now = Date.now();
  await smt.hashRoot();
  console.log(Date.now() - now);

  console.log(root);

  const key = items[Math.random() * items.length | 0][0];
  const proof = await smt.prove(key);
  const [code, value] = smt.verify(key, proof);
  assert(code === 0);
  assert(value !== null);
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
*/
