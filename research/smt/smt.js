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

/* eslint no-use-before-define: "off" */

'use strict';

const assert = require('assert');
const LRU = require('blru');
const {ensureHash} = require('../../lib/common');

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
const PROOF_UNEXPECTED_NODE = 3;
const PROOF_EARLY_END = 4;

const defMap = new WeakMap();

/**
 * SMT
 */

class SMT {
  /**
   * Create a sparse merkle tree.
   * @constructor
   * @param {Object} hash
   * @param {Number} bits
   * @param {Object} db
   * @param {Number} [mode=0]
   * @param {Number} [size=100000]
   */

  constructor(hash, bits, db, mode, size) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(db && typeof db === 'object');

    this.hash = ensureHash(hash);
    this.bits = bits;
    this.db = db || null;
    this.defaults = getDefaults(this.hash, this.bits);
    this.cache = new Cache(this.defaults, mode, size);
    this.base = Buffer.alloc(this.bits >>> 3, 0x00);
    this.root = this.defaults[0];
    this.context = null;
  }

  isKey(key) {
    if (!Buffer.isBuffer(key))
      return false;
    return key.length === (this.bits >>> 3);
  }

  isHash(hash) {
    if (!Buffer.isBuffer(hash))
      return false;
    return hash.length === this.hash.size;
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
    this.root = await this.readNode(0, this.base);
  }

  async close() {
    this.root = this.defaults[0];
  }

  async get(key) {
    assert(this.isKey(key));
    return this.db.get(key);
  }

  async readNode(depth, base) {
    const cached = this.cache.get(depth, base);

    if (cached)
      return cached;

    const key = nodeKey(depth, base);
    const hash = await this.db.get(key);

    if (!hash)
      return this.defaults[depth];

    return hash;
  }

  writeNode(batch, depth, base, hash) {
    if (!batch)
      return;

    const key = nodeKey(depth, base);

    if (hash.equals(this.defaults[depth]))
      batch.del(key);
    else
      batch.put(key, hash);
  }

  queue() {
    return new SMTQueue(this);
  }

  async update(batch, items, depth, base) {
    if (items.length === 0)
      return this.readNode(depth, base);

    if (depth === this.bits) {
      if (items.length !== 1)
        throw new Error('Unsorted or duplicate items.');

      const hash = items[0].leaf;

      this.writeNode(batch, depth, base, hash);

      if (batch) {
        const {key, value} = items[0];
        if (value)
          batch.put(key, value);
        else
          batch.del(key);
      }

      return hash;
    }

    const split = splitBits(base, depth);
    const [l, r] = splitSet(items, split);

    const left = await this.update(batch, l, depth + 1, base);
    const right = await this.update(batch, r, depth + 1, split);
    const hash = this.hashInternal(left, right);

    this.writeNode(batch, depth, base, hash);

    this.cache.insert(depth, base, hash, split, left, right);

    return hash;
  }

  rootHash(enc) {
    assert(!enc || typeof enc === 'string');

    if (enc === 'hex')
      return this.root.toString('hex');

    return this.root;
  }

  async commit(queue, batch) {
    assert(queue instanceof SMTQueue);
    assert(batch && typeof batch.put === 'function');

    this.root = await this.update(batch, queue.items, 0, this.base);

    return this.root;
  }

  async prove(key) {
    assert(this.isKey(key));

    const root = this.rootHash();
    const nodes = await this.path(key, 0, this.base);
    const value = await this.db.get(key);

    return new Proof(this.hash, this.bits, root, nodes, value);
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
    return proofs.verify(this.hash, this.bits, key, proof);
  }

  static get proof() {
    return proofs;
  }

  static get Proof() {
    return Proof;
  }
}

/**
 * SMTQueue
 */

class SMTQueue {
  constructor(smt) {
    this.smt = smt;
    this.items = [];
  }

  putLeaf(key, leaf, value) {
    assert(this.smt.isKey(key));
    assert(this.smt.isHash(leaf));
    assert(value === null || Buffer.isBuffer(value));

    const item = { key, leaf, value };
    const i = binarySearch(this.items, item.key);

    if (i < this.items.length) {
      const p = this.items[i];

      if (p.key.equals(item.key)) {
        p.leaf = item.leaf;
        return this;
      }
    }

    switch (i) {
      case 0:
        this.items.unshift(item);
        break;
      case this.items.length:
        this.items.push(item);
        break;
      default:
        this.items.splice(i, 0, item);
        break;
    }

    return this;
  }

  put(key, value) {
    const leaf = this.smt.hashLeaf(value);
    return this.putLeaf(key, leaf, value);
  }

  del(key) {
    return this.putLeaf(key, this.smt.hash.zero, null);
  }

  clear() {
    this.items.length = 0;
    return this;
  }

  async write(batch) {
    const root = await this.smt.commit(this, batch);
    this.items.length = 0;
    return root;
  }

  insert(key, value) {
    return this.put(key, value);
  }

  remove(key) {
    return this.del(key);
  }

  async commit(batch) {
    return this.write(batch);
  }
}

/**
 * Verifier
 */

class Verifier {
  constructor(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    this.hash = ensureHash(hash);
    this.context = this.hash.hash();
    this.bits = bits;
    this.base = Buffer.alloc(this.bits >>> 3, 0x00);
  }

  hashLeaf(value) {
    return hashLeaf(this.context, value);
  }

  hashInternal(left, right) {
    return hashInternal(this.context, left, right);
  }

  verify(key, proof) {
    assert(Buffer.isBuffer(key));
    assert(key.length === (this.bits >>> 3));
    assert(proof instanceof Proof);

    if (proof.nodes.length < this.bits)
      return [PROOF_EARLY_END, null];

    if (proof.nodes.length > this.bits)
      return [PROOF_UNEXPECTED_NODE, null];

    let next = proof.value
      ? this.hashLeaf(proof.value)
      : this.hash.zero;

    let depth = this.bits - 1;

    for (let i = 0; i < this.bits; i++) {
      const node = proof.nodes[i];

      if (hasBit(key, depth))
        next = this.hashInternal(node, next);
      else
        next = this.hashInternal(next, node);

      depth -= 1;
    }

    if (!proof.root.equals(next))
      return [PROOF_HASH_MISMATCH, null];

    return [PROOF_OK, proof.value];
  }

  verifyRecursive(key, proof) {
    assert(Buffer.isBuffer(key));
    assert(key.length === (this.bits >>> 3));
    assert(proof instanceof Proof);

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
 * Proof
 */

class Proof {
  constructor(hash, bits, root, nodes, value) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    this.root = getDefaults(hash, bits)[0];
    this.nodes = [];
    this.value = null;
    this.from(root, nodes, value);
  }

  from(root, nodes, value) {
    if (root != null) {
      assert(Buffer.isBuffer(root));
      this.root = root;
    }

    if (nodes != null) {
      assert(Array.isArray(nodes));
      this.nodes = nodes;
    }

    if (value != null) {
      assert(Buffer.isBuffer(value));
      this.value = value;
    }

    return this;
  }

  getSize(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    const defaults = getDefaults(hash, bits);

    let size = 0;

    size += hash.size;
    size += (this.nodes.length + 7) / 8 | 0;

    let depth = bits - 1;

    for (const node of this.nodes) {
      if (!node.equals(defaults[depth]))
        size += node.length;
      depth -= 1;
    }

    size += 2;

    if (this.value)
      size += this.value.length;

    return size;
  }

  encode(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    const size = this.getSize(hash, bits);
    const defaults = getDefaults(hash, bits);
    const data = Buffer.alloc(size);

    let pos = 0;

    pos += this.root.copy(data, 0);

    assert(this.nodes.length === bits);

    // data.fill(0x00, pos, pos + (bits >>> 3));

    pos += bits >>> 3;

    let depth = bits - 1;

    for (let i = 0; i < this.nodes.length; i++) {
      const node = this.nodes[i];

      if (node.equals(defaults[depth]))
        setBit(data, hash.size * 8 + i);
      else
        pos += node.copy(data, pos);

      depth -= 1;
    }

    const len = this.value ? this.value.length + 1 : 0;

    data[pos] = len & 0xff;
    pos += 1;
    data[pos] |= len >>> 8;
    pos += 1;

    if (this.value)
      pos += this.value.copy(data, pos);

    assert(pos === data.length);

    return data;
  }

  decode(data, hash, bits) {
    assert(Buffer.isBuffer(data));
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    let pos = 0;

    assert(pos + hash.size <= data.length);

    this.root = data.slice(pos, pos + hash.size);

    pos += hash.size;
    pos += bits >>> 3;

    assert(pos <= data.length);

    const defaults = getDefaults(hash, bits);

    let depth = bits - 1;

    for (let i = 0; i < bits; i++) {
      if (hasBit(data, hash.size * 8 + i)) {
        this.nodes.push(defaults[depth]);
      } else {
        assert(pos + hash.size <= data.length);
        const h = data.slice(pos, pos + hash.size);
        this.nodes.push(h);
        pos += hash.size;
      }

      depth -= 1;
    }

    assert(pos + 2 <= data.length);

    let size = 0;
    size |= data[pos];
    size |= data[pos + 1] << 8;
    pos += 2;

    if (size > 0) {
      size -= 1;
      assert(pos + size <= data.length);
      this.value = data.slice(pos, pos + size);
      pos += size;
    }

    return this;
  }

  static decode(data, hash, bits) {
    return new this(hash, bits).decode(data, hash, bits);
  }
}

/**
 * Proofs
 */

const proofs = {};

proofs.prove = async function prove(tree, key) {
  assert(tree instanceof SMT);
  return tree.prove(key);
};

proofs.verify = function verify(hash, bits, key, proof) {
  const v = new Verifier(hash, bits);
  return v.verify(key, proof);
};

/**
 * Cache
 */

class Cache {
  constructor(defaults, mode, size, chance) {
    if (mode == null)
      mode = 0;

    if (size == null)
      size = 100000;

    if (chance == null)
      chance = 0.6;

    assert(Array.isArray(defaults));
    assert((mode >>> 0) === mode);
    assert(mode <= 2);
    assert((size >>> 0) === size);
    assert(typeof chance === 'number');

    this.mode = mode;
    this.defaults = defaults;
    this.map = new LRU(size);
    this.chance = chance;
  }

  get size() {
    return this.map.size;
  }

  has(depth, base) {
    if (this.mode === 0)
      return false;
    return this.map.has(depth + base.toString('hex'));
  }

  get(depth, base) {
    if (this.mode === 0)
      return null;
    return this.map.get(depth + base.toString('hex'));
  }

  set(depth, base, value) {
    if (this.mode === 0)
      return this;
    this.map.set(depth + base.toString('hex'), value);
    return this;
  }

  delete(depth, base) {
    if (this.mode === 0)
      return this;
    this.map.remove(depth + base.toString('hex'));
    return this;
  }

  insert(depth, base, hash, split, left, right) {
    const def = this.defaults[depth + 1];

    switch (this.mode) {
      case 0: // Nothing
        break;
      case 1: // Cache Branch
        if (!left.equals(def) && !right.equals(def))
          this.set(depth, base, hash);
        else
          this.delete(depth, base);
        break;
      case 2: // Cache Branch Plus
        if (!left.equals(def) && !right.equals(def)) {
          this.set(depth + 1, base, left);
          this.set(depth + 1, split, right);
        } else {
          this.delete(depth, base);
        }
        break;
      case 3: // Cache Branch Minus
        if (Math.random() < this.chance
            && !left.equals(def)
            && !right.equals(def)) {
          this.set(depth, base, hash);
        } else {
          this.delete(depth, base);
        }
        break;
    }

    return this;
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

function nodeKey(depth, base) {
  const key = Buffer.allocUnsafe(2 + base.length);
  key[0] = depth >>> 8;
  key[1] = depth & 0xff;
  base.copy(key, 2);
  return key;
}

function splitBits(base, index) {
  const split = Buffer.allocUnsafe(base.length);
  base.copy(split, 0);
  setBit(split, index);
  return split;
}

function binarySearch(items, key) {
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
  const index = binarySearch(items, split);
  const left = items.slice(0, index);
  const right = items.slice(index);
  return [left, right];
}

function getDefaults(hash, bits) {
  const cachedBM = defMap.get(hash);

  if (cachedBM) {
    const cachedDefs = cachedBM.get(bits);
    if (cachedDefs)
      return cachedDefs;
  }

  const defaults = computeDefaults(hash, bits);

  let bm = cachedBM;

  if (!bm) {
    bm = new Map();
    defMap.set(hash, bm);
  }

  bm.set(bits, defaults);

  return defaults;
}

function computeDefaults(hash, bits) {
  const nodes = bits + 1;
  const defaults = [];

  defaults.push(hash.zero);

  const ctx = hash.hash();

  for (let i = 1; i < nodes; i++) {
    const child = defaults[i - 1];
    const parent = hashInternal(ctx, child, child);
    defaults.push(parent);
  }

  return defaults.reverse();
}

/*
 * Expose
 */

module.exports = SMT;
