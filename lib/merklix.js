/*!
 * merklix.js - merklix tree
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 *
 * Merklix Trees:
 *   https://www.deadalnix.me/2016/09/24/introducing-merklix-tree-as-an-unordered-merkle-tree-on-steroid/
 *   https://www.deadalnix.me/2016/09/29/using-merklix-tree-to-checkpoint-an-utxo-set/
 */

'use strict';

const assert = require('assert');
const LRU = require('blru');
const {ensureHash} = require('./common');

/*
 * Constants
 */

const STATE_KEY = Buffer.from([0x73]);
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

/**
 * Merklix
 */

class Merklix {
  /**
   * Create a merklix tree.
   * @constructor
   * @param {Object} hash
   * @param {Number} bits
   * @param {Object} [db=null]
   * @param {Number} [limit=100000]
   * @param {Number} [prune=false]
   */

  constructor(hash, bits, db, limit, prune) {
    if (limit == null)
      limit = 500000;

    if (prune == null)
      prune = false;

    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(!db || typeof db === 'object');
    assert((limit >>> 0) === limit);

    this.hash = ensureHash(hash);
    this.bits = bits;
    this.db = db || null;
    this.originalRoot = this.hash.zero;
    this.root = this.hash.zero;
    this.cache = new LRUCache(limit);
    this.context = null;
    this.pruner = new PruneList(prune);
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

  hashLeaf(key, value) {
    return hashLeaf(this.ctx(), key, value);
  }

  async open(root) {
    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    assert(!root || Buffer.isBuffer(root));

    // Try to retrieve best state.
    if (!root && this.db)
      root = await this.db.get(STATE_KEY);

    if (root && !root.equals(this.hash.zero)) {
      assert(root.length === this.hash.size);

      if (!this.db)
        throw new Error('Cannot use root without database.');

      if (!await this.db.has(root)) {
        throw new MissingNodeError({
          rootHash: root,
          nodeHash: root
        });
      }

      this.originalRoot = root;
      this.root = root;
    }
  }

  async close() {
    this.originalRoot = this.hash.zero;
    this.root = this.hash.zero;
  }

  async read(hash) {
    const cached = this.cache.get(hash);

    if (cached)
      return cached;

    const raw = await this.db.get(hash);

    if (!raw)
      return null;

    this.cache.set(hash, raw);

    return raw;
  }

  async readNode(hash) {
    if (hash.equals(this.hash.zero))
      return null;

    const raw = await this.read(hash);

    if (!raw)
      return null;

    if (raw.length === 0)
      throw new AssertionError('Database corruption.');

    if (raw.length !== sizeNode(raw[0], this.hash.size))
      throw new AssertionError('Database corruption.');

    const $33 = 1 + this.hash.size;
    const $65 = 1 + this.hash.size * 2;

    switch (raw[0]) {
      case 0: // 00
        return [this.hash.zero, this.hash.zero];
      case 1: // 01
        return [this.hash.zero, raw.slice(1, $33)];
      case 2: // 10
        return [raw.slice(1, $33), this.hash.zero];
      case 3: // 11
        return [raw.slice(1, $33), raw.slice($33, $65)];
      case 4:
        return [raw.slice(1)];
      default:
        throw new AssertionError('Database corruption.');
    }
  }

  putNode(hash, node) {
    const $33 = 1 + this.hash.size;

    switch (node.length) {
      case 2: {
        const [left, right] = node;

        let field = 0;
        field |= !left.equals(this.hash.zero) << 1;
        field |= !right.equals(this.hash.zero);

        const size = sizeNode(field, this.hash.size);
        assert(size !== -1);

        const val = Buffer.allocUnsafe(size);

        val[0] = field;

        switch (field) {
          case 0: // 00
            break;
          case 1: // 01
            right.copy(val, 1);
            break;
          case 2: // 10
            left.copy(val, 1);
            break;
          case 3: // 11
            left.copy(val, 1);
            right.copy(val, $33);
            break;
          default:
            assert(false);
            break;
        }

        this.cache.put(hash, val);
        this.pruner.unprune(hash);

        break;
      }

      case 1: {
        const [key] = node;
        const val = Buffer.allocUnsafe($33);
        val[0] = 0x04;
        key.copy(val, 1);
        this.cache.put(hash, val);
        this.pruner.unprune(hash);
        break;
      }

      default: {
        throw new TypeError('Node length must be 2 or 1.');
      }
    }
  }

  valueKey(leaf) {
    const key = Buffer.allocUnsafe(1 + this.hash.size);
    key[0] = 0x76;
    leaf.copy(key, 1);
    return key;
  }

  async readValue(leaf) {
    const key = this.valueKey(leaf);
    const cached = this.cache.get(key);

    if (cached)
      return cached;

    return this.db.get(key);
  }

  putValue(leaf, value) {
    const key = this.valueKey(leaf);
    this.cache.put(key, value);
    this.pruner.unprune(key);
  }

  async _get(root, key) {
    let next = root;
    let depth = 0;

    // Traverse bits left to right.
    for (;;) {
      if (depth === this.bits) {
        throw new MissingNodeError({
          rootHash: root,
          key,
          depth
        });
      }

      const node = await this.readNode(next);

      // Empty (sub)tree.
      if (!node) {
        if (!next.equals(this.hash.zero)) {
          throw new MissingNodeError({
            rootHash: root,
            nodeHash: next,
            key,
            depth
          });
        }

        next = null;
        break;
      }

      // Leaf node.
      if (node.length === 1) {
        // Prefix collision.
        if (!key.equals(node[0]))
          next = null;
        break;
      }

      // Internal node.
      const bit = hasBit(key, depth);
      next = node[bit];
      depth += 1;
    }

    if (!next)
      return null;

    return this.readValue(next);
  }

  async get(key) {
    assert(this.isKey(key));
    return this._get(this.root, key);
  }

  async _insert(root, key, value) {
    const leaf = this.hashLeaf(key, value);
    const nodes = [];
    const prune = [];

    let next = root;
    let depth = 0;

    // Traverse bits left to right.
    for (;;) {
      if (depth === this.bits) {
        throw new MissingNodeError({
          rootHash: root,
          key,
          depth
        });
      }

      const node = await this.readNode(next);

      // Empty (sub)tree.
      if (!node) {
        if (!next.equals(this.hash.zero)) {
          throw new MissingNodeError({
            rootHash: root,
            nodeHash: next,
            key,
            depth
          });
        }

        // Replace the empty node.
        depth -= 1;
        break;
      }

      // Leaf node.
      if (node.length === 1) {
        // Current key.
        const other = node[0];

        if (key.equals(other)) {
          // Exact leaf already exists.
          if (leaf.equals(next))
            return root;

          // Prune old nodes.
          if (this.pruner.enabled) {
            prune.push(next);
            // Value to remove.
            prune.push(this.valueKey(next));
          }

          // Uncache old nodes.
          this.cache.remove(next);

          // The branch doesn't grow.
          // Replace the current node.
          depth -= 1;

          break;
        }

        // Insert placeholder leaves to grow
        // the branch if we have bit collisions.
        while (hasBit(key, depth) === hasBit(other, depth)) {
          // Child-less sidenode.
          depth += 1;
          nodes.push(this.hash.zero);
        }

        nodes.push(next);

        break;
      }

      // Prune old nodes.
      if (this.pruner.enabled)
        prune.push(next);

      // Uncache old nodes.
      this.cache.remove(next);

      // Internal node.
      const bit = hasBit(key, depth);
      nodes.push(node[bit ^ 1]);
      next = node[bit];
      depth += 1;
    }

    // Prune old nodes.
    for (const hash of prune)
      this.pruner.prune(hash);

    // Store the key for
    // comparisons later (see above).
    this.putNode(leaf, [key]);

    // Start at the leaf.
    next = leaf;

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      if (hasBit(key, depth)) {
        const k = this.hashInternal(node, next);
        this.putNode(k, [node, next]);
        next = k;
      } else {
        const k = this.hashInternal(next, node);
        this.putNode(k, [next, node]);
        next = k;
      }

      depth -= 1;
    }

    // Store value.
    this.putValue(leaf, value);

    return next;
  }

  async insert(key, value) {
    assert(this.isKey(key));
    this.root = await this._insert(this.root, key, value);
    return this.root;
  }

  async _remove(root, key) {
    const nodes = [];
    const prune = [];

    let next = root;
    let depth = 0;

    // Traverse bits left to right.
    for (;;) {
      if (depth === this.bits) {
        throw new MissingNodeError({
          rootHash: root,
          key,
          depth
        });
      }

      const node = await this.readNode(next);

      // Empty (sub)tree.
      if (!node) {
        if (!next.equals(this.hash.zero)) {
          throw new MissingNodeError({
            rootHash: root,
            nodeHash: next,
            key,
            depth
          });
        }

        return root;
      }

      // Leaf node.
      if (node.length === 1) {
        // Current key.
        const other = node[0];

        if (!key.equals(other))
          return root;

        // Prune old nodes.
        if (this.pruner.enabled) {
          prune.push(next);
          // Value to remove.
          prune.push(this.valueKey(next));
        }

        // Uncache old nodes.
        this.cache.remove(next);

        // The branch doesn't grow.
        // Replace the current node.
        depth -= 1;

        break;
      }

      // Prune old nodes.
      if (this.pruner.enabled)
        prune.push(next);

      // Uncache old nodes.
      this.cache.remove(next);

      // Internal node.
      const bit = hasBit(key, depth);
      nodes.push(node[bit ^ 1]);
      next = node[bit];
      depth += 1;
    }

    // Prune old nodes.
    for (const hash of prune)
      this.pruner.prune(hash);

    // Replace with a zero hash.
    next = this.hash.zero;

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      if (hasBit(key, depth)) {
        const k = this.hashInternal(node, next);
        this.putNode(k, [node, next]);
        next = k;
      } else {
        const k = this.hashInternal(next, node);
        this.putNode(k, [next, node]);
        next = k;
      }

      depth -= 1;
    }

    return next;
  }

  async remove(key) {
    assert(this.isKey(key));
    this.root = await this._remove(this.root, key);
    return this.root;
  }

  rootHash(enc) {
    if (enc === 'hex')
      return this.root.toString('hex');
    return this.root;
  }

  commit(batch, enc) {
    assert(batch);

    // Commit tree.
    this.cache.commit(batch);

    // Prune nodes we don't need.
    this.pruner.commit(batch);

    // Write best state.
    batch.put(STATE_KEY, this.root);

    this.originalRoot = this.root;

    return this.rootHash(enc);
  }

  snapshot(root) {
    if (root == null)
      root = this.originalRoot;

    if (!this.db)
      throw new Error('Cannot snapshot without database.');

    const {hash, bits, db} = this;
    const tree = new this.constructor(hash, bits, db);
    tree.context = this.context;

    return tree.inject(root);
  }

  inject(root) {
    if (root == null)
      root = this.originalRoot;

    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    assert(Buffer.isBuffer(root));
    assert(root.length === this.hash.size);

    this.root = root;
    this.originalRoot = root;

    return this;
  }

  async prove(root, key) {
    if (key == null) {
      key = root;
      root = this.root;
    }
    return proofs.prove(this, root, key);
  }

  verify(root, key, proof) {
    return proofs.verify(this.hash, this.bits, root, key, proof);
  }

  static get proof() {
    return proofs;
  }

  static get Proof() {
    return Proof;
  }
}

/**
 * Proofs
 */

const proofs = {};

proofs.prove = async function prove(tree, root, key) {
  assert(tree instanceof Merklix);
  assert(tree.isHash(root));
  assert(tree.isKey(key));

  const nodes = [];

  let next = root;
  let depth = 0;
  let k = null;
  let v = null;

  // Traverse bits left to right.
  for (;;) {
    if (depth === tree.bits) {
      throw new MissingNodeError({
        rootHash: root,
        key,
        depth
      });
    }

    const node = await tree.readNode(next);

    // Empty (sub)tree.
    if (!node) {
      if (!next.equals(tree.hash.zero)) {
        throw new MissingNodeError({
          rootHash: root,
          nodeHash: next,
          key,
          depth
        });
      }

      nodes.push(next);
      break;
    }

    // Leaf node.
    if (node.length === 1) {
      nodes.push(next);

      if (!key.equals(node[0]))
        k = node[0];

      v = await tree.readValue(next);
      assert(v);

      break;
    }

    // Internal node.
    const bit = hasBit(key, depth);
    nodes.push(node[bit ^ 1]);
    next = node[bit];
    depth += 1;
  }

  return new Proof(nodes, k, v);
};

proofs.verify = function verify(hash, bits, root, key, proof) {
  assert(hash && typeof hash.digest === 'function');
  assert((bits >>> 0) === bits);
  assert(bits > 0 && (bits & 7) === 0);
  assert(Buffer.isBuffer(root));
  assert(Buffer.isBuffer(key));
  assert(root.length === hash.size);
  assert(key.length === (bits >>> 3));
  assert(proof instanceof Proof);

  const nodes = proof.nodes;

  if (nodes.length === 0)
    return [PROOF_EARLY_END, null];

  if (nodes.length > bits)
    return [PROOF_MALFORMED_NODE, null];

  const ctx = hash.hash();
  const leaf = nodes[nodes.length - 1];

  let next = leaf;
  let depth = nodes.length - 2;

  // Traverse bits right to left.
  while (depth >= 0) {
    const node = nodes[depth];

    if (hasBit(key, depth))
      next = hashInternal(ctx, node, next);
    else
      next = hashInternal(ctx, next, node);

    depth -= 1;
  }

  if (!next.equals(root))
    return [PROOF_HASH_MISMATCH, null];

  // Two types of NX proofs.

  // Type 1: Non-existent leaf.
  if (leaf.equals(hash.zero)) {
    if (proof.key)
      return [PROOF_UNEXPECTED_NODE, null];

    if (proof.value)
      return [PROOF_UNEXPECTED_NODE, null];

    return [PROOF_OK, null];
  }

  // Type 2: Prefix collision.
  // We have to provide the full preimage
  // to prove we're a leaf, and also that
  // we are indeed a different key.
  if (proof.key) {
    if (!proof.value)
      return [PROOF_UNEXPECTED_NODE, null];

    if (proof.key.equals(key))
      return [PROOF_UNEXPECTED_NODE, null];

    const h = hashLeaf(ctx, proof.key, proof.value);

    if (!h.equals(leaf))
      return [PROOF_HASH_MISMATCH, null];

    return [PROOF_OK, null];
  }

  // Otherwise, we should have a value.
  if (!proof.value)
    return [PROOF_NO_RESULT, null];

  const h = hashLeaf(ctx, key, proof.value);

  if (!h.equals(leaf))
    return [PROOF_HASH_MISMATCH, null];

  return [PROOF_OK, proof.value];
};

/**
 * Cache
 */

class Cache {
  constructor(size) {
    assert((size >>> 0) === size);
    this.map = new Map();
    this.ops = [];
  }

  get(key) {
    return this.map.get(key.toString('hex')) || null;
  }

  set(key, value) {
    this.map.set(key.toString('hex'), value);
    return this;
  }

  remove(key) {
    this.map.delete(key.toString('hex'));
    return this;
  }

  put(key, value) {
    this.set(key, value);
    this.ops.push([key, value]);
    return this;
  }

  commit(batch) {
    for (const [key, value] of this.ops)
      batch.put(key, value);

    this.ops.length = 0;
    this.map.clear();

    return this;
  }
}

/**
 * LRUCache
 */

class LRUCache {
  constructor(size) {
    assert((size >>> 0) === size);
    this.lru = new LRU(size);
    this.map = new Map();
  }

  get(key) {
    const hex = key.toString('hex');
    const cache = this.lru.get(hex);

    if (cache)
      return cache;

    const item = this.map.get(hex);

    if (!item)
      return null;

    return item[1];
  }

  set(key, value) {
    if (this.lru.capacity > 0)
      this.lru.set(key.toString('hex'), value);
    return this;
  }

  remove(key) {
    if (this.lru.capacity > 0)
      this.lru.remove(key.toString('hex'));
    return this;
  }

  put(key, value) {
    this.map.set(key.toString('hex'), [key, value]);
    return this;
  }

  commit(batch) {
    for (const [hex, [key, value]] of this.map) {
      batch.put(key, value);
      this.lru.set(hex, value);
      this.map.delete(hex);
    }

    return this;
  }
}

/**
 * Prune List
 */

class PruneList {
  constructor(enabled = false) {
    assert(typeof enabled === 'boolean');
    this.enabled = enabled;
    this.map = new Map();
  }

  prune(key) {
    if (this.enabled)
      this.map.set(key.toString('hex'), key);
    return this;
  }

  unprune(key) {
    if (this.enabled)
      this.map.delete(key.toString('hex'));
    return this;
  }

  commit(batch) {
    if (!this.enabled)
      return this;

    for (const [hex, key] of this.map) {
      batch.del(key);
      this.map.delete(hex);
    }

    return this;
  }
}

/**
 * Proof
 */

class Proof {
  constructor(nodes, key, value) {
    this.nodes = [];
    this.key = null;
    this.value = null;
    this.from(nodes, key, value);
  }

  from(nodes, key, value) {
    if (nodes != null) {
      assert(Array.isArray(nodes));
      this.nodes = nodes;
    }

    if (key != null) {
      assert(Buffer.isBuffer(key));
      this.key = key;
    }

    if (value != null) {
      assert(Buffer.isBuffer(value));
      this.value = value;
    }

    return this;
  }

  getSize(hashSize, bits) {
    assert((hashSize >>> 0) === hashSize);
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    let size = 0;

    size += 1;
    size += (this.nodes.length + 7) / 8 | 0;

    const zeroHash = Buffer.alloc(hashSize, 0x00);

    for (const node of this.nodes) {
      if (!node.equals(zeroHash))
        size += node.length;
    }

    size += 2;

    if (this.key)
      size += bits >>> 3;

    if (this.value)
      size += this.value.length;

    return size;
  }

  encode(hashSize, bits) {
    assert((hashSize >>> 0) === hashSize);
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    const zeroHash = Buffer.alloc(hashSize, 0x00);
    const size = this.getSize(hashSize, bits);
    const bsize = (this.nodes.length + 7) / 8 | 0;
    const data = Buffer.alloc(size);

    let pos = 0;

    assert(this.nodes.length > 0);
    assert(this.nodes.length <= bits);

    data[pos] = this.nodes.length - 1;

    pos += 1;

    // data.fill(0x00, pos, pos + bsize);

    pos += bsize;

    for (let i = 0; i < this.nodes.length; i++) {
      const node = this.nodes[i];

      if (node.equals(zeroHash))
        setBit(data, 8 + i);
      else
        pos += node.copy(data, pos);
    }

    let field = 0;

    if (this.key)
      field |= 1 << 15;

    if (this.value) {
      // 16kb max
      assert(this.value.length < (1 << 14));
      field |= 1 << 14;
      field |= this.value.length;
    }

    data[pos] = field & 0xff;
    pos += 1;
    data[pos] |= field >>> 8;
    pos += 1;

    if (this.key)
      pos += this.key.copy(data, pos);

    if (this.value)
      pos += this.value.copy(data, pos);

    assert(pos === data.length);

    return data;
  }

  decode(data, hashSize, bits) {
    assert(Buffer.isBuffer(data));
    assert((hashSize >>> 0) === hashSize);
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    let pos = 0;

    assert(pos + 1 <= data.length);

    const count = data[pos] + 1;
    const bsize = (count + 7) / 8 | 0;

    pos += 1;
    pos += bsize;

    assert(pos <= data.length);

    const zeroHash = Buffer.alloc(hashSize, 0x00);

    for (let i = 0; i < count; i++) {
      if (hasBit(data, 8 + i)) {
        this.nodes.push(zeroHash);
      } else {
        assert(pos + hashSize <= data.length);
        const hash = data.slice(pos, pos + hashSize);
        this.nodes.push(hash);
        pos += hashSize;
      }
    }

    assert(pos + 2 <= data.length);

    let field = 0;
    field |= data[pos];
    field |= data[pos + 1] << 8;
    pos += 2;

    if (field & (1 << 15)) {
      const keySize = bits >>> 3;
      assert(pos + keySize <= data.length);
      this.key = data.slice(pos, pos + keySize);
      pos += keySize;
    }

    if (field & (1 << 14)) {
      const size = field & ((1 << 14) - 1);
      assert(pos + size <= data.length);
      this.value = data.slice(pos, pos + size);
      pos += size;
    }

    return this;
  }

  static decode(data, hashSize, bits) {
    return new this().decode(data, hashSize, bits);
  }
}

/**
 * Missing Node Error
 */

class MissingNodeError extends Error {
  /**
   * Create an error.
   * @constructor
   * @param {Object?} options
   */

  constructor(options = {}) {
    super();
    this.type = 'MissingNodeError';
    this.name = 'MissingNodeError';
    this.code = 'ERR_MISSING_NODE';
    this.rootHash = options.rootHash || null;
    this.nodeHash = options.nodeHash || null;
    this.key = options.key || null;
    this.depth = options.depth >>> 0;
    this.message = 'Missing node.';

    if (this.nodeHash)
      this.message = `Missing node: ${this.nodeHash.toString('hex')}.`;

    if (Error.captureStackTrace)
      Error.captureStackTrace(this, MissingNodeError);
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

function sizeNode(field, size) {
  const $33 = 1 + size;
  const $65 = 1 + size * 2;

  switch (field) {
    case 0: // 00
      return 1;
    case 1: // 01
    case 2: // 10
    case 4:
      return $33;
    case 3: // 11
      return $65;
    default:
      return -1;
  }
}

function hashInternal(ctx, left, right) {
  ctx.init();
  ctx.update(INTERNAL);
  ctx.update(left);
  ctx.update(right);
  return ctx.final();
}

function hashLeaf(ctx, key, value) {
  ctx.init();
  ctx.update(LEAF);
  ctx.update(key);
  ctx.update(value);
  return ctx.final();
}

/*
 * Expose
 */

module.exports = Merklix;
