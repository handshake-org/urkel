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
const blake2b = require('bcrypto/lib/blake2b');

/*
 * Constants
 */

const ZERO_HASH = Buffer.alloc(32, 0x00);
const STATE_KEY = Buffer.from([0x73]);
const INTERNAL = Buffer.from([0x00]);
const LEAF = Buffer.from([0x01]);

const PROOF_OK = 0;
const PROOF_HASH_MISMATCH = 1;
const PROOF_MALFORMED_NODE = 2;
const PROOF_UNEXPECTED_NODE = 3;
const PROOF_EARLY_END = 4;
const PROOF_NO_RESULT = 5;
const PROOF_UNKNOWN_ERROR = 6;

/**
 * MerklixTree
 */

class MerklixTree {
  constructor(db) {
    this.db = db;
    this.cache = new Cache();
    this.depth = 0;
    this.hash = blake2b;
    this._ctx = null;
    this._hashes = [];
  }

  ctx() {
    if (!this._ctx)
      this._ctx = this.hash.hash();
    return this._ctx;
  }

  hashes() {
    if (this._hashes.length > 0)
      return this._hashes;

    const buf = Buffer.from('merklix ');

    for (let i = 0; i < 256; i++) {
      buf[7] = i;
      this._hashes.push(blake2b.digest(buf));
    }

    return this._hashes;
  }

  get zero() {
    return this.hashDepth(0);
  }

  isDepth(hash, depth) {
    const def = this.hashDepth(depth);
    return hash.equals(def);
  }

  hashDepth(depth) {
    return ZERO_HASH;
  }

  hashInternal(left, right, depth) {
    const zero = this.hashDepth(depth);

    if (left.equals(zero))
      return this.hash.digest(right);

    if (right.equals(zero))
      return this.hash.digest(left);

    const ctx = this.ctx();
    ctx.init();
    ctx.update(INTERNAL);
    ctx.update(left);
    ctx.update(right);
    return ctx.final();
  }

  hashLeaf(key, value) {
    const ctx = this.ctx();
    ctx.init();
    ctx.update(LEAF);
    ctx.update(key);
    ctx.update(value);
    return ctx.final();
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

  async readNode(hash, depth) {
    const raw = await this.read(hash);

    if (!raw)
      return null;

    if (raw.length === 0)
      throw new Error('Database inconsistency.');

    if (raw.length !== this.sizeNode(raw[0]))
      throw new Error('Database inconsistency.');

    const zero = this.hashDepth(depth);

    switch (raw[0]) {
      case 0: // 00
        return [zero, zero];
      case 1: // 01
        return [zero, raw.slice(1, 33)];
      case 2: // 10
        return [raw.slice(1, 33), zero];
      case 3: // 11
        return [raw.slice(1, 33), raw.slice(33, 65)];
      case 4:
        return [raw.slice(1)];
      default:
        throw new Error('Unknown node.');
    }
  }

  sizeNode(field) {
    switch (field) {
      case 0: // 00
        return 1;
      case 1: // 01
      case 2: // 10
      case 4:
        return 33;
      case 3: // 11
        return 65;
      default:
        throw new Error('Unknown node.');
    }
  }

  putNode(hash, node, depth) {
    switch (node.length) {
      case 2: {
        const [left, right] = node;
        const zero = this.hashDepth(depth);

        let field = 0;
        field |= !left.equals(zero) << 1;
        field |= !right.equals(zero);

        const size = this.sizeNode(field);
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
            right.copy(val, 33);
            break;
          default:
            throw new Error('Unknown node.');
        }

        this.cache.put(hash, val);

        break;
      }

      case 1: {
        const [key] = node;
        const val = Buffer.allocUnsafe(33);
        val[0] = 0x04;
        key.copy(val, 1);
        this.cache.put(hash, val);
        break;
      }

      default: {
        throw new Error('Unknown node.');
      }
    }
  }

  delNode(hash) {
    this.cache.del(hash);
  }

  valueKey(leaf) {
    const key = Buffer.allocUnsafe(33);
    key[0] = 0x76; // v
    leaf.copy(key, 1);
    return key;
  }

  async readValue(leaf) {
    return this.read(this.valueKey(leaf));
  }

  putValue(leaf, value) {
    this.cache.put(this.valueKey(leaf), value);
  }

  delValue(leaf) {
    this.cache.del(this.valueKey(leaf));
  }

  async _get(root, key) {
    let next = root;
    let depth = 0;

    // Traverse bits left to right.
    for (;;) {
      const node = await this.readNode(next, depth);

      // Empty (sub)tree.
      if (!node) {
        if (!this.isDepth(next, depth))
          throw new Error('Database inconsistency.');

        next = null;
        break;
      }

      // Leaf node.
      if (node.length === 1) {
        // Prefix collision.
        if (!key.equals(node[0]))
          next = null
        break;
      }

      // Internal node.
      const bit = hasBit(key, depth);
      next = node[bit];
      depth += 1;
    }

    return next;
  }

  async get(root, key) {
    const leaf = await this._get(root, key);

    if (!leaf)
      return null;

    return this.readValue(leaf);
  }

  async _insert(root, key, leaf) {
    const nodes = [];
    const del = [];

    let next = root;
    let depth = 0;
    let removed = null;

    // Traverse bits left to right.
    for (;;) {
      const node = await this.readNode(next, depth);

      // Empty (sub)tree.
      if (!node) {
        if (!this.isDepth(next, depth))
          throw new Error('Database inconsistency.');

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
            return [root, null, false];

          del.push(next);
          removed = next;

          // The branch doesn't grow.
          // Replace the current node.
          depth -= 1;

          break;
        }

        // Insert dummy nodes to artificially grow
        // the branch if we have bit collisions.
        // Is there a better way? Not sure.
        // Potential DoS vector.
        while (hasBit(key, depth) === hasBit(other, depth)) {
          // Child-less sidenode.
          depth += 1;
          nodes.push(this.hashDepth(depth));
        }

        nodes.push(next);

        break;
      }

      // Prune old nodes.
      del.push(next);

      // Internal node.
      const bit = hasBit(key, depth);
      nodes.push(node[bit ^ 1]);
      next = node[bit];
      depth += 1;
    }

    // Prune old nodes.
    for (const hash of del)
      this.delNode(hash);

    // Track max depth.
    if (depth > this.depth)
      this.depth = depth;

    // Store the key for
    // comparisons later (see above).
    this.putNode(leaf, [key], depth + 1);

    next = leaf;

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      if (hasBit(key, depth)) {
        const k = this.hashInternal(node, next, depth);
        this.putNode(k, [node, next], depth);
        next = k;
      } else {
        const k = this.hashInternal(next, node, depth);
        this.putNode(k, [next, node], depth);
        next = k;
      }

      depth -= 1;
    }

    return [next, removed, true];
  }

  async insert(root, key, value) {
    const leaf = this.hashLeaf(key, value);
    const [next, removed, updated] = await this._insert(root, key, leaf);

    if (updated)
      this.putValue(leaf, value);

    if (removed)
      this.delValue(removed);

    return next;
  }

  async _remove(root, key) {
    const nodes = [];
    const del = [];

    let next = root;
    let depth = 0;
    let removed = null;

    // Traverse bits left to right.
    for (;;) {
      const node = await this.readNode(next, depth);

      // Empty (sub)tree.
      if (!node)
        return [root, null];

      // Leaf node.
      if (node.length === 1) {
        // Current key.
        const other = node[0];

        if (!key.equals(other))
          return [root, null];

        del.push(next);
        removed = next;

        next = this.hashDepth(depth);
        depth -= 1;

        /*
        depth -= 2;
        next = nodes.pop();

        while (nodes.length > 0) {
          const node = nodes[nodes.length - 1];

          if (!this.isDepth(node, depth + 1))
            break;

          nodes.pop();
          depth -= 1;
        }
        */

        break;
      }

      // Prune old nodes.
      del.push(next);

      // Internal node.
      const bit = hasBit(key, depth);
      nodes.push(node[bit ^ 1]);
      next = node[bit];
      depth += 1;
    }

    // Prune old nodes.
    for (const hash of del)
      this.delNode(hash);

    // Track max depth.
    if (depth > this.depth)
      this.depth = depth;

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      if (hasBit(key, depth)) {
        const k = this.hashInternal(node, next, depth);
        this.putNode(k, [node, next], depth);
        next = k;
      } else {
        const k = this.hashInternal(next, node, depth);
        this.putNode(k, [next, node], depth);
        next = k;
      }

      depth -= 1;
    }

    return [next, removed];
  }

  async remove(root, key) {
    const [next, removed] = await this._remove(root, key);

    if (removed)
      this.delValue(removed);

    return next;
  }

  async prove(root, key) {
    const nodes = [];

    let next = root;
    let depth = 0;
    let k = null;
    let v = null;

    // Traverse bits left to right.
    for (;;) {
      const node = await this.readNode(next, depth);

      // Empty (sub)tree.
      if (!node) {
        if (!this.isDepth(next, depth))
          throw new Error('Database inconsistency.');

        nodes.push(next);
        break;
      }

      // Leaf node.
      if (node.length === 1) {
        nodes.push(next);

        if (!key.equals(node[0]))
          k = node[0];

        v = await this.readValue(next);
        assert(v);

        break;
      }

      // Internal node.
      const bit = hasBit(key, depth);
      nodes.push(node[bit ^ 1]);
      next = node[bit];
      depth += 1;
    }

    return {
      nodes,
      key: k,
      value: v
    };
  }

  verify(root, key, proof) {
    const nodes = proof.nodes;

    if (nodes.length === 0)
      return [PROOF_EARLY_END, null];

    if (nodes.length > 256)
      return [PROOF_MALFORMED_NODE, null];

    const leaf = nodes[nodes.length - 1];

    let next = leaf;
    let depth = nodes.length - 2;

    // Traverse bits right to left.
    while (depth >= 0) {
      const node = nodes[depth];

      if (hasBit(key, depth))
        next = this.hashInternal(node, next, depth);
      else
        next = this.hashInternal(next, node, depth);

      depth -= 1;
    }

    if (!next.equals(root))
      return [PROOF_HASH_MISMATCH, null];

    // Two types of NX proofs.

    // Type 1: Non-existent leaf.
    if (this.isDepth(leaf, nodes.length - 1)) {
      if (proof.key)
        return [PROOF_UNEXPECTED_NODE, null];

      if (proof.value)
        return [PROOF_UNEXPECTED_NODE, null];

      return [PROOF_OK, null];
    }

    // Type 2: Prefix collision.
    // We have to provide the full pre-image
    // to prove we're a leaf, and also that
    // we are indeed a different key.
    if (proof.key) {
      if (!proof.value)
        return [PROOF_UNEXPECTED_NODE, null];

      if (proof.key.equals(key))
        return [PROOF_UNEXPECTED_NODE, null];

      const hash = this.hashLeaf(proof.key, proof.value);

      if (!hash.equals(leaf))
        return [PROOF_HASH_MISMATCH, null];

      return [PROOF_OK, null];
    }

    // Otherwise, we should have a value.
    if (!proof.value)
      return [PROOF_NO_RESULT, null];

    const hash = this.hashLeaf(key, proof.value);

    if (!hash.equals(leaf))
      return [PROOF_HASH_MISMATCH, null];

    return [PROOF_OK, proof.value];
  }
}

/*
 * Merklix
 */

class Merklix {
  constructor(db) {
    this.db = db;
    this.tree = new MerklixTree(db);
    this.originalRoot = this.tree.zero;
    this.root = this.tree.zero;
  }

  async open(root) {
    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    assert(!root || Buffer.isBuffer(root));

    // Try to retrieve best state.
    if (!root && this.db)
      root = await this.db.get(STATE_KEY);

    if (root && !root.equals(this.tree.zero)) {
      assert(root.length === 32);

      if (!this.db)
        throw new Error('Cannot use root without database.');

      if (!await this.db.has(root))
        throw new Error('Missing merklix root.');

      this.originalRoot = root;
      this.root = root;
    }
  }

  async close() {
    this.originalRoot = this.tree.zero;
    this.root = this.tree.zero;
  }

  async get(key) {
    return this.tree.get(this.root, key);
  }

  async insert(key, value) {
    this.root = await this.tree.insert(this.root, key, value);
    return this.root;
  }

  async remove(key) {
    this.root = await this.tree.remove(this.root, key);
    return this.root;
  }

  hash(enc) {
    if (enc === 'hex')
      return this.root.toString('hex');
    return this.root;
  }

  commit(batch, enc) {
    this.tree.cache.commit(batch);
    this.originalRoot = this.root;
    batch.put(STATE_KEY, this.root);
    return this.hash(enc);
  }

  snapshot(root) {
    if (root == null)
      root = this.originalRoot;

    if (!this.db)
      throw new Error('Cannot snapshot without database.');

    const {db} = this;
    const tree = new this.constructor(db);
    tree.tree.hash = this.tree.hash;
    tree.tree._ctx = this.tree._ctx;
    tree.tree._hashes = this.tree._hashes;

    return tree.inject(root);
  }

  inject(root) {
    if (root == null)
      root = this.originalRoot;

    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    assert(Buffer.isBuffer(root));
    assert(root.length === 32);

    this.root = root;
    this.originalRoot = root;

    return this;
  }

  async prove(root, key) {
    if (key == null) {
      key = root;
      root = this.root;
    }
    return this.tree.prove(root, key);
  }

  verify(root, key, proof) {
    return this.tree.verify(root, key, proof);
  }
}

/**
 * Cache
 */

class Cache {
  constructor() {
    this.map = new Map();
    this.ops = [];
  }

  get(key) {
    return this.map.get(key.toString('hex')) || null;
  }

  set(key, value) {
    this.map.set(key.toString('hex'), value);
  }

  delete(key) {
    this.map.delete(key.toString('hex'));
  }

  put(key, value) {
    this.set(key, value);
    this.ops.push([key, value]);
  }

  push(key, value) {
    this.ops.push([key, value]);
  }

  del(key) {
    this.delete(key);
    this.ops.push([key]);
  }

  clear() {
    this.map.clear();
    this.ops.length = 0;
  }

  commit(batch) {
    for (const op of this.ops) {
      switch (op.length) {
        case 2:
          batch.put(op[0], op[1]);
          break;
        case 1:
          batch.del(op[0]);
          break;
      }
    }

    this.clear();
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

/*
 * Expose
 */

module.exports = Merklix;
