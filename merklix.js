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

/*
 * Constants
 */

const ZERO_HASH = Buffer.alloc(32, 0x00);
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
 * MerklixTree
 */

class MerklixTree {
  constructor(db, hash) {
    assert(db && typeof db === 'object');
    assert(hash && typeof hash.digest === 'function');

    this.db = db;
    this.cache = new Cache();
    this.hash = hash;
    this.context = null;
  }

  ctx() {
    if (!this.context)
      this.context = this.hash.hash();
    return this.context;
  }

  hashInternal(left, right) {
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

  async readNode(hash) {
    if (hash.equals(ZERO_HASH))
      return null;

    const raw = await this.read(hash);

    if (!raw)
      return null;

    if (raw.length === 0)
      throw new Error('Database inconsistency.');

    if (raw.length !== sizeNode(raw[0]))
      throw new Error('Database inconsistency.');

    switch (raw[0]) {
      case 0: // 00
        return [ZERO_HASH, ZERO_HASH];
      case 1: // 01
        return [ZERO_HASH, raw.slice(1, 33)];
      case 2: // 10
        return [raw.slice(1, 33), ZERO_HASH];
      case 3: // 11
        return [raw.slice(1, 33), raw.slice(33, 65)];
      case 4:
        return [raw.slice(1)];
      default:
        throw new Error('Unknown node.');
    }
  }

  putNode(hash, node) {
    switch (node.length) {
      case 2: {
        const [left, right] = node;

        let field = 0;
        field |= !left.equals(ZERO_HASH) << 1;
        field |= !right.equals(ZERO_HASH);

        const size = sizeNode(field);
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
    //this.cache.del(hash);
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
    //this.cache.del(this.valueKey(leaf));
  }

  async _get(root, key) {
    let next = root;
    let depth = 0;

    // Traverse bits left to right.
    for (;;) {
      const node = await this.readNode(next);

      // Empty (sub)tree.
      if (!node) {
        if (!next.equals(ZERO_HASH))
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
      const node = await this.readNode(next);

      // Empty (sub)tree.
      if (!node) {
        if (!next.equals(ZERO_HASH))
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

          // Prune old nodes.
          del.push(next);

          // Value to remove.
          removed = next;

          // The branch doesn't grow.
          // Replace the current node.
          depth -= 1;

          break;
        }

        // Insert dummy nodes to artificially grow
        // the branch if we have bit collisions.
        while (hasBit(key, depth) === hasBit(other, depth)) {
          // Child-less sidenode.
          depth += 1;
          nodes.push(ZERO_HASH);
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
      const node = await this.readNode(next);

      // Empty (sub)tree.
      if (!node)
        return [root, null];

      // Leaf node.
      if (node.length === 1) {
        // Current key.
        const other = node[0];

        if (!key.equals(other))
          return [root, null];

        // Prune old nodes.
        del.push(next);

        // Value to remove.
        removed = next;

        // The branch doesn't grow.
        // Replace the current node.
        depth -= 1;

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

    // Replace with a zero hash.
    next = ZERO_HASH;

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
      const node = await this.readNode(next);

      // Empty (sub)tree.
      if (!node) {
        if (!next.equals(ZERO_HASH))
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

    return new Proof(nodes, k, v);
  }

  verify(root, key, proof) {
    const nodes = proof.nodes;

    if (nodes.length === 0)
      return [PROOF_EARLY_END, null];

    if (nodes.length > 256)
      return [PROOF_MALFORMED_NODE, null];

    if (proof.value && proof.value.length > 512)
      return [PROOF_MALFORMED_NODE, null];

    const leaf = nodes[nodes.length - 1];

    let next = leaf;
    let depth = nodes.length - 2;

    // Traverse bits right to left.
    while (depth >= 0) {
      const node = nodes[depth];

      if (hasBit(key, depth))
        next = this.hashInternal(node, next);
      else
        next = this.hashInternal(next, node);

      depth -= 1;
    }

    if (!next.equals(root))
      return [PROOF_HASH_MISMATCH, null];

    // Two types of NX proofs.

    // Type 1: Non-existent leaf.
    if (leaf.equals(ZERO_HASH)) {
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

  static get Proof() {
    return Proof;
  }
}

/*
 * Merklix
 */

class Merklix {
  constructor(db, hash) {
    this.db = db;
    this.tree = new MerklixTree(db, hash);
    this.originalRoot = ZERO_HASH;
    this.root = ZERO_HASH;
  }

  async open(root) {
    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    assert(!root || Buffer.isBuffer(root));

    // Try to retrieve best state.
    if (!root && this.db)
      root = await this.db.get(STATE_KEY);

    if (root && !root.equals(ZERO_HASH)) {
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
    this.originalRoot = ZERO_HASH;
    this.root = ZERO_HASH;
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
    const {hash} = this.tree;
    const tree = new this.constructor(db, hash);
    tree.tree.context = this.tree.context;

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

  static get Proof() {
    return Proof;
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
      assert(Buffer.isBuffer(key) && key.length === 32);
      this.key = key;
    }

    if (value != null) {
      assert(Buffer.isBuffer(value));
      this.value = value;
    }

    return this;
  }

  getSize() {
    let size = 0;

    size += 1;
    size += (this.nodes.length + 7) / 8 | 0;

    for (const node of this.nodes) {
      if (!node.equals(ZERO_HASH))
        size += 32;
    }

    size += 2;

    if (this.key)
      size += 32;

    if (this.value)
      size += this.value.length;

    return size;
  }

  encode() {
    const size = this.getSize();
    const bits = (this.nodes.length + 7) / 8 | 0;
    const data = Buffer.alloc(size);

    let pos = 0;

    assert(this.nodes.length > 0);
    assert(this.nodes.length <= 256);

    data[pos] = this.nodes.length - 1;

    pos += 1;

    //data.fill(0x00, pos, pos + bits);

    pos += bits;

    for (let i = 0; i < this.nodes.length; i++) {
      const node = this.nodes[i];

      if (node.equals(ZERO_HASH))
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

  decode(data) {
    assert(Buffer.isBuffer(data));

    let pos = 0;

    assert(pos + 1 <= data.length);

    const count = data[pos] + 1;
    const bits = (count + 7) / 8 | 0;

    pos += 1;
    pos += bits;

    assert(pos <= data.length);

    for (let i = 0; i < count; i++) {
      if (hasBit(data, 8 + i)) {
        this.nodes.push(ZERO_HASH);
      } else {
        assert(pos + 32 <= data.length);
        const hash = data.slice(pos, pos + 32);
        this.nodes.push(hash);
        pos += 32;
      }
    }

    assert(pos + 2 <= data.length);

    let field = 0;
    field |= data[pos];
    field |= data[pos + 1] << 8;
    pos += 2;

    if (field & (1 << 15)) {
      assert(pos + 32 <= data.length);
      this.key = data.slice(pos, pos + 32);
      pos += 32;
    }

    if (field & (1 << 14)) {
      const size = field & ((1 << 14) - 1);
      assert(pos + size <= data.length);
      this.value = data.slice(pos, pos + size);
      pos += size;
    }

    return this;
  }

  static decode(data) {
    return new this().decode(data);
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

function sizeNode(field) {
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

/*
 * Expose
 */

module.exports = Merklix;
