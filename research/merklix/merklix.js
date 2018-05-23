/*!
 * merklix.js - merklix tree
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 *
 * Merklix Trees:
 *   https://www.deadalnix.me/2016/09/24/introducing-merklix-tree-as-an-unordered-merkle-tree-on-steroid/
 *   https://www.deadalnix.me/2016/09/29/using-merklix-tree-to-checkpoint-an-utxo-set/
 */

'use strict';

const assert = require('assert');
const common = require('./common');
const errors = require('./errors');
const nodes = require('./nodes');
const proof = require('./proof');
const store = require('./store');

const {
  ensureHash,
  hasBit,
  hashLeaf,
  hashInternal,
  readPos,
  writePos
} = common;

const {
  MissingNodeError
} = errors;

const {
  types,
  NIL,
  Internal,
  Leaf,
  Hash,
  decodeNode
} = nodes;

const {
  NULL,
  INTERNAL,
  LEAF,
  HASH
} = types;

const {
  prove,
  verify,
  Proof
} = proof;

const {
  FileStore,
  MemoryStore
} = store;

/*
 * Constants
 */

const STATE_KEY = Buffer.from([0x73]);

/**
 * Merklix
 */

class Merklix {
  /**
   * Create a merklix tree.
   * @constructor
   * @param {Object} hash
   * @param {Number} bits
   * @param {String} prefix
   * @param {Object} [db=null]
   * @param {Number} [limit=4]
   */

  constructor(hash, bits, prefix, db, limit) {
    if (limit == null)
      limit = 4;

    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(!prefix || typeof prefix === 'string');
    assert(!db || typeof db === 'object');
    assert((limit >>> 0) === limit);

    const Store = prefix
      ? FileStore
      : MemoryStore;

    this.hash = ensureHash(hash);
    this.bits = bits;
    this.prefix = prefix || null;
    this.db = db || null;
    this.store = new Store(prefix, hash, bits);
    this.originalRoot = this.hash.zero;
    this.root = NIL;
    this.cacheGen = 0;
    this.cacheLimit = limit;
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

  hashLeaf(key, value) {
    return hashLeaf(this.ctx(), key, value);
  }

  async open(root) {
    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    await this.store.open();

    // Try to retrieve best state.
    if (!root && this.db)
      root = await this.db.get(STATE_KEY);

    if (root) {
      this.root = await this.getHistory(root);
      this.originalRoot = root;
    }
  }

  async close() {
    this.root = NIL;
    this.originalRoot = this.hash.zero;
    this.cacheGen = 0;

    await this.store.close();
  }

  async getHistory(root) {
    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    assert(this.isHash(root));

    if (root.equals(this.hash.zero))
      return NIL;

    if (root.equals(this.originalRoot))
      return this.root;

    if (!this.db)
      throw new Error('Cannot get history without database.');

    const raw = await this.db.get(root);

    if (!raw) {
      throw new MissingNodeError({
        rootHash: root,
        nodeHash: root
      });
    }

    const [index, pos] = readPos(raw);

    return new Hash(root, index, pos);
  }

  async getRoot(root) {
    const node = await this.getHistory(root);
    if (node.isHash())
      return await node.resolve(this.store);
    return node;
  }

  async ensureRoot() {
    if (this.root.isHash())
      this.root = await this.root.resolve(this.store);
    return this.root;
  }

  async _get(root, key) {
    let node = root;
    let depth = 0;

    // Traverse bits left to right.
    for (;;) {
      // Empty (sub)tree.
      if (node.isNull())
        break;

      // Leaf node.
      if (node.isLeaf()) {
        // Prefix collision.
        if (!key.equals(node.key))
          node = NIL;
        break;
      }

      if (depth === this.bits) {
        throw new MissingNodeError({
          rootHash: root.hash(this.ctx()),
          key,
          depth
        });
      }

      assert(node.isInternal());

      // Internal node.
      if (hasBit(key, depth))
        node = await node.getRight(this.store);
      else
        node = await node.getLeft(this.store);

      depth += 1;
    }

    if (node.isNull())
      return null;

    return node.getValue(this.store);
  }

  async get(key) {
    assert(this.isKey(key));

    let root = this.root;

    if (root.isHash())
      root = await root.resolve(this.store);

    return this._get(root, key);
  }

  async _insert(root, key, value) {
    const leaf = this.hashLeaf(key, value);
    const nodes = [];

    let node = root;
    let depth = 0;
    let next;

    // Traverse bits left to right.
    for (;;) {
      // Empty (sub)tree.
      if (node.isNull()) {
        // Replace the empty node.
        break;
      }

      // Leaf node.
      if (node.isLeaf()) {
        // Current key.
        const other = node.key;

        if (key.equals(other)) {
          // Exact leaf already exists.
          if (leaf.equals(node.data))
            return root;

          // The branch doesn't grow.
          // Replace the current node.
          break;
        }

        assert(depth !== this.bits);

        // Insert placeholder leaves to grow
        // the branch if we have bit collisions.
        while (hasBit(key, depth) === hasBit(other, depth)) {
          // Child-less sidenode.
          nodes.push(NIL);
          depth += 1;
        }

        // Leaf is our sibling.
        nodes.push(node);
        depth += 1;

        break;
      }

      if (depth === this.bits) {
        throw new MissingNodeError({
          rootHash: root.hash(this.ctx()),
          key,
          depth
        });
      }

      assert(node.isInternal());

      // Internal node.
      if (hasBit(key, depth)) {
        nodes.push(node.left);
        node = await node.getRight(this.store);
      } else {
        nodes.push(node.right);
        node = await node.getLeft(this.store);
      }

      depth += 1;
    }

    // Start at the leaf.
    next = new Leaf(leaf, key, value);

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      depth -= 1;

      if (hasBit(key, depth))
        next = new Internal(node, next);
      else
        next = new Internal(next, node);
    }

    return next;
  }

  async insert(key, value) {
    assert(this.isKey(key));
    assert(Buffer.isBuffer(value));

    const root = await this.ensureRoot();

    this.root = await this._insert(root, key, value);

    return this.root;
  }

  async _remove(root, key) {
    const nodes = [];

    let node = root;
    let depth = 0;
    let next;

    // Traverse bits left to right.
    for (;;) {
      // Empty (sub)tree.
      if (node.isNull())
        return root;

      // Leaf node.
      if (node.isLeaf()) {
        // Current key.
        const other = node.key;

        if (!key.equals(other))
          return root;

        // Root can be a leaf.
        if (depth === 0) {
          // Remove the root.
          return NIL;
        }

        // Sibling.
        let s = nodes.pop();
        depth -= 1;

        if (s.isHash())
          s = await s.resolve(this.store);

        // Shrink the subtree if we're a leaf.
        if (s.isLeaf()) {
          // Sanity check (last comparison should have been different).
          assert(hasBit(key, depth) !== hasBit(s.key, depth));

          while (depth > 0) {
            const side = nodes[depth - 1];

            if (hasBit(key, depth - 1) !== hasBit(s.key, depth - 1))
              break;

            if (!side.isNull())
              break;

            nodes.pop();
            depth -= 1;
          }

          next = s;
        } else {
          assert(s.isInternal());
          nodes.push(s);
          depth += 1;
          next = NIL;
        }

        break;
      }

      if (depth === this.bits) {
        throw new MissingNodeError({
          rootHash: root.hash(this.ctx()),
          key,
          depth
        });
      }

      assert(node.isInternal());

      // Internal node.
      if (hasBit(key, depth)) {
        nodes.push(node.left);
        node = await node.getRight(this.store);
      } else {
        nodes.push(node.right);
        node = await node.getLeft(this.store);
      }

      depth += 1;
    }

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      depth -= 1;

      if (hasBit(key, depth))
        next = new Internal(node, next);
      else
        next = new Internal(next, node);
    }

    return next;
  }

  async remove(key) {
    assert(this.isKey(key));

    const root = await this.ensureRoot();

    this.root = await this._remove(root, key);

    return this.root;
  }

  rootHash(enc) {
    const ctx = this.ctx();
    const hash = this.root.hash(ctx);

    if (enc === 'hex')
      return hash.toString('hex');

    return hash;
  }

  async commit(batch, enc) {
    if (this.db)
      assert(batch && typeof batch.put === 'function');

    this.store.start();

    this.root = this._commit(this.root, this.ctx());

    await this.store.flush();
    await this.store.sync();

    this.originalRoot = this.rootHash();

    if (batch) {
      assert(this.db);
      batch.put(this.originalRoot, writePos(this.root.index, this.root.pos));
      batch.put(STATE_KEY, this.originalRoot);
    }

    if (enc === 'hex')
      return this.originalRoot.toString('hex');

    return this.originalRoot;
  }

  _commit(node, ctx) {
    switch (node.type) {
      case NULL: {
        assert(node.index === 0);
        return node;
      }

      case INTERNAL: {
        node.left = this._commit(node.left, ctx);
        node.right = this._commit(node.right, ctx);

        if (node.index === 0)
          this.store.writeNode(node);

        assert(node.index !== 0);

        if (node.gen === this.cacheLimit)
          return new Hash(node.hash(ctx), node.index, node.pos);

        node.gen += 1;

        return node;
      }

      case LEAF: {
        if (node.index === 0) {
          assert(node.value);
          this.store.writeValue(node);
          this.store.writeNode(node);
        }

        assert(node.index !== 0);

        return new Hash(node.hash(ctx), node.index, node.pos);
      }

      case HASH: {
        return node;
      }
    }

    throw new AssertionError('Unknown node.');
  }

  async snapshot(root) {
    if (root == null)
      root = this.originalRoot;

    const {hash, bits, prefix, db, cacheLimit} = this;
    const tree = new this.constructor(hash, bits, prefix, db, cacheLimit);
    tree.store = this.store;
    tree.context = this.context;

    return tree.inject(root);
  }

  async inject(root) {
    if (root == null)
      root = this.originalRoot;

    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    this.root = await this.getHistory(root);
    this.originalRoot = root;

    return this;
  }

  async prove(root, key) {
    if (key == null) {
      key = root;
      root = this.originalRoot;
    }
    return prove(this, root, key);
  }

  verify(root, key, proof) {
    return verify(this.hash, this.bits, root, key, proof);
  }

  static get proof() {
    return proof;
  }

  static get Proof() {
    return Proof;
  }
}

/*
 * Expose
 */

module.exports = Merklix;
