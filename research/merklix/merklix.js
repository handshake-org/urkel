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
  fromRecord,
  toRecord
} = common;

const {
  MissingNodeError,
  AssertionError
} = errors;

const {
  types,
  NIL,
  Internal,
  Leaf,
  Hash
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

  hashInternal(left, right) {
    return hashInternal(this.hash, left, right);
  }

  hashLeaf(key, value) {
    return hashLeaf(this.hash, key, value);
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

  async getRecord(root) {
    if (!this.db)
      throw new Error('Cannot get history without database.');

    const raw = await this.db.get(root);

    if (!raw) {
      throw new MissingNodeError({
        rootHash: root,
        nodeHash: root
      });
    }

    if (raw.length !== this.hash.size + 6)
      throw new AssertionError('Database corruption.');

    return fromRecord(raw);
  }

  writeRecord(batch, hash, prev, index, pos) {
    const raw = toRecord(prev, index, pos);
    batch.put(hash, raw);
  }

  writeRoot(batch, root, prev) {
    const hash = root.hash(this.hash);
    this.writeRecord(batch, hash, prev, root.index, root.pos);
    batch.put(STATE_KEY, hash);
  }

  async getHistory(root) {
    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    assert(this.isHash(root));

    if (root.equals(this.hash.zero))
      return NIL;

    if (root.equals(this.originalRoot)) {
      if (this.root.isNull())
        return NIL;

      if (this.root.isHash())
        return this.root;

      const {index, pos} = this.root;

      if (index !== 0)
        return new Hash(root, index, pos);
    }

    const [, index, pos] = await this.getRecord(root);

    return new Hash(root, index, pos);
  }

  async getRoot(root) {
    if (root == null)
      root = this.originalRoot;

    const node = await this.getHistory(root);

    if (node.isHash())
      return node.resolve(this.store);

    return node;
  }

  async ensureRoot() {
    if (this.root.isHash())
      return this.root.resolve(this.store);
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
          rootHash: root.hash(this.hash),
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
    const root = await this.getRoot();
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
          rootHash: root.hash(this.hash),
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
          rootHash: root.hash(this.hash),
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
    const hash = this.root.hash(this.hash);

    if (enc === 'hex')
      return hash.toString('hex');

    return hash;
  }

  async commit(batch) {
    if (this.db)
      assert(batch && typeof batch.put === 'function');

    const prev = this.originalRoot;

    this.store.start();

    const root = this._commit(this.root);

    await this.store.flush();
    await this.store.sync();

    this.root = root;
    this.originalRoot = root.hash(this.hash);

    if (batch) {
      assert(this.db);
      this.writeRoot(batch, root, prev);
    }

    return this.originalRoot;
  }

  _commit(node) {
    switch (node.type) {
      case NULL: {
        assert(node.index === 0);
        return node;
      }

      case INTERNAL: {
        node.left = this._commit(node.left);
        node.right = this._commit(node.right);

        if (node.index === 0)
          this.store.writeNode(node);

        assert(node.index !== 0);

        if (node.gen === this.cacheLimit)
          return new Hash(node.hash(this.hash), node.index, node.pos);

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

        return new Hash(node.hash(this.hash), node.index, node.pos);
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
    if (key instanceof Proof) {
      proof = key;
      key = root;
      root = this.originalRoot;
    }
    return verify(this.hash, this.bits, root, key, proof);
  }

  async getPrevious(rootHash) {
    assert(this.isHash(rootHash));
    const [prev] = await this.getRecord(rootHash);
    return prev;
  }

  async getPastRoots(rootHash) {
    assert(this.isHash(rootHash));

    const roots = [];

    for (;;) {
      if (rootHash.equals(this.hash.zero))
        break;

      roots.push(rootHash);

      rootHash = await this.getPrevious(rootHash);
    }

    return roots.reverse();
  }

  async compact(batch) {
    if (this.db)
      assert(batch && typeof batch.put === 'function');

    const node = await this.getRoot();

    if (node.isNull())
      return;

    const index = await this.store.advance();

    this.store.start();

    const root = await this._compact(node);
    assert(root.isHash());
    assert(root.hash(this.ctx).equals(node.hash(this.ctx)));

    await this.store.flush();
    await this.store.sync();
    await this.store.prune(index);

    if (batch) {
      assert(this.db);

      const roots = await this.getPastRoots(root.data);

      for (const hash of roots)
        batch.del(hash);

      this.writeRoot(batch, root, this.hash.zero);
    }

    this.root = root;
  }

  async _compact(node) {
    if (this.store.wb.written > (100 << 20)) {
      await this.store.flush();
      this.store.start();
    }

    switch (node.type) {
      case NULL: {
        return node;
      }

      case INTERNAL: {
        node.left = await this._compact(node.left);
        node.right = await this._compact(node.right);

        node.index = 0;
        node.pos = 0;

        this.store.writeNode(node);

        return new Hash(node.hash(this.hash), node.index, node.pos);
      }

      case LEAF: {
        node.index = 0;
        node.pos = 0;
        node.value = await node.getValue(this.store);
        this.store.writeValue(node);
        this.store.writeNode(node);
        return new Hash(node.hash(this.hash), node.index, node.pos);
      }

      case HASH: {
        node = await node.resolve(this.store);
        return this._compact(node);
      }
    }

    throw new AssertionError('Unknown node.');
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
