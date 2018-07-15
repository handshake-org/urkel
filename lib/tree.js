/*!
 * tree.js - authenticated tree
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('assert');
const common = require('./common');
const errors = require('./errors');
const nodes = require('./nodes');
const proof = require('./proof');
const store = require('./store');

const {
  hasBit,
  hashValue,
  fromRecord,
  toRecord,
  randomPath
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
 * Tree
 */

class Tree {
  /**
   * Create a tree.
   * @constructor
   * @param {Object} hash
   * @param {Number} bits
   * @param {String} prefix
   * @param {Object} [db=null]
   * @param {Number} [depth=4]
   */

  constructor(hash, bits, prefix, db, depth) {
    if (depth == null)
      depth = 4;

    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(!prefix || typeof prefix === 'string');
    assert(!db || typeof db === 'object');
    assert((depth >>> 0) === depth);

    let Store = FileStore;

    if (!prefix) {
      Store = MemoryStore;
      prefix = '/store';
    }

    this.hash = hash;
    this.bits = bits;
    this.prefix = prefix || null;
    this.db = db || null;
    this.store = new Store(prefix, hash, bits, !db);
    this.originalRoot = this.hash.zero;
    this.root = NIL;
    this.cacheDepth = depth;
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

  hashValue(key, value) {
    return hashValue(this.hash, key, value);
  }

  async open(root) {
    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    await this.store.open();

    // Try to retrieve best state.
    if (!root) {
      if (this.db)
        root = await this.db.get(STATE_KEY);
      else
        root = await this.store.getRootHash();
    }

    if (root) {
      this.root = await this.getHistory(root);
      this.originalRoot = root;
    }
  }

  async close() {
    this.root = NIL;
    this.originalRoot = this.hash.zero;

    return this.store.close();
  }

  async getRecord(root) {
    if (!this.db) {
      const node = await this.store.getHistory(root);
      return [node.index, node.pos];
    }

    const raw = await this.db.get(root);

    if (!raw) {
      throw new MissingNodeError({
        rootHash: root,
        nodeHash: root
      });
    }

    return fromRecord(raw);
  }

  putRecord(batch, root) {
    assert(this.db);
    assert(batch && typeof batch.put === 'function');

    if (root.isNull()) {
      batch.del(STATE_KEY);
      return;
    }

    const hash = root.hash(this.hash);
    const {index, pos} = root;

    batch.put(hash, toRecord(index, pos));
    batch.put(STATE_KEY, hash);
  }

  async getHistory(root) {
    if (root == null)
      root = this.originalRoot;

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

      if (this.root.index !== 0)
        return this.root.toHash(this.hash);
    }

    const [index, pos] = await this.getRecord(root);

    return new Hash(root, index, pos);
  }

  async getRoot(root) {
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
    const leaf = this.hashValue(key, value);
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
        if (key.equals(node.key)) {
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
        while (hasBit(key, depth) === hasBit(node.key, depth)) {
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
        // Not our key.
        if (!key.equals(node.key))
          return root;

        // Root can be a leaf.
        if (depth === 0) {
          // Remove the root.
          return NIL;
        }

        // Sibling.
        let s = nodes.pop();
        depth -= 1;

        // One extra disk read.
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
    const root = this._commit(this.root, 0);

    await this.store.commit(root);

    this.root = root;
    this.originalRoot = root.hash(this.hash);

    if (this.db && batch)
      this.putRecord(batch, root);

    return this.originalRoot;
  }

  _commit(node, depth) {
    // if (this.store.needsFlush())
    //   await this.store.flush();

    switch (node.type()) {
      case NULL: {
        assert(node.index === 0);
        return node;
      }

      case INTERNAL: {
        node.left = this._commit(node.left, depth + 1);
        node.right = this._commit(node.right, depth + 1);

        if (node.index === 0)
          this.store.writeNode(node);

        assert(node.index !== 0);

        if (depth < this.cacheDepth)
          return node;

        return node.toHash(this.hash);
      }

      case LEAF: {
        if (node.index === 0) {
          assert(node.value);
          this.store.writeValue(node);
          this.store.writeNode(node);
        }

        assert(node.index !== 0);

        return node.toHash(this.hash);
      }

      case HASH: {
        assert(node.index !== 0);
        return node;
      }
    }

    throw new AssertionError('Unknown node.');
  }

  snapshotSync() {
    const {hash, bits, prefix, db, cacheDepth} = this;
    const tree = new this.constructor(hash, bits, prefix, db, cacheDepth);

    tree.store = this.store;
    tree.originalRoot = this.originalRoot;

    if (!this.root.isNull())
      tree.root = this.root.toHash(this.hash);

    return tree;
  }

  async snapshot(root) {
    if (root == null)
      root = this.originalRoot;

    const {hash, bits, prefix, db, cacheDepth} = this;
    const tree = new this.constructor(hash, bits, prefix, db, cacheDepth);

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

  async keys(iter) {
    return this.iterate(false, iter);
  }

  async values(iter) {
    return this.iterate(true, iter);
  }

  async iterate(values, iter) {
    assert(typeof values === 'boolean');
    assert(typeof iter === 'function');

    const node = await this.getRoot();

    return this._iterate(node, values, iter);
  }

  async _iterate(node, values, iter) {
    switch (node.type()) {
      case NULL: {
        return undefined;
      }

      case INTERNAL: {
        await this._iterate(node.left, values, iter);
        await this._iterate(node.right, values, iter);
        return undefined;
      }

      case LEAF: {
        let result = null;

        if (values) {
          const value = await node.getValue(this.store);
          result = iter(node.key, value);
        } else {
          result = iter(node.key);
        }

        if (result instanceof Promise)
          await result;

        return undefined;
      }

      case HASH: {
        const r = await node.resolve(this.store);
        return this._iterate(r, values, iter);
      }
    }

    throw new AssertionError('Unknown node.');
  }

  async compact(batch) {
    const Store = this.store.constructor;
    const prefix = randomPath(this.prefix);
    const {hash, bits} = this;

    const node = await this.getRoot();

    const store = new Store(prefix, hash, bits, !this.db);
    await store.open();

    const root = await this._compact(node, store, 0);

    await store.commit(root);
    await store.close();

    // Need lock here.
    await this.store.close();
    await this.store.destroy();
    await store.rename(this.prefix);
    await store.open();

    if (this.db && batch)
      this.putRecord(batch, root);

    this.store = store;
    this.root = root;
  }

  async _compact(node, store, depth) {
    if (store.needsFlush())
      await store.flush();

    switch (node.type()) {
      case NULL: {
        return node;
      }

      case INTERNAL: {
        node.left = await this._compact(node.left, store, depth + 1);
        node.right = await this._compact(node.right, store, depth + 1);

        node.index = 0;
        node.pos = 0;

        store.writeNode(node);

        return node.toHash(this.hash);
      }

      case LEAF: {
        node.index = 0;
        node.pos = 0;
        node.value = await node.getValue(this.store);

        store.writeValue(node);
        store.writeNode(node);

        return node.toHash(this.hash);
      }

      case HASH: {
        const r = await node.resolve(this.store);
        return this._compact(r, store, depth);
      }
    }

    throw new AssertionError('Unknown node.');
  }
}

/*
 * Expose
 */

module.exports = Tree;
