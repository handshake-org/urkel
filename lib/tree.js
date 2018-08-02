/*!
 * tree.js - authenticated tree
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('bsert');
const common = require('./common');
const errors = require('./errors');
const nodes = require('./nodes');
const Proof = require('./proof');
const store = require('./store');

const {
  hasBit,
  hashValue,
  randomPath
} = common;

const {
  MissingNodeError,
  AssertionError
} = errors;

const {
  types,
  Node,
  NIL,
  Internal,
  Leaf
} = nodes;

const {
  NULL,
  INTERNAL,
  LEAF,
  HASH
} = types;

const {
  FileStore,
  MemoryStore
} = store;

/*
 * Compat
 */

const asyncIterator = Symbol.asyncIterator || 'asyncIterator';

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
   */

  constructor(hash, bits, prefix) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(!prefix || typeof prefix === 'string');

    let Store = FileStore;

    if (!prefix) {
      Store = MemoryStore;
      prefix = '/store';
    }

    this.hash = hash;
    this.bits = bits;
    this.prefix = prefix || null;
    this.store = new Store(prefix, hash, bits);
    this.root = NIL;
  }

  isKey(key) {
    if (!Buffer.isBuffer(key))
      return false;
    return key.length === (this.bits >>> 3);
  }

  isValue(value) {
    if (!Buffer.isBuffer(value))
      return false;
    return value.length <= 0xffff;
  }

  isHash(hash) {
    if (!Buffer.isBuffer(hash))
      return false;
    return hash.length === this.hash.size;
  }

  hashValue(key, value) {
    return hashValue(this.hash, key, value);
  }

  rootHash() {
    return this.root.hash(this.hash);
  }

  async getRoot() {
    return this.root;
  }

  async open(root) {
    this.root = await this.store.open();

    if (root)
      await this.inject(root);
  }

  async close() {
    this.root = NIL;
    return this.store.close();
  }

  async inject(root) {
    this.root = await this.getHistory(root);
  }

  async getHistory(root) {
    assert(this.isHash(root));
    return this.store.getHistory(root);
  }

  async resolve(node) {
    return this.store.resolve(node);
  }

  async retrieve(node) {
    return this.store.retrieve(node);
  }

  async _get(root, key) {
    let node = root;
    let depth = 0;

    for (;;) {
      switch (node.type()) {
        case NULL: {
          // Empty (sub)tree.
          return null;
        }

        case INTERNAL: {
          if (depth === this.bits) {
            throw new MissingNodeError({
              rootHash: root.hash(this.hash),
              key,
              depth
            });
          }

          // Internal node.
          if (hasBit(key, depth))
            node = node.right;
          else
            node = node.left;

          depth += 1;

          break;
        }

        case LEAF: {
          // Prefix collision.
          if (!key.equals(node.key))
            return null;

          return this.retrieve(node);
        }

        case HASH: {
          node = await this.resolve(node);
          break;
        }

        default: {
          throw new AssertionError('Unknown node type.');
        }
      }
    }
  }

  async get(key) {
    assert(this.isKey(key));
    return this._get(this.root, key);
  }

  async _insert(root, key, value) {
    const leaf = this.hashValue(key, value);
    const nodes = [];

    let node = root;
    let depth = 0;

    // Traverse bits left to right.
outer:
    for (;;) {
      switch (node.type()) {
        case NULL: {
          // Empty (sub)tree.
          // Replace the empty node.
          break outer;
        }

        case INTERNAL: {
          if (depth === this.bits) {
            throw new MissingNodeError({
              rootHash: root.hash(this.hash),
              key,
              depth
            });
          }

          // Internal node.
          if (hasBit(key, depth)) {
            nodes.push(node.left);
            node = node.right;
          } else {
            nodes.push(node.right);
            node = node.left;
          }

          depth += 1;

          break;
        }

        case LEAF: {
          // Current key.
          if (key.equals(node.key)) {
            // Exact leaf already exists.
            if (leaf.equals(node.data))
              return root;

            // The branch doesn't grow.
            // Replace the current node.
            break outer;
          }

          assert(depth !== this.bits);

          // Insert placeholder leaves to grow
          // the branch if we have bit collisions.
          while (hasBit(key, depth) === hasBit(node.key, depth)) {
            // Child-less sibling.
            nodes.push(NIL);
            depth += 1;
          }

          // Leaf is our sibling.
          nodes.push(node);
          depth += 1;

          break outer;
        }

        case HASH: {
          node = await this.resolve(node);
          break;
        }

        default: {
          throw new AssertionError('Unknown node type.');
        }
      }
    }

    // Start at the leaf.
    root = new Leaf(leaf, key, value);

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      depth -= 1;

      if (hasBit(key, depth))
        root = new Internal(node, root);
      else
        root = new Internal(root, node);
    }

    return root;
  }

  async _remove(root, key) {
    const nodes = [];

    let node = root;
    let depth = 0;

    // Traverse bits left to right.
outer:
    for (;;) {
      switch (node.type()) {
        case NULL: {
          // Empty (sub)tree.
          return root;
        }

        case INTERNAL: {
          if (depth === this.bits) {
            throw new MissingNodeError({
              rootHash: root.hash(this.hash),
              key,
              depth
            });
          }

          // Internal node.
          if (hasBit(key, depth)) {
            nodes.push(node.left);
            node = node.right;
          } else {
            nodes.push(node.right);
            node = node.left;
          }

          depth += 1;

          break;
        }

        case LEAF: {
          // Not our key.
          if (!key.equals(node.key))
            return root;

          // Root can be a leaf.
          if (depth === 0) {
            // Remove the root.
            return NIL;
          }

          // Sibling.
          root = nodes[depth - 1];

          // Shrink the subtree if we're a leaf.
          if (root.leaf) {
            nodes.pop();
            depth -= 1;

            while (depth > 0) {
              const side = nodes[depth - 1];

              if (!side.isNull())
                break;

              nodes.pop();
              depth -= 1;
            }
          } else {
            root = NIL;
          }

          break outer;
        }

        case HASH: {
          node = await this.resolve(node);
          break;
        }

        default: {
          throw new AssertionError('Unknown node type.');
        }
      }
    }

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      depth -= 1;

      if (hasBit(key, depth))
        root = new Internal(node, root);
      else
        root = new Internal(root, node);
    }

    return root;
  }

  async _commit(node) {
    const root = this._write(node);

    await this.store.commit(root);

    this.root = root;

    return root;
  }

  _write(node) {
    switch (node.type()) {
      case NULL: {
        assert(node.index === 0);
        return node;
      }

      case INTERNAL: {
        node.left = this._write(node.left);
        node.right = this._write(node.right);

        if (node.index === 0) {
          this.store.writeNode(node);

          // if (this.store.needsFlush())
          //   await this.store.flush();
        }

        assert(node.index !== 0);

        return node.toHash(this.hash);
      }

      case LEAF: {
        if (node.index === 0) {
          assert(node.value);

          this.store.writeValue(node);
          this.store.writeNode(node);

          // if (this.store.needsFlush())
          //   await this.store.flush();
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

  async prove(key) {
    assert(this.isKey(key));
    return this._prove(this.root, key);
  }

  async _prove(root, key) {
    const proof = new Proof();

    let node = root;
    let depth = 0;

    // Traverse bits left to right.
outer:
    for (;;) {
      switch (node.type()) {
        case NULL: {
          // Empty (sub)tree.
          break outer;
        }

        case INTERNAL: {
          if (depth === this.bits) {
            throw new MissingNodeError({
              rootHash: root.hash(this.hash),
              key,
              depth
            });
          }

          // Internal node.
          if (hasBit(key, depth)) {
            const hash = node.left.hash(this.hash);
            proof.push(hash);
            node = node.right;
          } else {
            const hash = node.right.hash(this.hash);
            proof.push(hash);
            node = node.left;
          }

          depth += 1;

          break;
        }

        case LEAF: {
          const value = await this.retrieve(node);

          if (node.key.equals(key)) {
            proof.type = Proof.TYPE_EXISTS;
            proof.value = value;
          } else {
            proof.type = Proof.TYPE_COLLISION;
            proof.key = Buffer.from(node.key);
            proof.hash = this.hash.digest(value);
          }

          break outer;
        }

        case HASH: {
          node = await this.resolve(node);
          break;
        }

        default: {
          throw new AssertionError('Unknown node type.');
        }
      }
    }

    return proof;
  }

  async compact() {
    const prefix = randomPath(this.prefix);
    const store = this.store.clone(prefix);

    await store.open();

    const root = await this._compact(this.root, store);

    await store.commit(root);
    await store.close();

    await this.store.close();
    await this.store.destroy();

    await store.rename(this.prefix);
    await store.open();

    this.store = store;
    this.root = root;
  }

  async _compact(node, store) {
    switch (node.type()) {
      case NULL: {
        return node;
      }

      case INTERNAL: {
        node.left = await this._compact(node.left, store);
        node.right = await this._compact(node.right, store);

        node.index = 0;
        node.pos = 0;

        store.writeNode(node);

        if (store.needsFlush())
          await store.flush();

        return node.toHash(this.hash);
      }

      case LEAF: {
        node.index = 0;
        node.pos = 0;
        node.value = await this.retrieve(node);

        store.writeValue(node);
        store.writeNode(node);

        if (store.needsFlush())
          await store.flush();

        return node.toHash(this.hash);
      }

      case HASH: {
        const rn = await this.resolve(node);
        return this._compact(rn, store);
      }
    }

    throw new AssertionError('Unknown node.');
  }

  snapshot(hash) {
    let root = null;

    if (hash == null) {
      hash = this.rootHash();
      root = this.root;
    }

    return new Snapshot(this, hash, root);
  }

  transaction() {
    return new Transaction(this);
  }

  iterator(read = true) {
    const iter = new Iterator(this, this, read);
    iter.root = this.root;
    return iter;
  }

  [asyncIterator]() {
    return this.entries();
  }

  keys() {
    const iter = this.iterator(false);
    return iter.keys();
  }

  values() {
    const iter = this.iterator(true);
    return iter.values();
  }

  entries() {
    const iter = this.iterator(true);
    return iter.entries();
  }

  batch() {
    return this.transaction();
  }

  txn() {
    return this.transaction();
  }
}

/**
 * Snapshot
 */

class Snapshot {
  constructor(tree, hash, root) {
    assert(tree instanceof Tree);
    assert(tree.isHash(hash));
    assert(root === null || (root instanceof Node));

    this.tree = tree;
    this.hash = hash;
    this.root = root;
  }

  rootHash() {
    return this.hash;
  }

  async getRoot() {
    if (!this.root)
      this.root = await this.tree.getHistory(this.hash);
    return this.root;
  }

  async inject(root) {
    this.root = await this.tree.getHistory(root);
    this.hash = root;
  }

  async get(key) {
    assert(this.tree.isKey(key));

    if (!this.root)
      this.root = await this.getRoot();

    return this.tree._get(this.root, key);
  }

  async prove(key) {
    if (!this.root)
      this.root = await this.getRoot();

    return this.tree._prove(this.root, key);
  }

  iterator(read = true) {
    const iter = new Iterator(this.tree, this, read);
    iter.root = this.root;
    return iter;
  }

  [asyncIterator]() {
    return this.entries();
  }

  keys() {
    const iter = this.iterator(false);
    return iter.keys();
  }

  values() {
    const iter = this.iterator(true);
    return iter.values();
  }

  entries() {
    const iter = this.iterator(true);
    return iter.entries();
  }
}

/**
 * Transaction
 */

class Transaction extends Snapshot {
  constructor(tree) {
    assert(tree instanceof Tree);
    super(tree, tree.rootHash(), tree.root);
  }

  rootHash() {
    return this.root.hash(this.tree.hash);
  }

  async getRoot() {
    return this.root;
  }

  async insert(key, value) {
    assert(this.tree.isKey(key));
    assert(this.tree.isValue(value));
    this.root = await this.tree._insert(this.root, key, value);
  }

  async remove(key) {
    assert(this.tree.isKey(key));
    this.root = await this.tree._remove(this.root, key);
  }

  async commit() {
    this.root = await this.tree._commit(this.root);
    this.hash = this.rootHash();
    return this.hash;
  }

  clear() {
    this.root = this.tree.root;
    return this;
  }
}

/**
 * Iterator
 */

class Iterator {
  constructor(tree, parent, read) {
    assert(tree instanceof Tree);
    assert(parent && typeof parent.getRoot === 'function');
    assert(typeof read === 'boolean');

    this.tree = tree;
    this.parent = parent;
    this.read = read;
    this.root = null;
    this.stack = [];
    this.done = false;
    this.node = NIL;
    this.key = null;
    this.value = null;
  }

  [asyncIterator]() {
    return this.entries();
  }

  keys() {
    return new AsyncIterator(this, 0);
  }

  values() {
    return new AsyncIterator(this, 1);
  }

  entries() {
    return new AsyncIterator(this, 2);
  }

  push(node, depth) {
    const state = new IteratorState(node, depth);
    return this.stack.push(state);
  }

  pop() {
    assert(this.stack.length > 0);
    return this.stack.pop();
  }

  top() {
    assert(this.stack.length > 0);
    return this.stack[this.stack.length - 1];
  }

  length() {
    return this.stack.length;
  }

  async seek() {
    if (!this.root)
      this.root = await this.parent.getRoot();

    this.node = NIL;

    if (this.done)
      return false;

    if (this.length() === 0) {
      this.push(this.root, 0);
    } else {
      this.pop();

      if (this.length() === 0) {
        this.done = true;
        return false;
      }
    }

outer:
    for (;;) {
      const parent = this.top();
      const {node, depth} = parent;

      switch (node.type()) {
        case NULL: {
          this.node = node;
          break outer;
        }

        case INTERNAL: {
          if (parent.child >= 1)
            break outer;

          parent.child += 1;

          if (parent.child)
            this.push(node.right, depth + 1);
          else
            this.push(node.left, depth + 1);

          break;
        }

        case LEAF: {
          this.node = node;
          break outer;
        }

        case HASH: {
          if (parent.child >= 0)
            break outer;

          parent.child += 1;

          const rn = await this.tree.resolve(node);
          this.push(rn, depth);
          break;
        }

        default: {
          throw new AssertionError('Unknown node.');
        }
      }
    }

    return true;
  }

  async next() {
    for (;;) {
      if (!await this.seek())
        break;

      if (!this.node.isLeaf())
        continue;

      this.key = this.node.key;
      this.value = this.node.value;

      if (this.read && !this.value)
        this.value = await this.tree.retrieve(this.node);

      return true;
    }

    this.key = null;
    this.value = null;

    return false;
  }
}

/**
 * IteratorState
 */

class IteratorState {
  constructor(node, depth) {
    this.node = node;
    this.depth = depth;
    this.child = -1;
  }
}

/**
 * AsyncIterator
 */

class AsyncIterator {
  constructor(iter, type) {
    assert(iter instanceof Iterator);
    assert((type & 3) === type);
    assert(type < 3);

    this.iter = iter;
    this.type = type;
  }

  async next() {
    if (!await this.iter.next())
      return { value: undefined, done: true };

    switch (this.type) {
      case 0:
        return { value: this.iter.key, done: false };
      case 1:
        return { value: this.iter.value, done: false };
      case 2:
        return { value: [this.iter.key, this.iter.value], done: false };
      default:
        throw new AssertionError('Bad value mode.');
    }
  }
}

/*
 * Expose
 */

module.exports = Tree;
