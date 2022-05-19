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
const {AssertionError} = errors;

const {
  hasBit,
  hashValue,
  randomPath
} = common;

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
   * @param {Object} options
   * @param {Object} options.hash
   * @param {Number} options.bits
   * @param {String} options.prefix
   * @param {Boolean} options.cacheOnly - Only read disk on open.
   *                                      Depend on the inititial root cache.
   * @param {Number} options.initCacheSize - How many root hashes to index on
   *                                         open.
   *                                          - `-1` means all.
   *                                          - `0` means disabled.
   */

  constructor(options) {
    this.options = new TreeOptions(options);
    this.hash = this.options.hash;
    this.bits = this.options.bits;
    this.prefix = this.options.prefix;;
    this.store = new this.options.Store(this.options);
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
    return value.length < 0x400;
  }

  isHash(hash) {
    if (!Buffer.isBuffer(hash))
      return false;
    return hash.length === this.hash.size;
  }

  leaf(key, value) {
    const hash = hashValue(this.hash, key, value);
    return new Leaf(hash, key, value);
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
          // Empty tree.
          return null;
        }

        case INTERNAL: {
          if (!node.prefix.has(key, depth))
            return null;

          depth += node.prefix.size;

          const bit = hasBit(key, depth);

          node = node.get(bit);
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

  async _insert(node, key, value, depth) {
    switch (node.type()) {
      case NULL: {
        // Replace the empty node.
        return this.leaf(key, value);
      }

      case INTERNAL: {
        const bits = node.prefix.count(key, depth);

        depth += bits;

        const bit = hasBit(key, depth);

        if (bits !== node.prefix.size) {
          const leaf = this.leaf(key, value);
          const [front, back] = node.prefix.split(bits);
          const child = new Internal(back, node.left, node.right);
          return Internal.from(front, leaf, child, bit);
        }

        const x = node.get(bit);
        const y = node.get(bit ^ 1);
        const z = await this._insert(x, key, value, depth + 1);

        if (!z)
          return null;

        return Internal.from(node.prefix, z, y, bit);
      }

      case LEAF: {
        // Current key.
        if (key.equals(node.key)) {
          // Exact leaf already exists.
          if (node.value && value.equals(node.value))
            return null;

          // The branch doesn't grow.
          // Replace the current node.
          return this.leaf(key, value);
        }

        assert(depth !== this.bits);

        const prefix = node.bits.collide(key, depth);

        depth += prefix.size;

        const leaf = this.leaf(key, value);
        const bit = hasBit(key, depth);

        return Internal.from(prefix, leaf, node, bit);
      }

      case HASH: {
        const rn = await this.resolve(node);
        return this._insert(rn, key, value, depth);
      }

      default: {
        throw new AssertionError('Unknown node.');
      }
    }
  }

  async _remove(node, key, depth) {
    switch (node.type()) {
      case NULL: {
        // Empty tree.
        return null;
      }

      case INTERNAL: {
        if (!node.prefix.has(key, depth))
          return null;

        depth += node.prefix.size;

        const bit = hasBit(key, depth);
        const x = node.get(bit);
        const y = node.get(bit ^ 1);
        const z = await this._remove(x, key, depth + 1);

        if (!z)
          return null;

        if (z.isNull()) {
          const side = await this.resolve(y);

          if (side.isInternal()) {
            const prefix = node.prefix.join(side.prefix, bit ^ 1);
            return new Internal(prefix, side.left, side.right);
          }

          return side;
        }

        return Internal.from(node.prefix, z, y, bit);
      }

      case LEAF: {
        // Not our key.
        if (!key.equals(node.key))
          return null;

        return NIL;
      }

      case HASH: {
        const rn = await this.resolve(node);
        return this._remove(rn, key, depth);
      }

      default: {
        throw new AssertionError('Unknown node.');
      }
    }
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
        return node;
      }

      case INTERNAL: {
        node.left = this._write(node.left);
        node.right = this._write(node.right);

        if (!node.ptr) {
          this.store.writeNode(node);

          // if (this.store.needsFlush())
          //   await this.store.flush();
        }

        assert(node.ptr);

        return node.toHash(this.hash);
      }

      case LEAF: {
        if (!node.ptr) {
          assert(node.value);

          this.store.writeValue(node);
          this.store.writeNode(node);

          // if (this.store.needsFlush())
          //   await this.store.flush();
        }

        assert(node.ptr);

        return node.toHash(this.hash);
      }

      case HASH: {
        assert(node.ptr);
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
          proof.type = Proof.TYPE_DEADEND;
          proof.depth = depth;
          break outer;
        }

        case INTERNAL: {
          if (!node.prefix.has(key, depth)) {
            const left = node.left.hash(this.hash);
            const right = node.right.hash(this.hash);

            proof.type = Proof.TYPE_SHORT;
            proof.depth = depth;
            proof.prefix = node.prefix.clone();
            proof.left = Buffer.from(left);
            proof.right = Buffer.from(right);

            break outer;
          }

          depth += node.prefix.size;

          const bit = hasBit(key, depth);
          const side = node.get(bit ^ 1);
          const hash = side.hash(this.hash);

          proof.push(node.prefix.clone(), hash);

          node = node.get(bit);
          depth += 1;

          break;
        }

        case LEAF: {
          const value = await this.retrieve(node);

          if (node.key.equals(key)) {
            proof.type = Proof.TYPE_EXISTS;
            proof.depth = depth;
            proof.value = value;
          } else {
            proof.type = Proof.TYPE_COLLISION;
            proof.depth = depth;
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

  async compact(tmpPrefix) {
    if (!tmpPrefix)
      tmpPrefix = randomPath(this.prefix);

    const store = this.store.clone(tmpPrefix);

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

        node.ptr = null;

        store.writeNode(node);

        if (store.needsFlush())
          await store.flush();

        return node.toHash(this.hash);
      }

      case LEAF: {
        node.ptr = null;
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

class TreeOptions {
  /**
   * Create TreeOptions
   * @param {Object} options
   */

  constructor(options) {
    this.hash = null;
    this.bits = null;
    this.Store = MemoryStore;
    this.prefix = '/store';
    this.memory = true;

    this.cacheOnly = false;
    this.initCacheSize = 0;

    this.fromOptions(options);
  }

  /**
   * Init options.
   * @param {Object} options
   * @returns {TreeOptions}
   */

  fromOptions(options) {
    assert(options, 'Tree requires options.');
    assert(options.hash && typeof options.hash.digest === 'function',
      'Tree requires proper hash with digest.');
    assert((options.bits >>> 0) === options.bits,
      'Bits must be a 32 bit integer.');
    assert(options.bits > 0 && (options.bits & 7) === 0);

    this.hash = options.hash;
    this.bits = options.bits;

    if (options.prefix != null) {
      assert(typeof options.prefix === 'string',
        'options.prefix must be a string');
      this.Store = FileStore;
      this.prefix = options.prefix;
      this.memory = false;
    }

    if (options.cacheOnly != null) {
      assert(typeof options.cacheOnly === 'boolean');
      this.cacheOnly = options.cacheOnly;
    }

    if (options.initCacheSize != null) {
      assert(typeof options.initCacheSize === 'number');
      assert(options.initCacheSize >= -1);
      this.initCacheSize = options.initCacheSize;
    }

    return this;
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

    const root = await this.tree._insert(this.root, key, value, 0);

    if (root)
      this.root = root;
  }

  async remove(key) {
    assert(this.tree.isKey(key));

    const root = await this.tree._remove(this.root, key, 0);

    if (root)
      this.root = root;
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
            this.push(node.right, depth + node.prefix.size + 1);
          else
            this.push(node.left, depth + node.prefix.size + 1);

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
