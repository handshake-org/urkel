/*!
 * tree.js - authenticated tree
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('assert');
const Path = require('path');
const common = require('../common');
const errors = require('../errors');
const nodes = require('./nodes');
const store = require('./store');

const {
  hasBit
} = common;

const {
  AssertionError
} = errors;

const {
  types,
  NIL,
  Internal,
  Leaf
} = nodes;

const {
  NULL,
  INTERNAL,
  LEAF,
  POSITION
} = types;

const {
  FileStore,
  MemoryStore
} = store;

/**
 * Index
 */

class Index {
  /**
   * Create a index.
   * @constructor
   * @param {String} prefix
   */

  constructor(prefix) {
    assert(!prefix || typeof prefix === 'string');

    let Store = FileStore;

    if (!prefix) {
      Store = MemoryStore;
      prefix = '/store';
    }

    this.prefix = Path.resolve(prefix, 'index');
    this.store = new Store(this.prefix);
    this.root = NIL;
  }

  isKey(key) {
    if (!Buffer.isBuffer(key))
      return false;
    return key.length >= 1 && key.length <= 64;
  }

  isValue(value) {
    if (!Buffer.isBuffer(value))
      return false;
    return value.length === 9;
  }

  async open(root) {
    this.root = await this.store.open();
  }

  async close() {
    this.root = NIL;
    return this.store.close();
  }

  async resolve(node) {
    if (node.isPosition())
      return this.store.readNode(node.index, node.pos, node.leaf);

    return node;
  }

  async get(key) {
    assert(this.isKey(key));

    let node = await this.resolve(this.root);
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

      assert(node.isInternal());

      // Internal node.
      if (hasBit(key, depth))
        node = await this.resolve(node.right);
      else
        node = await this.resolve(node.left);

      depth += 1;
    }

    if (node.isNull())
      return null;

    return node.value;
  }

  async insert(key, value) {
    assert(this.isKey(key));
    assert(this.isValue(value));

    const nodes = [];

    let node = await this.resolve(this.root);
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
          if (value.equals(node.value))
            return;

          // The branch doesn't grow.
          // Replace the current node.
          break;
        }

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

      assert(node.isInternal());

      // Internal node.
      if (hasBit(key, depth)) {
        nodes.push(node.left);
        node = await this.resolve(node.right);
      } else {
        nodes.push(node.right);
        node = await this.resolve(node.left);
      }

      depth += 1;
    }

    // Start at the leaf.
    next = new Leaf(key, value);

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      depth -= 1;

      if (hasBit(key, depth))
        next = new Internal(node, next);
      else
        next = new Internal(next, node);
    }

    const root = this._commit(next);

    await this.store.commit(root);

    this.root = root;
  }

  _commit(node) {
    switch (node.type()) {
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

        return node.toPosition();
      }

      case LEAF: {
        if (node.index === 0)
          this.store.writeNode(node);

        assert(node.index !== 0);

        return node.toPosition();
      }

      case POSITION: {
        assert(node.index !== 0);
        return node;
      }
    }

    throw new AssertionError('Unknown node.');
  }
}

(async () => {
  const crypto = require('crypto');
  const tree = new Index();
  const key = crypto.randomBytes(32);
  const value = crypto.randomBytes(9);

  await tree.open();
  await tree.insert(key, value);

  const v = await tree.get(key);
  console.log(value);
  console.log(v);
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
/*
 * Expose
 */

module.exports = Index;
