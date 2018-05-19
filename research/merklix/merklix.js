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
const {sha256} = require('../../test/util/util');

/*
 * Constants
 */

const ZERO_HASH = Buffer.alloc(32, 0x00);
ZERO_HASH[31] |= 1;

/**
 * Merklix
 */

class Merklix {
  constructor() {
    this.db = new DB();
    this.root = ZERO_HASH;
    this.depth = 0;
  }

  leaf(hash) {
    if (hash[31] & 1)
      return hash;
    const buf = Buffer.allocUnsafe(32);
    hash.copy(buf, 0);
    buf[31] |= 1;
    return buf;
  }

  hashLeaf(data) {
    const h = sha256.digest(data);
    h[31] |= 1;
    return h;
  }

  hashRoot(left, right) {
    const h = sha256.root(left, right);
    h[31] &= ~1;
    return h;
  }

  get(key) {
    let next = this.root;
    let depth = 0;

    // Traverse bits left to right.
    for (;;) {
      const node = this.db.get(next);

      // Empty (sub)tree.
      if (!node) {
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

  insert(key, value) {
    const nodes = [];

    let next = this.root;
    let depth = 0;

    // Traverse bits left to right.
    for (;;) {
      const node = this.db.get(next);

      // Empty (sub)tree.
      if (!node) {
        nodes.push(ZERO_HASH);
        break;
      }

      // Leaf node.
      if (node.length === 1) {
        // Current key.
        const other = node[0];

        if (key.equals(other)) {
          // Nothing. The branch doesn't grow.
          break;
        }

        // Insert dummy nodes to artificially grow
        // the branch if we have bit collisions.
        // Is there a better way? Not sure.
        // Potential DoS vector.
        while (hasBit(other, depth) === hasBit(key, depth)) {
          // Child-less sidenode.
          nodes.push(ZERO_HASH);
          depth += 1;
        }

        nodes.push(next);

        break;
      }

      // Prune old nodes.
      this.db.del(next);

      // Internal node.
      const bit = hasBit(key, depth);
      nodes.push(node[bit ^ 1]);
      next = node[bit];
      depth += 1;
    }

    // Track max depth.
    if (depth > this.depth)
      this.depth = depth;

    next = this.leaf(value);

    // Store the key for
    // comparisons later (see above).
    this.db.set(next, [key]);

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      if (hasBit(key, depth)) {
        const k = this.hashRoot(node, next);
        this.db.set(k, [node, next]);
        next = k;
      } else {
        const k = this.hashRoot(next, node);
        this.db.set(k, [next, node]);
        next = k;
      }

      depth -= 1;
    }

    this.root = next;

    return next;
  }

  prove(root, key) {
    const nodes = [];

    let next = root;
    let depth = 0;
    let exists = false;

    // Traverse bits left to right.
    for (;;) {
      const node = this.db.get(next);

      // Empty (sub)tree.
      if (!node) {
        nodes.push(ZERO_HASH);
        exists = false;
        break;
      }

      // Leaf node.
      if (node.length === 1) {
        nodes.push(next);
        // Could be a prefix collision.
        // i.e. What we need provable leaves for.
        exists = key.equals(node[0]);
        break;
      }

      // Internal node.
      const bit = hasBit(key, depth);
      nodes.push(node[bit ^ 1]);
      next = node[bit];
      depth += 1;
    }

    return {
      exists,
      nodes
    };
  }

  verify(proof, root, key, value) {
    const nodes = proof.nodes;

    if (nodes.length === 0)
      return false;

    let next = nodes[nodes.length - 1];

    // Ensure this is a leaf.
    if (!(next[31] & 1))
      return false;

    // Check for existence/non-existence.
    if (value) {
      if (!proof.exists)
        return false;

      if (!this.leaf(value).equals(next))
        return false;
    } else {
      if (proof.exists)
        return false;

      // Leaf doesn't matter.
    }

    let depth = nodes.length - 2;

    // Traverse bits right to left.
    while (depth >= 0) {
      const node = nodes[depth];

      if (hasBit(key, depth))
        next = this.hashRoot(node, next);
      else
        next = this.hashRoot(next, node);

      depth -= 1;
    }

    return root.equals(next);
  }
}

/**
 * DB
 */

class DB {
  constructor() {
    this.map = new Map();
  }

  get(key) {
    return this.map.get(key.toString('hex')) || null;
  }

  set(key, value) {
    this.map.set(key.toString('hex'), value);
  }

  del(key) {
    this.map.delete(key.toString('hex'));
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
