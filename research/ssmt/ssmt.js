/*!
 * ssmt.js - simple sparse merkle tree (unoptimized proof-of-concept)
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 *
 * Sparse Merkle Trees:
 *   https://eprint.iacr.org/2016/683
 *
 * Parts of this software are based on
 * Vitalik Buterin's proof-of-concept:
 *   https://github.com/ethereum/research/blob/master/trie_research/bintrie2/new_bintrie.py
 *
 * See Also:
 *   https://ethresear.ch/t/data-availability-proof-friendly-state-tree-transitions/1453/7
 */

'use strict';

const assert = require('assert');
const sha256 = require('bcrypto/lib/sha256');

/*
 * Constants
 */

const ZERO_HASH = Buffer.alloc(32, 0x00);

const EMPTY_ROOT = Buffer.from(
  '8a95af78e69c7a4c3949314ada1a22aa63a108461c6258739598107155b2d85b',
  'hex');

const defaults = [];

for (let i = 0; i < 256; i++)
  defaults.push(ZERO_HASH);

for (let i = 255; i >= 1; i--) {
  const hash = defaults[i];
  defaults[i - 1] = sha256.root(hash, hash);
}

/**
 * SSMT
 */

class SSMT {
  constructor() {
    this.db = new DB();
    this.root = EMPTY_ROOT;
    this.ensured = false;
  }

  ensure() {
    if (this.ensured)
      return;

    this.ensured = true;

    let hash = ZERO_HASH;

    for (let i = 0; i < 256; i++) {
      const key = sha256.root(hash, hash);
      const value = [hash, hash];
      this.db.set(key, value);
      hash = key;
    }

    this.root = hash;
  }

  get(key) {
    let next = this.root;

    // Traverse bits left to right.
    for (let i = 0; i < 256; i++) {
      const node = this.db.get(next);
      const bit = hasBit(key, i);
      next = node[bit];
    }

    return next;
  }

  insert(key, value) {
    this.ensure();

    const nodes = [];

    let next = this.root;

    // Traverse bits left to right.
    for (let i = 0; i < 256; i++) {
      const node = this.db.get(next);
      const bit = hasBit(key, i);
      nodes.push(node[bit ^ 1]);
      next = node[bit];
    }

    next = value;

    // Traverse bits right to left.
    for (let i = 0; i < 256; i++) {
      const node = nodes.pop();

      if (hasBit(key, 255 - i)) {
        const k = sha256.root(node, next);
        this.db.set(k, [node, next]);
        next = k;
      } else {
        const k = sha256.root(next, node);
        this.db.set(k, [next, node]);
        next = k;
      }
    }

    this.root = next;

    return next;
  }

  prove(root, key) {
    const nodes = [];

    let next = root;

    for (let i = 0; i < 256; i++) {
      const node = this.db.get(next);
      const bit = hasBit(key, i);
      nodes.push(node[bit ^ 1]);
      next = node[bit];
    }

    const map = Buffer.alloc(32, 0x00);
    const proof = [];

    proof.push(map);

    for (let i = 0; i < 256; i++) {
      const node = nodes[i];

      if (node.equals(defaults[i]))
        map[i >>> 3] ^= 1 << (i & 7);
      else
        proof.push(node);
    }

    return proof;
  }

  verify(proof, root, key, value) {
    const map = proof[0];
    const nodes = [];

    let p = 1;

    for (let i = 0; i < 256; i++) {
      if (map[i >>> 3] & (1 << (i & 7))) {
        nodes.push(defaults[i]);
      } else {
        nodes.push(proof[p]);
        p += 1;
      }
    }

    let next = value;

    // Traverse bits right to left.
    for (let i = 0; i < 256; i++) {
      const node = nodes.pop();

      if (hasBit(key, 255 - i))
        next = sha256.root(node, next);
      else
        next = sha256.root(next, node);
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

module.exports = SSMT;
