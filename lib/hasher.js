/*!
 * hasher.js - patricia merkle trie hasher
 * Copyright (c) 2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 *
 * Patricia Merkle Tries:
 *   https://github.com/ethereum/wiki/wiki/Patricia-Tree
 *
 * Parts of this software are based on go-ethereum:
 *   Copyright (C) 2014 The go-ethereum Authors.
 *   https://github.com/ethereum/go-ethereum/tree/master/trie
 */

'use strict';

const assert = require('assert');
const nodes = require('./nodes');
const {HashNode} = nodes;
const {SHORTNODE, FULLNODE, VALUENODE} = nodes.types;

/**
 * Hasher
 */

class Hasher {
  /**
   * Create a hasher.
   * @constructor
   * @param {Object} hash
   * @param {Number} [cacheGen=0]
   * @param {Number} [cacheLimit=0]
   */

  constructor(hash, cacheGen = 0, cacheLimit = 0) {
    assert(hash && typeof hash.digest === 'function');
    assert((cacheGen >>> 0) === cacheGen);
    assert((cacheLimit >>> 0) === cacheLimit);

    this.hash = hash;
    this.cacheGen = cacheGen;
    this.cacheLimit = cacheLimit;
  }

  hashRoot(n, batch, force) {
    const [h, dirty] = n.cache();

    if (h) {
      if (!batch)
        return [h, n];

      if (n.canUnload(this.cacheGen, this.cacheLimit))
        return [h, h];

      if (!dirty)
        return [h, n];
    }

    const [collapsed, cached] = this.hashChildren(n, batch);
    const hashed = this.store(collapsed, batch, force);

    if (hashed.isHash()) { // !force
      switch (cached.type) {
        case SHORTNODE:
        case FULLNODE: {
          const c = cached.clone();
          c.flags.hash = hashed;
          if (batch)
            c.flags.dirty = false;
          c.id = hashed.data;
          return [hashed, c];
        }
        case VALUENODE: {
          c.id = hashed.data;
          break;
        }
      }
    }

    return [hashed, cached];
  }

  hashChildren(n, batch) {
    switch (n.type) {
      case SHORTNODE: {
        const collapsed = n.clone();
        const cached = n.clone();

        if (!n.value.isValue()) {
          const [h, c] = this.hashRoot(n.value, batch, false);
          collapsed.value = h;
          cached.value = c;
        }

        return [collapsed, cached];
      }
      case FULLNODE: {
        const collapsed = n.clone();
        const cached = n.clone();

        for (let i = 0; i < 16; i++) {
          if (!n.children[i].isNull()) {
            const [h, c] = this.hashRoot(n.children[i], batch, false);
            collapsed.children[i] = h;
            cached.children[i] = c;
          }
        }

        return [collapsed, cached];
      }
      default: {
        return [n, n];
      }
    }
  }

  store(n, batch, force) {
    if (n.isNull() || n.isHash())
      return n;

    const raw = n.encode(this.hash);

    if (raw.length < this.hash.size && !force)
      return n;

    let [hash] = n.cache();
    if (!hash)
      hash = new HashNode(this.hash.digest(raw), this.hash);

    if (batch)
      batch.put(hash.data, raw);

    return hash;
  }
}

/*
 * Expose
 */

module.exports = Hasher;
