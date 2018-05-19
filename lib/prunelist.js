/*!
 * prunelist.js - memory optimal prune list
 * Copyright (c) 2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const blake2b = require('bcrypto/lib/blake2b');
const HashList = require('./hashlist');

/**
 * Constants
 */

// BLAKE2B("prune")
const PREFIX = Buffer.from(
  'd7bfee60020d6f003984053d2c358d718cef27dbcb1db7242e478aa1df194ad3',
  'hex');

/**
 * PruneList
 */

class PruneList extends HashList {
  constructor() {
    super();
  }

  key(root) {
    return this.constructor.key(root);
  }

  add(n) {
    assert(n);

    if (n.id)
      this.push(n.id);

    n.id = null;
  }

  save(batch, root) {
    assert(batch);

    batch.put(this.key(root), this.encode());

    console.log('Saved %d items for pruning: %s.',
      this.length, root.toString('hex'));

    this.clear();
  }

  prune(batch) {
    assert(batch);

    for (const hash of this.values())
      batch.del(hash);

    console.log('Pruned %d items.', this.length);

    this.clear();
  }

  static key(root) {
    return blake2b.root(PREFIX, root);
  }
}

/*
 * Expose
 */

module.exports = PruneList;
