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
    assert(Buffer.isBuffer(root) && root.length === 32);
    const key = Buffer.allocUnsafe(33);
    key[0] = 0x70; // p
    root.copy(key, 1);
    return key;
  }
}

/*
 * Expose
 */

module.exports = PruneList;
