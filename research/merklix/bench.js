/*!
 * bench.js - merklix tree bench
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 */

'use strict';

const assert = require('assert');

/*
 * Test
 */

async function benchMerklix(rounds, roundSize) {
  const crypto = require('crypto');
  const Merklix = require('./merklix');
  const tree = new Merklix();

  let now;

  for (let i = 0; i < rounds; i++) {
    const kv = [];

    for (let i = 0; i < roundSize; i++)
      kv.push([crypto.randomBytes(32), crypto.randomBytes(32)]);

    console.log('Merklix Round %d (%d+%d items)', i, i * roundSize, roundSize);

    now = Date.now();
    for (const [key, value] of kv)
      tree.insert(key, value);
    console.log('  Insertion: %dms', Date.now() - now);

    {
      const key = crypto.randomBytes(32);
      now = Date.now();
      const proof = await tree.prove(tree.root, key);
      console.log('  Non-membership Proof: %dms', Date.now() - now);
      assert(!proof.exists);
      assert(tree.verify(proof, tree.root, key, null));
    }

    {
      const item = kv[Math.random() * kv.length | 0];
      const key = item[0];
      const value = item[1];
      now = Date.now();
      const proof = await tree.prove(tree.root, key);
      console.log('  Membership Proof: %dms', Date.now() - now);
      assert(proof.exists);
      assert(tree.verify(proof, tree.root, key, value));
      let size = 0;
      for (const n of proof.nodes)
        size += n.length;
      console.log('  Proof Size: %d', size);
    }

    console.log('  Nodes: %d', tree.db.map.size);
    console.log('  Max Depth: %d', tree.depth);

    const size = (3 * 32) * tree.db.map.size + (((i + 1) * roundSize) * 32)
    console.log('  DB Size: %dmb', (size / 1024 / 1024).toFixed(2));
  }
}

async function benchTrie(rounds, roundSize) {
  const {sha256} = require('../../test/util/util');
  const crypto = require('crypto');
  const DB = require('../../test/util/db');
  const Trie = require('../../lib/trie');
  const db = new DB(true);
  const trie = new Trie(trie, db);

  let now;

  for (let i = 0; i < rounds; i++) {
    const pairs = [];

    for (let i = 0; i < roundSize; i++) {
      pairs.push([
        crypto.randomBytes(trie.hash.size),
        crypto.randomBytes(trie.hash.size)
      ]);
    }

    console.log('Trie Round %d (%d+%d items)', i, i * roundSize, roundSize);

    now = Date.now();

    for (const [key, value] of pairs)
      await trie.insert(key, value);

    trie.commit(db);
    db.write();

    console.log('  Insertion: %dms', Date.now() - now);

    now = Date.now();
    const root = trie.rootHash('hex');
    console.log('  Root Hash: %dms', Date.now() - now);

    assert.strictEqual(root, trie.originalRoot.toString('hex'));

    {
      const key = sha256.digest(Buffer.from('non-member'));
      now = Date.now();
      const proof = await trie.prove(key);
      console.log('  Non-membership Proof: %dms', Date.now() - now);
      const [code, data] = trie.verify(trie.rootHash(), key, proof);
      assert(code === 0);
      assert(data === null);
    }

    {
      const key = pairs[pairs.length - 1][0];
      now = Date.now();
      const proof = await trie.prove(key);
      console.log('  Membership Proof: %dms', Date.now() - now);
      const [code, data] = trie.verify(trie.rootHash(), key, proof);
      assert(code === 0);
      assert(data !== null);
      let size = 0;
      for (const n of proof)
        size += n.length;
      console.log('  Proof Size: %d', size);
    }

    console.log('  Nodes: %d.', db.items);
    console.log('  DB Size: %dmb.', (db.size / 1024 / 1024).toFixed(2));
  }
}

/*
 * Execute
 */

(async () => {
  const rounds = 50;
  const roundSize = 5000;
  await benchMerklix(rounds, roundSize);
  await benchTrie(rounds, roundSize);
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
