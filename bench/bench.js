/* eslint no-unused-vars: "off" */

'use strict';

const assert = require('assert');
const crypto = require('crypto');
const DB = require('../test/util/db');
const Trie = require('../lib/trie');
const SecureTrie = require('../lib/securetrie');

const BLOCKS = 10000;
const PER_BLOCK = 100;
const RATE = Math.floor(BLOCKS / 20);
const TOTAL = BLOCKS * PER_BLOCK;
const INTERVAL = 144;

async function stress(Trie) {
  const db = new DB(true);
  const trie = new Trie(db);
  const pairs = [];
  const keys = [];

  console.log(
    'Committing %d values to trie at a rate of %d per block.',
    TOTAL,
    PER_BLOCK);

  for (let i = 0; i < BLOCKS; i++) {
    let last = null;

    for (let j = 0; j < PER_BLOCK; j++) {
      const key = crypto.randomBytes(32);
      const value = crypto.randomBytes(300);

      pairs.push([key, value]);

      last = key;
    }

    if ((i % INTERVAL) === 0) {
      for (const [key, value] of pairs)
        await trie.insert(key, value);

      trie.commit(db);
      db.flush();

      pairs.length = 0;
    }

    if ((i % RATE) === 0) {
      keys.push(last);
      console.log(i * PER_BLOCK);
    }
  }

  console.log('Total Items: %d.', TOTAL);
  console.log('Blocks: %d.', BLOCKS);
  console.log('Items Per Block: %d.', PER_BLOCK);
  console.log('DB Records: %d.', db.items);
  console.log('DB Size: %dmb.', db.size >>> 20);

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    const now = Date.now();
    const nodes = await trie.prove(key);

    console.log('Proof %d time: %d.', i, Date.now() - now);

    let size = 32 + 2 + 0 + 2;
    for (const node of nodes)
      size += node.length;

    console.log('Proof %d length: %d', i, nodes.length);
    console.log('Proof %d size: %d', i, size);
  }
}

async function bench(Trie, secure) {
  const db = new DB();
  const trie = new Trie(db);
  const items = [];

  for (let i = 0; i < 100000; i++) {
    const r = Math.random() > 0.5;
    const key = crypto.randomBytes(r ? 32 : 7);
    const value = crypto.randomBytes(r ? 100 : 1);

    items.push([key, value]);
  }

  {
    const now = Date.now();

    for (const [key, value] of items)
      await trie.insert(key, value);

    console.log('Insert: %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    for (const [key] of items)
      await trie.get(key);

    console.log('Get (cached): %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    trie.commit(db);
    db.flush();

    console.log('Commit: %d.', Date.now() - now);
  }

  await trie.close();
  await trie.open();

  {
    const now = Date.now();

    for (const [key] of items)
      await trie.get(key);

    console.log('Get (uncached): %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    for (const [i, [key]] of items.entries()) {
      if (i & 1)
        await trie.remove(key);
    }

    console.log('Remove: %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    trie.commit(db);
    db.flush();

    console.log('Commit: %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    trie.commit(db);
    db.flush();

    console.log('Commit (nothing): %d.', Date.now() - now);
  }

  await trie.close();
  await trie.open();

  {
    const now = Date.now();
    const iter = trie.iterator(true);

    for (;;) {
      if (!await iter.next())
        break;
    }

    console.log('Iteration: %d.', Date.now() - now);
  }

  {
    const root = trie.hash();

    const [key] = items[items.length - 100];

    const now1 = Date.now();
    const proof = await trie.prove(key);
    console.log('Proof: %d.', Date.now() - now1);

    const now2 = Date.now();
    trie.verify(root, key, proof);
    console.log('Verify: %d.', Date.now() - now2);
  }
}

(async () => {
  console.log('Running Trie bench...');

  await stress(Trie);
  await bench(Trie, false);

  console.log('');
  console.log('Running SecureTrie bench...');
  await bench(SecureTrie, true);
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
