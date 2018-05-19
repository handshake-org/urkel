/* eslint no-unused-vars: "off" */

'use strict';

const assert = require('assert');
const crypto = require('crypto');
const DB = require('../test/util/db');
// const {sha256} = require('../test/util/util');
const sha256 = require('bcrypto/lib/sha256');
const Trie = require('../lib/trie');
const SecureTrie = require('../lib/securetrie');
const {wait, logMemory, createDB} = require('./util');

const BLOCKS = +process.argv[3] || 10000;
const PER_BLOCK = +process.argv[4] || 500;
const INTERVAL = +process.argv[5] || 88;
const RATE = Math.floor(BLOCKS / 20);
const TOTAL = BLOCKS * PER_BLOCK;

async function stress(Trie, db, pruneMode) {
  const trie = new Trie(sha256, db, 4, pruneMode);
  const pairs = [];
  const keys = [];

  await db.open();

  console.log(
    'Committing %d values to trie at a rate of %d per block.',
    TOTAL,
    PER_BLOCK);

  for (let i = 0; i < BLOCKS; i++) {
    let last = null;

    for (let j = 0; j < PER_BLOCK; j++) {
      const key = crypto.randomBytes(trie.hash.size);
      const value = crypto.randomBytes(300);

      pairs.push([key, value]);

      last = key;
    }

    if ((i % INTERVAL) === 0) {
      const now = Date.now();

      for (const [key, value] of pairs)
        await trie.insert(key, value);

      const b = db.batch();
      trie.commit(b);
      await b.write();

      pairs.length = 0;

      console.log('Insertion: %d', Date.now() - now);

      if (typeof gc === 'function')
        gc();

      logMemory();
    }

    if ((i % RATE) === 0) {
      keys.push(last);
      console.log(i * PER_BLOCK);
    }

    if ((i % 100) === 0) {
      console.log('waiting');
      console.log('keys %d', i * PER_BLOCK);
      await wait();
    }
  }

  console.log('Total Items: %d.', TOTAL);
  console.log('Blocks: %d.', BLOCKS);
  console.log('Items Per Block: %d.', PER_BLOCK);

  if (db.items != null) {
    console.log('DB Records: %d.', db.items);
    console.log('DB Size: %dmb.', db.size >>> 20);
  }

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    const now = Date.now();
    const nodes = await trie.prove(key);

    console.log('Proof %d time: %d.', i, Date.now() - now);

    let size = 0;
    for (const node of nodes)
      size += node.length;

    console.log('Proof %d length: %d', i, nodes.length);
    console.log('Proof %d size: %d', i, size);
  }

  await db.close();
}

async function bench(Trie, secure, db) {
  const trie = new Trie(sha256, db);
  const items = [];

  await db.open();

  for (let i = 0; i < 100000; i++) {
    const r = Math.random() > 0.5;
    const key = crypto.randomBytes(r ? trie.hash.size : 7);
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

    const b = db.batch();
    trie.commit(b);
    await b.write();

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
    db.write();

    console.log('Commit: %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    const b = db.batch();
    trie.commit(b);
    await b.write();

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
    const root = trie.rootHash();

    const [key] = items[items.length - 100];

    const now1 = Date.now();
    const proof = await trie.prove(key);
    console.log('Proof: %d.', Date.now() - now1);

    const now2 = Date.now();
    trie.verify(root, key, proof);
    console.log('Verify: %d.', Date.now() - now2);
  }

  await db.close();
}

(async () => {
  if (process.argv[2] === 'bdb') {
    console.log('Stress testing with BDB...');
    await stress(Trie, createDB(), 1);
    setInterval(() => {}, 1000);
    return;
  }

  if (process.argv[2] === 'stress') {
    console.log('Stress testing...');
    await stress(Trie, new DB(true), 0);
    return;
  }

  console.log('Running Trie bench...');
  await bench(Trie, false, new DB());

  console.log('');
  console.log('Running SecureTrie bench...');
  await bench(SecureTrie, true, new DB());
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
