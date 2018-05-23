/* eslint no-unused-vars: "off" */
/* eslint no-implicit-coercion: "off" */

'use strict';

const assert = require('assert');
const crypto = require('crypto');
const DB = require('../test/util/db');
// const {sha256} = require('../test/util/util');
const sha256 = require('bcrypto/lib/sha256');
const Merklix = require('../research/merklix');
const {wait, logMemory, createDB} = require('./util');

const BLOCKS = +process.argv[3] || 10000;
const PER_BLOCK = +process.argv[4] || 500;
const INTERVAL = +process.argv[5] || 88;
const RATE = Math.floor(BLOCKS / 20);
const TOTAL = BLOCKS * PER_BLOCK;
const FILE = __dirname + '/merklixdb';

async function stress(db, prune) {
  const tree = new Merklix(sha256, 160, FILE, 0);
  const pairs = [];
  const keys = [];

  await tree.open();

  console.log(
    'Committing %d values to tree at a rate of %d per block.',
    TOTAL,
    PER_BLOCK);

  for (let i = 0; i < BLOCKS; i++) {
    let last = null;

    for (let j = 0; j < PER_BLOCK; j++) {
      const key = crypto.randomBytes(tree.bits >>> 3);
      const value = crypto.randomBytes(300);

      pairs.push([key, value]);

      last = key;
    }

    const now = Date.now();

    for (const [key, value] of pairs)
      await tree.insert(key, value);

    console.log('Insertion: %d', Date.now() - now);

    pairs.length = 0;

    if ((i % INTERVAL) === 0) {
      const now = Date.now();

      await tree.commit();

      console.log('Commit: %d', Date.now() - now);

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
    const proof = await tree.prove(key);

    console.log('Proof %d time: %d.', i, Date.now() - now);

    let size = 0;
    for (const node of proof.nodes)
      size += node.length;

    if (proof.key)
      size += proof.key.length;

    if (proof.value)
      size += proof.value.length;

    console.log('Proof %d length: %d', i, proof.nodes.length);
    console.log('Proof %d size: %d', i, size);
    console.log('Proof %d compressed size: %d',
      i, proof.getSize(tree.hash.size, tree.bits));
  }

  await tree.close();
}

async function bench(db, prune) {
  const tree = new Merklix(sha256, 160, db, null, prune);
  const items = [];

  await db.open();

  for (let i = 0; i < 100000; i++) {
    const r = Math.random() > 0.5;
    const key = crypto.randomBytes(tree.bits >>> 3);
    const value = crypto.randomBytes(r ? 100 : 1);

    items.push([key, value]);
  }

  {
    const now = Date.now();

    for (const [key, value] of items)
      await tree.insert(key, value);

    console.log('Insert: %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    for (const [key] of items)
      await tree.get(key);

    console.log('Get (cached): %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    const b = db.batch();
    tree.commit(b);
    await b.write();

    console.log('Commit: %d.', Date.now() - now);
  }

  await tree.close();
  await tree.open();

  {
    const now = Date.now();

    for (const [key] of items)
      await tree.get(key);

    console.log('Get (uncached): %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    for (const [i, [key]] of items.entries()) {
      if (i & 1)
        await tree.remove(key);
    }

    console.log('Remove: %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    const b = db.batch();
    tree.commit(b);
    await b.write();

    console.log('Commit: %d.', Date.now() - now);
  }

  {
    const now = Date.now();

    const b = db.batch();
    tree.commit(b);
    await b.write();

    console.log('Commit (nothing): %d.', Date.now() - now);
  }

  await tree.close();
  await tree.open();

  {
    const root = tree.rootHash();

    const [key] = items[items.length - 100];

    const now1 = Date.now();
    const proof = await tree.prove(key);
    console.log('Proof: %d.', Date.now() - now1);

    const now2 = Date.now();
    tree.verify(root, key, proof);
    console.log('Verify: %d.', Date.now() - now2);
  }

  await db.close();
}

(async () => {
  if (process.argv[2] === 'bdb') {
    console.log('Stress testing with BDB...');
    await stress(createDB(), false);
    setInterval(() => {}, true);
    return;
  }

  if (process.argv[2] === 'stress') {
    console.log('Stress testing...');
    await stress(new DB(true), false);
    return;
  }

  console.log('Running Merklix bench...');
  await bench(new DB(), false);
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
