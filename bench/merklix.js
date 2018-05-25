/* eslint no-unused-vars: "off" */
/* eslint no-implicit-coercion: "off" */

'use strict';

const assert = require('assert');
const crypto = require('crypto');
const DB = require('../test/util/db');
// const {sha256} = require('../test/util/util');
const sha256 = require('bcrypto/lib/sha256');
const Merklix = require('../research/merklix');
const {wait, memory, logMemory, createDB} = require('./util');

const BLOCKS = +process.argv[3] || 10000;
const PER_BLOCK = +process.argv[4] || 500;
const INTERVAL = +process.argv[5] || 88;
const RATE = Math.floor(BLOCKS / 20);
const TOTAL = BLOCKS * PER_BLOCK;
const FILE = __dirname + '/merklixdb';

async function stress(prefix, db) {
  const tree = new Merklix(sha256, 160, prefix, db, 4);
  const pairs = [];
  const keys = [];

  await db.open();
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

    tree.rootHash();

    console.log('Insertion: %d', Date.now() - now);

    pairs.length = 0;

    if (i && (i % INTERVAL) === 0) {
      memory();

      const now = Date.now();

      const b = db.batch();
      await tree.commit(b);
      await b.write();

      console.log('Commit: %d', Date.now() - now);

      logMemory();

      await doProof(tree, i, last);
    }

    if ((i % RATE) === 0)
      keys.push(last);

    if ((i % 100) === 0)
      console.log('Keys: %d', i * PER_BLOCK);
  }

  console.log('Total Items: %d.', TOTAL);
  console.log('Blocks: %d.', BLOCKS);
  console.log('Items Per Block: %d.', PER_BLOCK);

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    await doProof(tree, i, key);
  }

  await tree.close();
  await db.close();
}

async function doProof(tree, i, key) {
  const now = Date.now();
  const proof = await tree.prove(key);

  console.log('Proof %d time: %d.', i, Date.now() - now);

  let size = 0;
  for (const node of proof.nodes)
    size += node.length;

  if (proof.key)
    size += proof.key.length;

  let vsize = 0;

  if (proof.value)
    vsize = 1 + proof.value.length;

  const [code, value] = tree.verify(key, proof);
  assert(code === 0);

  console.log('Proof %d length: %d', i, proof.nodes.length);
  console.log('Proof %d size: %d', i, size);
  console.log('Proof %d compressed size: %d',
    i, proof.getSize(tree.hash, tree.bits) - vsize);
}

async function bench(prefix, db) {
  const tree = new Merklix(sha256, 160, prefix, db);
  const items = [];

  await db.open();
  await tree.open();

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

  await tree.close();
  await db.close();
}

(async () => {
  if (process.argv[2] === 'bdb') {
    console.log('Stress testing with BDB...');
    await stress(FILE, createDB());
    setInterval(() => {}, true);
    return;
  }

  if (process.argv[2] === 'stress') {
    console.log('Stress testing...');
    await stress(null, new DB(true));
    return;
  }

  console.log('Running Merklix bench...');
  await bench(null, new DB());
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
