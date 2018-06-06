/* eslint no-unused-vars: "off" */
/* eslint no-implicit-coercion: "off" */

'use strict';

const assert = require('assert');
const crypto = require('crypto');
const sha256 = require('bcrypto/lib/sha256');
const DB = require('../test/util/db');
const {Tree} = require('../');
const util = require('./util');

const {
  memory,
  logMemory,
  createDB
} = util;

const BLOCKS = +process.argv[3] || 10000;
const PER_BLOCK = +process.argv[4] || 500;
const INTERVAL = +process.argv[5] || 88;
const RATE = Math.floor(BLOCKS / 20);
const TOTAL = BLOCKS * PER_BLOCK;
const FILE = `${__dirname}/treedb`;

async function commit(tree, db) {
  if (!db)
    return tree.commit();

  const b = db.batch();
  const r = await tree.commit(b);
  await b.write();
  return r;
}

async function stress(prefix, db) {
  const tree = new Tree(sha256, 160, prefix, db, 4);
  const keys = [];

  if (db)
    await db.open();

  await tree.open();

  console.log(
    'Committing %d values to tree at a rate of %d per block.',
    TOTAL,
    PER_BLOCK);

  for (let i = 0; i < BLOCKS; i++) {
    const pairs = [];

    let last = null;

    for (let j = 0; j < PER_BLOCK; j++) {
      const key = crypto.randomBytes(tree.bits >>> 3);
      const value = crypto.randomBytes(300);

      pairs.push([key, value]);

      last = key;
    }

    const now = util.now();

    for (const [key, value] of pairs)
      await tree.insert(key, value);

    tree.rootHash();

    console.log('Insertion: %d', util.now() - now);

    pairs.length = 0;

    if (i && (i % INTERVAL) === 0) {
      memory();

      const now = util.now();

      await commit(tree, db);

      console.log('Commit: %d', util.now() - now);

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

  if (db)
    await db.close();
}

async function doProof(tree, i, key) {
  const now = util.now();
  const proof = await tree.prove(key);

  console.log('Proof %d time: %d.', i, util.now() - now);

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
  const tree = new Tree(sha256, 160, prefix, db);
  const items = [];

  if (db)
    await db.open();

  await tree.open();

  for (let i = 0; i < 100000; i++) {
    const r = Math.random() > 0.5;
    const key = crypto.randomBytes(tree.bits >>> 3);
    const value = crypto.randomBytes(r ? 100 : 1);

    items.push([key, value]);
  }

  {
    const now = util.now();

    for (const [key, value] of items)
      await tree.insert(key, value);

    console.log('Insert: %d.', util.now() - now);
  }

  {
    const now = util.now();

    for (const [key] of items)
      await tree.get(key);

    console.log('Get (cached): %d.', util.now() - now);
  }

  {
    const now = util.now();

    await commit(tree, db);

    console.log('Commit: %d.', util.now() - now);
  }

  await tree.close();
  await tree.open();

  {
    const now = util.now();

    for (const [key] of items)
      await tree.get(key);

    console.log('Get (uncached): %d.', util.now() - now);
  }

  {
    const now = util.now();

    for (const [i, [key]] of items.entries()) {
      if (i & 1)
        await tree.remove(key);
    }

    console.log('Remove: %d.', util.now() - now);
  }

  {
    const now = util.now();

    await commit(tree, db);

    console.log('Commit: %d.', util.now() - now);
  }

  {
    const now = util.now();

    await commit(tree, db);

    console.log('Commit (nothing): %d.', util.now() - now);
  }

  await tree.close();
  await tree.open();

  {
    const root = tree.rootHash();

    const [key] = items[items.length - 100];

    const now1 = util.now();
    const proof = await tree.prove(key);
    console.log('Proof: %d.', util.now() - now1);

    const now2 = util.now();
    tree.verify(root, key, proof);
    console.log('Verify: %d.', util.now() - now2);
  }

  await tree.close();

  if (db)
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
    await stress(FILE, null);
    return;
  }

  console.log('Running Tree bench...');
  await bench(null, new DB());
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
