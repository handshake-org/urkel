/* eslint no-unused-vars: "off" */
/* eslint no-implicit-coercion: "off" */

'use strict';

const assert = require('assert');
const crypto = require('crypto');
const blake2b = require('bcrypto/lib/blake2b');
const DB = require('../test/util/db');
const {Tree} = require('../');
const util = require('./util');

const BLOCKS = +process.argv[3] || 10000;
const PER_BLOCK = +process.argv[4] || 500;
const INTERVAL = +process.argv[5] || 88;
const RATE = Math.floor(BLOCKS / 20);
const TOTAL = BLOCKS * PER_BLOCK;
const FILE = `${__dirname}/treedb`;

function verify(root, key, proof) {
  return proof.verify(root, key, blake2b, 256);
}

async function stress(prefix) {
  const tree = new Tree(blake2b, 256, prefix);
  const keys = [];

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

    const batch = tree.batch();

    for (const [key, value] of pairs)
      await batch.insert(key, value);

    batch.rootHash();

    console.log('Insertion: %d', util.now() - now);

    pairs.length = 0;

    if (i && (i % INTERVAL) === 0) {
      util.memory();

      const now = util.now();

      await batch.commit();

      console.log('Commit: %d', util.now() - now);

      util.logMemory();

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

  const [code, value] = verify(tree.rootHash(), key, proof);
  assert(code === 0);

  console.log('Proof %d length: %d', i, proof.nodes.length);
  console.log('Proof %d size: %d', i, size);
  console.log('Proof %d compressed size: %d',
    i, proof.getSize(tree.hash, tree.bits) - vsize);
}

async function bench(prefix) {
  const tree = new Tree(blake2b, 256, prefix);
  const items = [];

  await tree.open();

  let batch = tree.batch();

  for (let i = 0; i < 100000; i++) {
    const r = Math.random() > 0.5;
    const key = crypto.randomBytes(tree.bits >>> 3);
    const value = crypto.randomBytes(r ? 100 : 1);

    items.push([key, value]);
  }

  {
    const now = util.now();

    for (const [key, value] of items)
      await batch.insert(key, value);

    console.log('Insert: %d.', util.now() - now);
  }

  {
    const now = util.now();

    for (const [key] of items)
      assert(await batch.get(key));

    console.log('Get (cached): %d.', util.now() - now);
  }

  {
    const now = util.now();

    await batch.commit();

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

  batch = tree.batch();

  {
    const now = util.now();

    for (const [i, [key]] of items.entries()) {
      if (i & 1)
        await batch.remove(key);
    }

    console.log('Remove: %d.', util.now() - now);
  }

  {
    const now = util.now();

    await batch.commit();

    console.log('Commit: %d.', util.now() - now);
  }

  {
    const now = util.now();

    await batch.commit();

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

    const [code, value] = verify(root, key, proof);
    assert(code === 0);
    assert(value);

    console.log('Verify: %d.', util.now() - now2);
  }

  await tree.close();
}

(async () => {
  if (process.argv[2] === 'stress') {
    console.log('Stress testing...');
    await stress(FILE);
    return;
  }

  console.log('Running Tree bench...');
  await bench();
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
