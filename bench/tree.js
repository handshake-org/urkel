/* eslint no-unused-vars: "off" */
/* eslint no-implicit-coercion: "off" */

'use strict';

const assert = require('bsert');
const Path = require('path');
const crypto = require('crypto');
const blake2b = require('bcrypto/lib/blake2b');
const util = require('./util');

const argv = process.argv.slice();

let Tree = null;

switch (argv[2]) {
  case 'optimized':
    Tree = require('../optimized').Tree;
    argv.splice(2, 1);
    break;
  case 'trie':
    Tree = require('../trie').Tree;
    argv.splice(2, 1);
    break;
  case 'radix':
    Tree = require('../radix').Tree;
    argv.splice(2, 1);
    break;
  default:
    Tree = require('../optimized').Tree;
    break;
}

const BLOCKS = +argv[3] || 10;
const PER_BLOCK = +argv[4] || 100000;
const INTERVAL = +argv[5] || 72;
const TOTAL = BLOCKS * PER_BLOCK;
const FILE = Path.resolve(__dirname, 'treedb');

function verify(root, key, proof) {
  return proof.verify(root, key, blake2b, 256);
}

async function stress(prefix) {
  const tree = new Tree(blake2b, 256, prefix);
  const store = tree.store;

  await tree.open();

  {
    const now = util.now();

    const fakeHash = Buffer.alloc(32, 0x00);
    fakeHash[0] = 0x01;

    let missing = false;

    try {
      await tree.getHistory(fakeHash);
    } catch (e) {
      if (e.code !== 'ERR_MISSING_NODE')
        throw e;
      missing = true;
    }

    assert(missing);

    console.log('Size: %s', tree.store.rootCache.size);
    console.log('Roots: %d', util.now() - now);
  }

  const batch = tree.batch();

  console.log(
    'Committing %d values to tree at a rate of %d per block.',
    TOTAL,
    PER_BLOCK);

  for (let i = 0; i < BLOCKS; i++) {
    const pairs = [];

    for (let j = 0; j < PER_BLOCK; j++) {
      const k = crypto.randomBytes(tree.bits >>> 3);
      const v = crypto.randomBytes(3000);

      pairs.push([k, v]);
    }

    {
      const now = util.now();

      for (const [k, v] of pairs)
        await batch.insert(k, v);

      batch.rootHash();

      console.log('Insertion: %d', util.now() - now);
    }

    const [key, value] = pairs.pop();

    pairs.length = 0;

    if (i === 0)
      continue;

    if ((i % INTERVAL) === 0) {
      util.memory();

      const now = util.now();

      await batch.commit();

      console.log('Commit: %d', util.now() - now);
      console.log('WB Size: %dmb', store.buffer.data.length / 1024 / 1024);

      util.logMemory();

      await doProof(tree, i, key, value);
    }

    if ((i % 100) === 0)
      console.log('Keys: %d', i * PER_BLOCK);
  }

  console.log('Total Items: %d.', TOTAL);
  console.log('Blocks: %d.', BLOCKS);
  console.log('Items Per Block: %d.', PER_BLOCK);

  return tree.close();
}

async function doProof(tree, i, key, expect) {
  const now = util.now();
  const proof = await tree.prove(key);

  console.log('Proof %d time: %d.', i, util.now() - now);

  let size = 0;
  if (proof.nodes.length > 0) {
    if (Buffer.isBuffer(proof.nodes[0])) {
      for (const node of proof.nodes)
        size += 2 + node.length;
    } else {
      for (const {prefix, node} of proof.nodes)
        size += 2 + prefix.data.length + node.length;
    }
  }

  size += proof.value.length;

  const [code, value] = verify(tree.rootHash(), key, proof);

  assert.strictEqual(code, 0);
  assert.notStrictEqual(value, null);
  assert(Buffer.isBuffer(value));
  assert.strictEqual(value.length, 300);
  assert.bufferEqual(value, expect);

  console.log('Proof %d depth: %d', i, proof.depth);
  console.log('Proof %d length: %d', i, proof.nodes.length);
  console.log('Proof %d size: %d', i, size);
  console.log('Proof %d compressed size: %d',
    i, proof.getSize(tree.hash, tree.bits));
}




async function bench(prefix) {
  const tree = new Tree(blake2b, 256, prefix);
  const items = [];

  await tree.open();

  let batch = tree.batch();

  //Random Reads and Writes
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
      assert(await tree.get(key));
    console.log('Get (uncached): %d.', util.now() - now);
  }

  batch = tree.batch();

  {
    const now = util.now();

    for (let i = 0; i < items.length; i++) {
      const [key] = items[i];

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

    let proof = null;

    {
      const now = util.now();

      proof = await tree.prove(key);

      console.log('Proof: %d.', util.now() - now);
    }

    {
      const now = util.now();
      const [code, value] = verify(root, key, proof);
      assert(code === 0);
      assert(value);
      console.log('Verify: %d.', util.now() - now);
    }
  }

  return tree.close();
}

(async () => {
  const arg = argv.length >= 3
    ? argv[2]
    : '';

  switch (arg) {
    case 'stress':
      console.log('Stress testing...');
      return stress(FILE);
    case 'bench':
      console.log('Benchmarking (disk)...');
      return bench(FILE);
    default:
      console.log('Benchmarking (memory)...');
      return bench();
  }
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
