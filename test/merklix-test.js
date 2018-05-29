/* eslint-env mocha */
/* eslint prefer-arrow-callback: "off" */
/* eslint no-unused-vars: "off" */

'use strict';

const assert = require('./util/assert');
const crypto = require('crypto');
const DB = require('./util/db');
const {sha1, sha256} = require('./util/util');
const {Merklix, Proof} = require('../research/merklix');

const FOO1 = sha1.digest(Buffer.from('foo1'));
const FOO2 = sha1.digest(Buffer.from('foo2'));
const FOO3 = sha1.digest(Buffer.from('foo3'));
const FOO4 = sha1.digest(Buffer.from('foo4'));
const FOO5 = sha1.digest(Buffer.from('foo5'));

const BAR1 = Buffer.from('bar1');
const BAR2 = Buffer.from('bar2');
const BAR3 = Buffer.from('bar3');
const BAR4 = Buffer.from('bar4');

function random(min, max) {
  return Math.floor(Math.random() * (max - min)) + min;
}

function reencode(tree, proof) {
  const raw = proof.encode(tree.hash, tree.bits);
  return Proof.decode(raw, tree.hash, tree.bits);
}

async function commit(tree, db) {
  if (!db)
    return tree.commit();

  const b = db.batch();
  const r = await tree.commit(b);
  await b.write();
  return r;
}

async function compact(tree, db) {
  if (!db)
    return tree.compact();

  const b = db.batch();
  const r = await tree.compact(b);
  await b.write();
  return r;
}

async function runTest(db) {
  const tree = new Merklix(sha256, 160, null, db, 0);

  if (db)
    await db.open();

  await tree.open();

  // Insert some values.
  await tree.insert(FOO1, BAR1);
  await tree.insert(FOO2, BAR2);
  await tree.insert(FOO3, BAR3);

  // Commit and get first non-empty root.
  const first = await commit(tree, db);
  assert.strictEqual(first.length, tree.hash.size);

  // Get a committed value.
  assert.bufferEqual(await tree.get(FOO2), BAR2);

  // Insert a new value.
  await tree.insert(FOO4, BAR4);

  // Get second root with new committed value.
  // Ensure it is different from the first!
  {
    const root = await commit(tree, db);
    assert.strictEqual(root.length, tree.hash.size);
    assert.notBufferEqual(root, first);
  }

  // Make sure our committed value is there.
  assert.bufferEqual(await tree.get(FOO4), BAR4);

  // Make sure we can snapshot the old root.
  const ss = await tree.snapshot(first);
  assert.strictEqual(await ss.get(FOO4), null);
  assert.bufferEqual(ss.rootHash(), first);

  // Remove the last value.
  await tree.remove(FOO4);

  // Commit removal and ensure our root hash
  // has reverted to what it was before (first).
  assert.bufferEqual(await commit(tree, db), first);

  // Make sure removed value is gone.
  assert.strictEqual(await tree.get(FOO4), null);

  // Make sure older values are still there.
  assert.bufferEqual(await tree.get(FOO2), BAR2);

  // Create a proof and verify.
  {
    const proof = await tree.prove(first, FOO2);
    assert.deepStrictEqual(reencode(tree, proof), proof);
    const [code, data] = tree.verify(first, FOO2, proof);
    assert.strictEqual(code, 0);
    assert.bufferEqual(data, BAR2);
  }

  // Create a non-existent proof and verify.
  {
    const proof = await tree.prove(first, FOO5);
    assert.deepStrictEqual(reencode(tree, proof), proof);
    const [code, data] = tree.verify(first, FOO5, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Create a non-existent proof and verify.
  {
    const proof = await tree.prove(first, FOO4);
    assert.deepStrictEqual(reencode(tree, proof), proof);
    const [code, data] = tree.verify(first, FOO4, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Create a proof and verify.
  {
    const proof = await tree.prove(FOO2);
    assert.deepStrictEqual(reencode(tree, proof), proof);
    const [code, data] = tree.verify(tree.rootHash(), FOO2, proof);
    assert.strictEqual(code, 0);
    assert.bufferEqual(data, BAR2);
  }

  // Create a non-existent proof and verify.
  {
    const proof = await tree.prove(FOO5);
    assert.deepStrictEqual(reencode(tree, proof), proof);
    const [code, data] = tree.verify(tree.rootHash(), FOO5, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Create a proof and verify.
  {
    const proof = await tree.prove(FOO4);
    assert.deepStrictEqual(reencode(tree, proof), proof);
    const [code, data] = tree.verify(tree.rootHash(), FOO4, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Iterate over values.
  {
    const items = [];

    await tree.values((key, value) => {
      items.push([key, value]);
    });

    assert.deepStrictEqual(items, [
      [FOO1, BAR1],
      [FOO2, BAR2],
      [FOO3, BAR3]
    ]);
  }

  // Test persistence.
  {
    const root = await commit(tree, db);

    await tree.close();
    await tree.open(root);

    // Make sure older values are still there.
    assert.bufferEqual(await tree.get(FOO2), BAR2);
  }

  // Test persistence of best state.
  {
    const root = await commit(tree, db);

    await tree.close();
    await tree.open();

    assert.bufferEqual(tree.rootHash(), root);

    // Make sure older values are still there.
    assert.bufferEqual(await tree.get(FOO2), BAR2);
  }

  await tree.close();

  if (db)
    await db.close();
}

async function pummel(db) {
  const tree = new Merklix(sha256, 160, null, db, 0);
  const items = [];
  const set = new Set();

  if (db)
    await db.open();

  await tree.open();

  while (set.size < 10000) {
    const key = crypto.randomBytes(tree.bits >>> 3);
    const value = crypto.randomBytes(random(1, 100));
    const hex = key.toString('hex');

    if (set.has(hex))
      continue;

    key[key.length - 1] ^= 1;

    const h = key.toString('hex');

    key[key.length - 1] ^= 1;

    if (set.has(h))
      continue;

    set.add(hex);

    items.push([key, value]);
  }

  set.clear();

  let midRoot = null;
  let lastRoot = null;

  {
    for (const [i, [key, value]] of items.entries()) {
      await tree.insert(key, value);
      if (i === (items.length >>> 1) - 1)
        midRoot = tree.rootHash();
    }

    const root = await commit(tree, db);
    lastRoot = root;

    for (const [key, value] of items) {
      assert.bufferEqual(await tree.get(key), value);

      key[key.length - 1] ^= 1;
      assert.strictEqual(await tree.get(key), null);
      key[key.length - 1] ^= 1;
    }

    await tree.close();
    await tree.open();

    assert.bufferEqual(tree.rootHash(), root);
  }

  for (const [key, value] of items) {
    assert.bufferEqual(await tree.get(key), value);

    key[key.length - 1] ^= 1;
    assert.strictEqual(await tree.get(key), null);
    key[key.length - 1] ^= 1;
  }

  items.reverse();

  for (const [i, [key]] of items.entries()) {
    if (i < (items.length >>> 1))
      await tree.remove(key);
  }

  {
    const root = await commit(tree, db);

    await tree.close();
    await tree.open();

    assert.bufferEqual(tree.rootHash(), root);
    assert.bufferEqual(tree.rootHash(), midRoot);
  }

  for (const [i, [key, value]] of items.entries()) {
    const val = await tree.get(key);

    if (i < (items.length >>> 1))
      assert.strictEqual(val, null);
    else
      assert.bufferEqual(val, value);
  }

  {
    const root = await commit(tree, db);

    await tree.close();
    await tree.open();

    assert.bufferEqual(tree.rootHash(), root);
  }

  {
    const expect = [];

    for (const [i, item] of items.entries()) {
      if (i < (items.length >>> 1))
        continue;

      expect.push(item);
    }

    expect.sort((a, b) => {
      const [x] = a;
      const [y] = b;
      return x.compare(y);
    });

    let i = 0;

    await tree.values((key, value) => {
      const [k, v] = expect[i];

      assert.bufferEqual(key, k);
      assert.bufferEqual(value, v);

      i += 1;
    });

    assert.strictEqual(i, items.length >>> 1);
  }

  for (let i = 0; i < items.length; i += 11) {
    const [key, value] = items[i];

    const root = tree.rootHash();
    const proof = await tree.prove(key);
    const [code, data] = tree.verify(root, key, proof);

    assert.strictEqual(code, 0);

    if (i < (items.length >>> 1))
      assert.strictEqual(data, null);
    else
      assert.bufferEqual(data, value);
  }

  {
    const stat1 = await tree.store.stat();
    await compact(tree, db);
    const stat2 = await tree.store.stat();
    assert(stat1.size > stat2.size);
  }

  const rand = items.slice(0, items.length >>> 1);

  rand.sort((a, b) => Math.random() >= 0.5 ? 1 : -1);

  for (const [i, [key, value]] of rand.entries())
    await tree.insert(key, value);

  {
    assert.bufferEqual(tree.rootHash(), lastRoot);

    const root = await commit(tree, db);

    await tree.close();
    await tree.open();

    assert.bufferEqual(tree.rootHash(), root);
    assert.bufferEqual(tree.rootHash(), lastRoot);
  }

  await tree.close();

  if (db)
    await db.close();
}

describe('Merklix', function() {
  this.timeout(5000);

  it('should test tree', async () => {
    await runTest(new DB());
  });

  it('should test tree standalone', async () => {
    await runTest(null);
  });

  it('should pummel tree', async () => {
    await pummel(new DB());
  });

  it('should pummel tree standalone', async () => {
    await pummel(null);
  });
});
