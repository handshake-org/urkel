/* eslint-env mocha */
/* eslint prefer-arrow-callback: "off" */
/* eslint no-unused-vars: "off" */

'use strict';

const assert = require('./util/assert');
const crypto = require('crypto');
const DB = require('./util/db');
const {sha256} = require('./util/util');
const Merklix = require('../lib/merklix');
const {Proof} = Merklix;

const FOO1 = sha256.digest(Buffer.from('foo1'));
const FOO2 = sha256.digest(Buffer.from('foo2'));
const FOO3 = sha256.digest(Buffer.from('foo3'));
const FOO4 = sha256.digest(Buffer.from('foo4'));
const FOO5 = sha256.digest(Buffer.from('foo5'));

const BAR1 = Buffer.from('bar1');
const BAR2 = Buffer.from('bar2');
const BAR3 = Buffer.from('bar3');
const BAR4 = Buffer.from('bar4');

function random(min, max) {
  return Math.floor(Math.random() * (max - min)) + min;
}

async function runTest(db) {
  const tree = new Merklix(sha256, db);

  await db.open();

  let b = null;

  // Insert some values.
  await tree.insert(FOO1, BAR1);
  await tree.insert(FOO2, BAR2);
  await tree.insert(FOO3, BAR3);

  // Commit and get first non-empty root.
  b = db.batch();
  const first = tree.commit(b);
  await b.write();
  assert.strictEqual(first.length, tree.hash.size);

  // Get a committed value.
  assert.bufferEqual(await tree.get(FOO2), BAR2);

  // Insert a new value.
  await tree.insert(FOO4, BAR4);

  // Get second root with new committed value.
  // Ensure it is different from the first!
  {
    b = db.batch();
    const root = tree.commit(b);
    await b.write();
    assert.strictEqual(root.length, tree.hash.size);
    assert.notBufferEqual(root, first);
  }

  // Make sure our committed value is there.
  assert.bufferEqual(await tree.get(FOO4), BAR4);

  // Make sure we can snapshot the old root.
  const ss = tree.snapshot(first);
  assert.strictEqual(await ss.get(FOO4), null);
  assert.bufferEqual(ss.rootHash(), first);

  // Remove the last value.
  await tree.remove(FOO4);

  // Commit removal and ensure our root hash
  // has reverted to what it was before (first).
  b = db.batch();
  // assert.bufferEqual(tree.commit(b), first);
  tree.commit(b);
  await b.write();

  // Make sure removed value is gone.
  assert.strictEqual(await tree.get(FOO4), null);

  // Make sure older values are still there.
  assert.bufferEqual(await tree.get(FOO2), BAR2);

  // Create a proof and verify.
  {
    const proof = await tree.prove(first, FOO2);
    assert.deepStrictEqual(Proof.decode(proof.encode(), tree.hash.size), proof);
    const [code, data] = tree.verify(first, FOO2, proof);
    assert.strictEqual(code, 0);
    assert.bufferEqual(data, BAR2);
  }

  // Create a non-existent proof and verify.
  {
    const proof = await tree.prove(first, FOO5);
    assert.deepStrictEqual(Proof.decode(proof.encode(), tree.hash.size), proof);
    const [code, data] = tree.verify(first, FOO5, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Create a non-existent proof and verify.
  {
    const proof = await tree.prove(first, FOO4);
    assert.deepStrictEqual(Proof.decode(proof.encode(), tree.hash.size), proof);
    const [code, data] = tree.verify(first, FOO4, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Create a proof and verify.
  {
    const proof = await tree.prove(FOO2);
    assert.deepStrictEqual(Proof.decode(proof.encode(), tree.hash.size), proof);
    const [code, data] = tree.verify(tree.root, FOO2, proof);
    assert.strictEqual(code, 0);
    assert.bufferEqual(data, BAR2);
  }

  // Create a non-existent proof and verify.
  {
    const proof = await tree.prove(FOO5);
    assert.deepStrictEqual(Proof.decode(proof.encode(), tree.hash.size), proof);
    const [code, data] = tree.verify(tree.root, FOO5, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Create a proof and verify.
  {
    const proof = await tree.prove(FOO4);
    assert.deepStrictEqual(Proof.decode(proof.encode(), tree.hash.size), proof);
    const [code, data] = tree.verify(tree.root, FOO4, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Test persistence.
  {
    b = db.batch();
    const root = tree.commit(b);
    await b.write();

    await tree.close();
    await tree.open(root);

    // Make sure older values are still there.
    assert.bufferEqual(await tree.get(FOO2), BAR2);
  }

  // Test persistence of best state.
  {
    b = db.batch();
    const root = tree.commit(b);
    await b.write();

    await tree.close();
    await tree.open();

    assert.bufferEqual(tree.rootHash(), root);

    // Make sure older values are still there.
    assert.bufferEqual(await tree.get(FOO2), BAR2);
  }

  await db.close();
}

async function pummel(db) {
  const tree = new Merklix(sha256, db);
  const items = [];
  const set = new Set();

  await db.open();

  let b = null;

  while (set.size < 10000) {
    const key = crypto.randomBytes(tree.hash.size);
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

  {
    for (const [key, value] of items)
      await tree.insert(key, value);

    b = db.batch();
    const root = tree.commit(b);
    await b.write();

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

  for (const [i, [key]] of items.entries()) {
    if (i & 1)
      await tree.remove(key);
  }

  {
    b = db.batch();
    const root = tree.commit(b);
    await b.write();

    await tree.close();
    await tree.open();

    assert.bufferEqual(tree.rootHash(), root);
  }

  for (const [i, [key, value]] of items.entries()) {
    const val = await tree.get(key);

    if (i & 1)
      assert.strictEqual(val, null);
    else
      assert.bufferEqual(val, value);
  }

  {
    b = db.batch();
    const root = tree.commit(b);
    await b.write();

    await tree.close();
    await tree.open();

    assert.bufferEqual(tree.rootHash(), root);
  }

  for (let i = 0; i < items.length; i += 11) {
    const [key, value] = items[i];

    const root = tree.rootHash();
    const proof = await tree.prove(key);
    const [code, data] = tree.verify(root, key, proof);

    assert.strictEqual(code, 0);

    if (i & 1)
      assert.strictEqual(data, null);
    else
      assert.bufferEqual(data, value);
  }

  await db.close();
}

describe('Merklix', function() {
  this.timeout(5000);

  it('should test tree', async () => {
    await runTest(new DB());
  });

  it('should pummel tree', async () => {
    await pummel(new DB());
  });
});
