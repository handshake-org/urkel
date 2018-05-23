/* eslint-env mocha */
/* eslint prefer-arrow-callback: "off" */
/* eslint no-unused-vars: "off" */

'use strict';

const assert = require('./util/assert');
const crypto = require('crypto');
const DB = require('./util/db');
const {sha256} = require('./util/util');
const SMT = require('../research/smt');
const {Proof} = SMT;

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

function reencode(smt, proof) {
  const raw = proof.encode(smt.hash, smt.bits);
  return Proof.decode(raw, smt.hash, smt.bits);
}

async function runTest(db) {
  const smt = new SMT(sha256, 256, db);
  const q = smt.queue();

  await db.open();

  let b = null;

  // Insert some values.
  q.insert(FOO1, BAR1);
  q.insert(FOO2, BAR2);
  q.insert(FOO3, BAR3);

  // Commit and get first non-empty root.
  b = db.batch();
  const first = await q.commit(b);
  await b.write();
  assert.strictEqual(first.length, smt.hash.size);

  // Get a committed value.
  assert.bufferEqual(await smt.get(FOO2), BAR2);

  // Insert a new value.
  q.insert(FOO4, BAR4);

  // Get second root with new committed value.
  // Ensure it is different from the first!
  {
    b = db.batch();
    const root = await q.commit(b);
    await b.write();
    assert.strictEqual(root.length, smt.hash.size);
    assert.notBufferEqual(root, first);
  }

  // Make sure our committed value is there.
  assert.bufferEqual(await smt.get(FOO4), BAR4);

  // Remove the last value.
  q.remove(FOO4);

  // Commit removal and ensure our root hash
  // has reverted to what it was before (first).
  b = db.batch();
  assert.bufferEqual(await q.commit(b), first);
  await b.write();

  // Make sure removed value is gone.
  assert.strictEqual(await smt.get(FOO4), null);

  // Make sure older values are still there.
  assert.bufferEqual(await smt.get(FOO2), BAR2);

  // Create a proof and verify.
  {
    const proof = await smt.prove(FOO2);
    assert.deepStrictEqual(reencode(smt, proof), proof);
    const [code, data] = smt.verify(FOO2, proof);
    assert.strictEqual(code, 0);
    assert.bufferEqual(data, BAR2);
  }

  // Create a non-existent proof and verify.
  {
    const proof = await smt.prove(FOO4);
    assert.deepStrictEqual(reencode(smt, proof), proof);
    const [code, data] = smt.verify(FOO4, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Create a non-existent proof and verify.
  {
    const proof = await smt.prove(FOO5);
    assert.deepStrictEqual(reencode(smt, proof), proof);
    const [code, data] = smt.verify(FOO5, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Test persistence of best state.
  {
    b = db.batch();
    const root = await q.commit(b);
    await b.write();

    await smt.close();
    await smt.open();

    assert.bufferEqual(smt.rootHash(), root);

    // Make sure older values are still there.
    assert.bufferEqual(await smt.get(FOO2), BAR2);
  }

  await db.close();
}

async function pummel(db) {
  const smt = new SMT(sha256, 256, db);
  const q = smt.queue();
  const items = [];
  const set = new Set();

  await db.open();

  let b = null;

  while (set.size < 500) {
    const key = crypto.randomBytes(smt.bits >>> 3);
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
      q.insert(key, value);

    b = db.batch();
    const root = await q.commit(b);
    await b.write();

    for (const [key, value] of items) {
      assert.bufferEqual(await smt.get(key), value);

      key[key.length - 1] ^= 1;
      assert.strictEqual(await smt.get(key), null);
      key[key.length - 1] ^= 1;
    }

    await smt.close();
    await smt.open();

    assert.bufferEqual(smt.rootHash(), root);
  }

  for (const [key, value] of items) {
    assert.bufferEqual(await smt.get(key), value);

    key[key.length - 1] ^= 1;
    assert.strictEqual(await smt.get(key), null);
    key[key.length - 1] ^= 1;
  }

  for (const [i, [key]] of items.entries()) {
    if (i & 1)
      q.remove(key);
  }

  {
    b = db.batch();
    const root = await q.commit(b);
    await b.write();

    await smt.close();
    await smt.open();

    assert.bufferEqual(smt.rootHash(), root);
  }

  for (const [i, [key, value]] of items.entries()) {
    const val = await smt.get(key);

    if (i & 1)
      assert.strictEqual(val, null);
    else
      assert.bufferEqual(val, value);
  }

  {
    b = db.batch();
    const root = await q.commit(b);
    await b.write();

    await smt.close();
    await smt.open();

    assert.bufferEqual(smt.rootHash(), root);
  }

  for (let i = 0; i < items.length; i += 11) {
    const [key, value] = items[i];

    const root = smt.rootHash();
    const proof = await smt.prove(key);
    const [code, data] = smt.verify(key, proof);

    assert.strictEqual(code, 0);

    if (i & 1)
      assert.strictEqual(data, null);
    else
      assert.bufferEqual(data, value);
  }

  await db.close();
}

describe('SMT', function() {
  this.timeout(5000);

  it('should test tree', async () => {
    await runTest(new DB());
  });

  it('should pummel tree', async () => {
    await pummel(new DB());
  });
});

