/* eslint-env mocha */
/* eslint prefer-arrow-callback: "off" */
/* eslint no-unused-vars: "off" */

'use strict';

const assert = require('./util/assert');
const crypto = require('crypto');
const DB = require('./util/db');
const {sha256} = require('./util/util');
const Trie = require('../lib/trie');
const SecureTrie = require('../lib/securetrie');

const FOO1 = Buffer.from('foo1');
const FOO2 = Buffer.from('foo2');
const FOO3 = Buffer.from('foo3');
const FOO4 = Buffer.from('foo4');
const FOO5 = Buffer.from('foo5');

const BAR1 = Buffer.from('bar1');
const BAR2 = Buffer.from('bar2');
const BAR3 = Buffer.from('bar3');
const BAR4 = Buffer.from('bar4');

function random(min, max) {
  return Math.floor(Math.random() * (max - min)) + min;
}

function compare(k1, k2) {
  const len = Math.min(k1.length, k2.length);

  for (let i = 0; i < len; i++) {
    if (k1[i] < k2[i])
      return -1;

    if (k1[i] > k2[i])
      return 1;
  }

  // The trie's "length comparison" is reversed.
  // In other words, longer keys come first.
  if (k1.length > k2.length)
    return -1;

  if (k1.length < k2.length)
    return 1;

  return 0;
}

async function runTest(Trie, secure, db) {
  const trie = new Trie(sha256, db);

  await db.open();

  let b = null;

  // Insert some values.
  await trie.insert(FOO1, BAR1);
  await trie.insert(FOO2, BAR2);
  await trie.insert(FOO3, BAR3);

  // Commit and get first non-empty root.
  b = db.batch();
  const first = trie.commit(b);
  await b.write();
  assert.strictEqual(first.length, trie.hash.size);

  // Get a committed value.
  assert.bufferEqual(await trie.get(FOO2), BAR2);

  // Insert a new value.
  await trie.insert(FOO4, BAR4);

  // Get second root with new committed value.
  // Ensure it is different from the first!
  {
    b = db.batch();
    const root = trie.commit(b);
    await b.write();
    assert.strictEqual(root.length, trie.hash.size);
    assert.notBufferEqual(root, first);
  }

  // Make sure our committed value is there.
  assert.bufferEqual(await trie.get(FOO4), BAR4);

  // Make sure we can snapshot the old root.
  const ss = trie.snapshot(first);
  assert.strictEqual(await ss.get(FOO4), null);
  assert.bufferEqual(ss.rootHash(), first);

  // Remove the last value.
  await trie.remove(FOO4);

  // Commit removal and ensure our root hash
  // has reverted to what it was before (first).
  b = db.batch();
  assert.bufferEqual(trie.commit(b), first);
  await b.write();

  // Make sure removed value is gone.
  assert.strictEqual(await trie.get(FOO4), null);

  // Make sure older values are still there.
  assert.bufferEqual(await trie.get(FOO2), BAR2);

  // Create a proof and verify.
  {
    const proof = await trie.prove(FOO2);
    const [code, data] = trie.verify(first, FOO2, proof);
    assert.strictEqual(code, 0);
    assert.bufferEqual(data, BAR2);
  }

  // Create a non-existent proof and verify.
  {
    const proof = await trie.prove(FOO5);
    const [code, data] = trie.verify(first, FOO5, proof);
    assert.strictEqual(code, 0);
    assert.strictEqual(data, null);
  }

  // Iterate over values.
  {
    const iter = trie.iterator(true);
    const items = [];

    while (await iter.next()) {
      const {key, value} = iter;
      items.push([key, value]);
    }

    if (!secure) {
      assert.deepStrictEqual(items, [
        [FOO1, BAR1],
        [FOO2, BAR2],
        [FOO3, BAR3]
      ]);
    } else {
      // Order is different for secure trie due to
      // the fact that the keys are actually hashes.
      assert.deepStrictEqual(items, [
        [FOO2, BAR2],
        [FOO3, BAR3],
        [FOO1, BAR1]
      ]);
    }
  }

  // Test persistence.
  {
    b = db.batch();
    const root = trie.commit(b);
    await b.write();

    await trie.close();
    await trie.open(root);

    // Make sure older values are still there.
    assert.bufferEqual(await trie.get(FOO2), BAR2);
  }

  // Test persistence of best state.
  {
    b = db.batch();
    const root = trie.commit(b);
    await b.write();

    await trie.close();
    await trie.open();

    assert.bufferEqual(trie.rootHash(), root);

    // Make sure older values are still there.
    assert.bufferEqual(await trie.get(FOO2), BAR2);
  }

  // Iterate over values (secure only).
  if (secure) {
    const iter = trie.iterator(false);
    const items = [];

    while (await iter.next()) {
      const {key, value} = iter;
      items.push([key, value]);
    }

    assert.deepStrictEqual(items, [
      [trie.hashKey(FOO2), BAR2],
      [trie.hashKey(FOO3), BAR3],
      [trie.hashKey(FOO1), BAR1]
    ]);
  }

  await db.close();
}

async function pummel(Trie, secure, db) {
  const trie = new Trie(sha256, db);
  const items = [];
  const set = new Set();

  await db.open();

  let b = null;

  while (set.size < 10000) {
    const key = crypto.randomBytes(random(1, 100));
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
      await trie.insert(key, value);

    b = db.batch();
    const root = trie.commit(b);
    await b.write();

    for (const [key, value] of items) {
      assert.bufferEqual(await trie.get(key), value);

      key[key.length - 1] ^= 1;
      assert.strictEqual(await trie.get(key), null);
      key[key.length - 1] ^= 1;
    }

    await trie.close();
    await trie.open();

    assert.bufferEqual(trie.rootHash(), root);
  }

  for (const [key, value] of items) {
    assert.bufferEqual(await trie.get(key), value);

    key[key.length - 1] ^= 1;
    assert.strictEqual(await trie.get(key), null);
    key[key.length - 1] ^= 1;
  }

  for (const [i, [key]] of items.entries()) {
    if (i & 1)
      await trie.remove(key);
  }

  {
    b = db.batch();
    const root = trie.commit(b);
    await b.write();

    await trie.close();
    await trie.open();

    assert.bufferEqual(trie.rootHash(), root);
  }

  for (const [i, [key, value]] of items.entries()) {
    const val = await trie.get(key);

    if (i & 1)
      assert.strictEqual(val, null);
    else
      assert.bufferEqual(val, value);
  }

  {
    b = db.batch();
    const root = trie.commit(b);
    await b.write();

    await trie.close();
    await trie.open();

    assert.bufferEqual(trie.rootHash(), root);
  }

  {
    const iter = trie.iterator(true);
    const expect = [];

    for (const [i, item] of items.entries()) {
      if (i & 1)
        continue;

      expect.push(item);
    }

    expect.sort((a, b) => {
      let [k1] = a;
      let [k2] = b;

      if (secure) {
        k1 = trie.hashKey(k1);
        k2 = trie.hashKey(k2);
      }

      return compare(k1, k2);
    });

    let i = 0;
    while (await iter.next()) {
      const {key, value} = iter;

      assert(i < expect.length);

      const [k, v] = expect[i];

      assert.bufferEqual(key, k);
      assert.bufferEqual(value, v);

      i += 1;
    }

    assert.strictEqual(i, items.length >>> 1);
  }

  if (secure) {
    const iter = trie.iterator(false);
    const expect = [];

    for (const [i, [key, value]] of items.entries()) {
      if (i & 1)
        continue;

      expect.push([trie.hashKey(key), value]);
    }

    expect.sort((a, b) => {
      const [k1] = a;
      const [k2] = b;
      return compare(k1, k2);
    });

    let i = 0;
    while (await iter.next()) {
      const {key, value} = iter;

      assert(i < expect.length);

      const [k, v] = expect[i];

      assert.bufferEqual(key, k);
      assert.bufferEqual(value, v);

      i += 1;
    }

    assert.strictEqual(i, items.length >>> 1);
  }

  for (let i = 0; i < items.length; i += 11) {
    const [key, value] = items[i];

    const root = trie.rootHash();
    const proof = await trie.prove(key);
    const [code, data] = trie.verify(root, key, proof);

    assert.strictEqual(code, 0);

    if (i & 1)
      assert.strictEqual(data, null);
    else
      assert.bufferEqual(data, value);
  }

  await db.close();
}

describe('Trie', function() {
  it('should test trie', async () => {
    await runTest(Trie, false, new DB());
  });

  it('should pummel trie', async () => {
    await pummel(Trie, false, new DB());
  });

  it('should test secure trie', async () => {
    await runTest(SecureTrie, true, new DB());
  });

  it('should pummel secure trie', async () => {
    await pummel(SecureTrie, true, new DB());
  });
});
