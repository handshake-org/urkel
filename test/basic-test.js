/* eslint-env mocha */
/* eslint prefer-arrow-callback: "off" */
/* eslint no-unused-vars: "off" */

'use strict';

const assert = require('bsert');
const crypto = require('crypto');
const {sha1, sha256} = require('./util/util');

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

function runTest(name, Tree, Proof) {
  function reencode(tree, proof) {
    const raw = proof.encode(tree.hash, tree.bits);
    return Proof.decode(raw, tree.hash, tree.bits);
  }

  function rejson(tree, proof) {
    const json = proof.toJSON(tree.hash, tree.bits);
    return Proof.fromJSON(json, tree.hash, tree.bits);
  }

  function verify(root, key, proof) {
    return proof.verify(root, key, sha256, 160);
  }

  async function test() {
    const tree = new Tree(sha256, 160);

    await tree.open();

    const batch = tree.batch();

    // Insert some values.
    await batch.insert(FOO1, BAR1);
    await batch.insert(FOO2, BAR2);
    await batch.insert(FOO3, BAR3);

    // Commit and get first non-empty root.
    const first = await batch.commit();
    assert.strictEqual(first.length, tree.hash.size);

    // Get a committed value.
    assert.bufferEqual(await tree.get(FOO2), BAR2);

    // Insert a new value.
    await batch.insert(FOO4, BAR4);

    // Get second root with new committed value.
    // Ensure it is different from the first!
    {
      const root = await batch.commit();
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
    await batch.remove(FOO4);

    // Commit removal and ensure our root hash
    // has reverted to what it was before (first).
    assert.bufferEqual(await batch.commit(), first);

    // Make sure removed value is gone.
    assert.strictEqual(await tree.get(FOO4), null);

    // Make sure older values are still there.
    assert.bufferEqual(await tree.get(FOO2), BAR2);

    // Create a proof and verify.
    {
      const ss = tree.snapshot(first);
      const proof = await ss.prove(FOO2);
      assert.deepStrictEqual(reencode(tree, proof), proof);
      assert.deepStrictEqual(rejson(tree, proof), proof);
      const [code, data] = verify(first, FOO2, proof);
      assert.strictEqual(code, 0);
      assert.bufferEqual(data, BAR2);
    }

    // Create a non-existent proof and verify.
    {
      const ss = tree.snapshot(first);
      const proof = await ss.prove(FOO5);
      assert.deepStrictEqual(reencode(tree, proof), proof);
      assert.deepStrictEqual(rejson(tree, proof), proof);
      const [code, data] = verify(first, FOO5, proof);
      assert.strictEqual(code, 0);
      assert.strictEqual(data, null);
    }

    // Create a non-existent proof and verify.
    {
      const ss = tree.snapshot(first);
      const proof = await ss.prove(FOO4);
      assert.deepStrictEqual(reencode(tree, proof), proof);
      assert.deepStrictEqual(rejson(tree, proof), proof);
      const [code, data] = verify(first, FOO4, proof);
      assert.strictEqual(code, 0);
      assert.strictEqual(data, null);
    }

    // Create a proof and verify.
    {
      const ss = tree.snapshot();
      const proof = await ss.prove(FOO2);
      assert.deepStrictEqual(reencode(tree, proof), proof);
      assert.deepStrictEqual(rejson(tree, proof), proof);
      const [code, data] = verify(tree.rootHash(), FOO2, proof);
      assert.strictEqual(code, 0);
      assert.bufferEqual(data, BAR2);
    }

    // Create a non-existent proof and verify.
    {
      const ss = tree.snapshot();
      const proof = await tree.prove(FOO5);
      assert.deepStrictEqual(reencode(tree, proof), proof);
      assert.deepStrictEqual(rejson(tree, proof), proof);
      const [code, data] = verify(tree.rootHash(), FOO5, proof);
      assert.strictEqual(code, 0);
      assert.strictEqual(data, null);
    }

    // Create a proof and verify.
    {
      const ss = tree.snapshot();
      const proof = await tree.prove(FOO4);
      assert.deepStrictEqual(reencode(tree, proof), proof);
      assert.deepStrictEqual(rejson(tree, proof), proof);
      const [code, data] = verify(tree.rootHash(), FOO4, proof);
      assert.strictEqual(code, 0);
      assert.strictEqual(data, null);
    }

    // Iterate over values.
    {
      const ss = tree.snapshot();
      const items = [];

      for await (const [key, value] of ss)
        items.push([key, value]);

      assert.strictEqual(items.length, 3);
      assert.deepStrictEqual(items, [
        [FOO1, BAR1],
        [FOO2, BAR2],
        [FOO3, BAR3]
      ]);
    }

    // Test persistence.
    {
      const root = await batch.commit();

      await tree.close();
      await tree.open();

      const ss = tree.snapshot(root);

      // Make sure older values are still there.
      assert.bufferEqual(await ss.get(FOO2), BAR2);
    }

    // Test persistence of best state.
    {
      const root = await batch.commit();

      await tree.close();
      await tree.open();

      assert.bufferEqual(tree.rootHash(), root);

      // Make sure older values are still there.
      assert.bufferEqual(await tree.get(FOO2), BAR2);
    }

    await tree.close();
  }









  async function pummel() {
    const tree = new Tree(sha256, 160);
    const items = [];
    const set = new Set();

    await tree.open();

    let batch = tree.batch();

    while (set.size < 10000) {
      const key = crypto.randomBytes(tree.bits >>> 3);
      const value = crypto.randomBytes(random(1, 100));
      const key1 = key.toString('binary');

      if (set.has(key1))
        continue;

      key[key.length - 1] ^= 1;

      const key2 = key.toString('binary');

      key[key.length - 1] ^= 1;

      if (set.has(key2))
        continue;

      set.add(key1);

      items.push([key, value]);
    }

    set.clear();

    let midRoot = null;
    let lastRoot = null;

    {
      for (const [i, [key, value]] of items.entries()) {
        await batch.insert(key, value);
        if (i === (items.length >>> 1) - 1)
          midRoot = batch.rootHash();
      }

      const root = await batch.commit();
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
        await batch.remove(key);
    }

    {
      const root = await batch.commit();

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
      const root = await batch.commit();

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

      for await (const [key, value] of tree) {
        const [k, v] = expect[i];

        assert.bufferEqual(key, k);
        assert.bufferEqual(value, v);

        i += 1;
      }

      assert.strictEqual(i, items.length >>> 1);
    }

    for (let i = 0; i < items.length; i += 11) {
      const [key, value] = items[i];

      const root = tree.rootHash();
      const proof = await tree.prove(key);
      const [code, data] = verify(root, key, proof);

      assert.strictEqual(code, 0);

      if (i < (items.length >>> 1))
        assert.strictEqual(data, null);
      else
        assert.bufferEqual(data, value);
    }

    {
      const stat1 = await tree.store.stat();
      await tree.compact();
      const stat2 = await tree.store.stat();
      assert(stat1.size > stat2.size);
    }

    const rand = items.slice(0, items.length >>> 1);

    rand.sort((a, b) => Math.random() >= 0.5 ? 1 : -1);

    batch = tree.batch();

    for (const [i, [key, value]] of rand.entries())
      await batch.insert(key, value);

    {
      assert.bufferEqual(batch.rootHash(), lastRoot);

      const root = await batch.commit();

      await tree.close();
      await tree.open();

      assert.bufferEqual(tree.rootHash(), root);
      assert.bufferEqual(tree.rootHash(), lastRoot);
    }

    await tree.close();
  }

  async function history() {
    const items = [];
    const removed = [];
    const remaining = [];

    while (items.length < 10000) {
      const key = crypto.randomBytes(20);
      const value = crypto.randomBytes(random(1, 100));
      items.push([key, value]);
    }

    const tree1 = new Tree(sha256, 160);
    await tree1.open();

    const tree2 = new Tree(sha256, 160);
    await tree2.open();

    let root = null;
    let fullRoot1 = null;
    let fullRoot2 = null;
    let midRoot1 = null;
    let midRoot2 = null;

    {
      const batch = tree1.batch();

      for (const [key, value] of items)
        await batch.insert(key, value);

      root = await batch.commit();
    }

    {
      const batch = tree1.batch();

      for (const [key, value] of items) {
        if (Math.random() < 0.5) {
          remaining.push([key, value]);
          continue;
        }

        await batch.remove(key);

        removed.push([key, value]);
      }

      midRoot1 = await batch.commit();
    }

    {
      const batch = tree1.batch();

      for (const [key, value] of removed)
        await batch.insert(key, value);

      fullRoot1 = await batch.commit();
    }

    {
      const batch = tree2.batch();

      for (const [key, value] of remaining)
        await batch.insert(key, value);

      midRoot2 = await batch.commit();
    }

    {
      const batch = tree2.batch();

      for (const [key, value] of removed)
        await batch.insert(key, value);

      fullRoot2 = await batch.commit();
    }

    assert.bufferEqual(fullRoot1, root);
    assert.bufferEqual(fullRoot2, root);
    assert.bufferEqual(fullRoot1, fullRoot2);
    assert.bufferEqual(midRoot1, midRoot2);

    await tree1.close();
    await tree2.close();
  }

  describe(name, function() {
    this.timeout(5000);

    it('should test tree', async () => {
      await test();
    });


    it('should pummel tree', async () => {
      await pummel();
    });

    it('should test history independence', async () => {
      await history();
    });
  });
}



{
  const {Tree, Proof} = require('../optimized');
  runTest('Optimized', Tree, Proof);
}


{
  const {Tree, Proof} = require('../tree');
  runTest('Tree', Tree, Proof);
}


{
  const {Tree, Proof} = require('../trie');
  runTest('Trie', Tree, Proof);
}

{
  const {Tree, Proof} = require('../radix');
  runTest('Radix', Tree, Proof);
}
