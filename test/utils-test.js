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





  async function errors() {
    assert.throws(() => { var tree = new Tree(sha256, 160, 5555)}, Error, "Error thrown")

    const tree = new Tree(sha256, 160)
    await tree.open();

    const batch = tree.batch();


    //assert.throws(async () => { await tree.get("hello")}, Error, "Error thrown")


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







  describe(name, function() {
    this.timeout(5000);

    it('should test errors', async () => {
      await errors();
    });
  });
}



{
  const {Tree, Proof} = require('../optimized');
  runTest('Optimized', Tree, Proof);
}

{
  const {Tree, Proof} = require('../trie');
  runTest('Trie', Tree, Proof);
}

{
  const {Tree, Proof} = require('../radix');
  runTest('Radix', Tree, Proof);
}
