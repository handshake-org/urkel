'use strict';

const assert = require('bsert');
const crypto = require('crypto');
const blake2b160 = require('bcrypto/lib/blake2b160');
const {Tree, Proof} = require('../lib/urkel');
const {types, typesByVal} = Proof;

describe('Proof Insertion', function() {
  this.timeout(30000);

 /**
  * Escher protocol: Data must fit inside 512 byte UPDATE covenant item
  *
  * 1     version (0x01)
  * 20    current tree root
  * 1     method (REGISTER: 0x00, UPDATE: 0x01)
  * 20    compound namehash (H(sld.tld) = key)
  * ...   params
  *
  * REGISTER: 0x00
  * 32    NEW ed25519 public key
  * 4-438 Urkel proof-of-nonexistence of namehash
  *
  * UPDATE:   0x01
  * 32    NEW ed25519 public key
  * 64    signature
  * 4-374 urkel proof of OLD public key at namehash
  */

  const hash = blake2b160;
  const bits = 160;
  const tree = new Tree({hash, bits});
  const entries = 2000;
  const proofSizeLimit = 374;

  const data = [];
  for (let i = 0; i < entries; i++) {
    data.push({
      key: crypto.randomBytes(20),
      value: crypto.randomBytes(32)
    });
  }

  before(async () => {
    await tree.open();
  });

  after(async () => {
    await tree.close();
  });

  const count = {
    'TYPE_DEADEND': 0,
    'TYPE_SHORT': 0,
    'TYPE_COLLISION': 0
  };

  let maxProofSize = 0;
  let maxDepth = 0;

  it(`should insert ${entries} entries`, async () => {
    for (const datum of data) {
      // Prove nonexistence
      const proof = await tree.prove(datum.key);

      const size = proof.getSize(hash, bits);
      if (size > maxProofSize)
        maxProofSize = size;
      if (proof.depth > maxDepth)
        maxDepth = proof.depth;

      count[typesByVal[proof.type]]++;

      // Insert into proof and compute new root hash
      proof.insert(datum.key, datum.value, hash);
      const expectedRoot = proof.computeRoot(datum.key, hash, bits);

      // Insert into actual tree and compute new root hash
      const b = tree.batch();
      await b.insert(datum.key, datum.value);
      await b.commit();
      const actualRoot = tree.rootHash();

      // Compare
      assert.bufferEqual(expectedRoot, actualRoot);
    }
  });

  it(`should update ${entries} entries`, async () => {
    for (const datum of data) {
      // Get current existence proof
      const proof = await tree.prove(datum.key);

      const size = proof.getSize(hash, bits);
      if (size > maxProofSize)
        maxProofSize = size;
      if (proof.depth > maxDepth)
        maxDepth = proof.depth;

      assert.strictEqual(proof.type, types.TYPE_EXISTS);

      // Modify value
      datum.value = crypto.randomBytes(33);

      // Insert into proof and compute new root hash
      proof.insert(datum.key, datum.value, hash);
      const expectedRoot = proof.computeRoot(datum.key, hash, bits);

      // Insert into actual tree and compute new root hash
      const b = tree.batch();
      await b.insert(datum.key, datum.value);
      await b.commit();
      const actualRoot = tree.rootHash();

      // Compare
      assert.bufferEqual(expectedRoot, actualRoot);
    }
  });

  it('should have at least one TYPE_DEADEND', () => {
    console.log(count['TYPE_DEADEND']);
    assert(count['TYPE_DEADEND']);
  });

  it('should have at least one TYPE_SHORT', () => {
    console.log(count['TYPE_SHORT']);
    assert(count['TYPE_SHORT']);
  });

  it('should have at least one TYPE_COLLISION', () => {
    console.log(count['TYPE_COLLISION']);
    assert(count['TYPE_COLLISION']);
  });

  it(`should have max proof size < ${proofSizeLimit} bytes`, () => {
    console.log(maxProofSize);
    assert(maxProofSize < proofSizeLimit);
  });

  it('should have max depth', () => {
    console.log(maxDepth);
  });
});
