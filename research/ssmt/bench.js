/*!
 * bench.js - simple sparse merkle tree bench
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 */

'use strict';

const assert = require('assert');

/*
 * Test
 */

async function benchSSMT() {
  const crypto = require('crypto');
  const SSMT = require('./ssmt');
  const smt = new SSMT();
  const kv = [];

  for (let i = 0; i < 5000; i++)
    kv.push([crypto.randomBytes(32), crypto.randomBytes(32)]);

  let now = Date.now();

  for (const [key, value] of kv)
    smt.insert(key, value);

  console.log('Insertion: %d', Date.now() - now);

  now = Date.now();
  for (const [key, value] of kv) {
    const val = smt.get(key);
    assert.strictEqual(val.toString('hex'), value.toString('hex'));
  }
  console.log('Retrieval: %d', Date.now() - now);

  {
    const [key, value] = kv[Math.random() * kv.length | 0];
    const proof = await smt.prove(smt.root, key);
    let s = 0;
    for (const p of proof)
      s += p.length;
    console.log('Proof Size: %d', s);
    assert(smt.verify(proof, smt.root, key, value));
  }
}

/*
 * Execute
 */

(async () => {
  await benchSSMT();
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
