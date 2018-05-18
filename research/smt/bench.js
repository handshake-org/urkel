/*!
 * bench.js - sparse merkle tree bench
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 */

'use strict';

const assert = require('assert');

/*
 * Test
 */

async function benchSMT(rounds, roundSize) {
  const random = require('bcrypto/lib/random');
  const SMT = require('./smt');
  const smt = new SMT();

  const db = [];

  let now;

  for (let i = 0; i < rounds; i++) {
    const keys = [];

    for (let i = 0; i < roundSize; i++)
      keys.push(random.randomBytes(smt.bytes));

    for (const key of keys)
      db.push(key);

    sortSet(db);
    sortSet(keys);

    console.log('SMT Round %d (%d+%d items)', i, i * roundSize, roundSize);

    now = Date.now();
    const root = await smt.update(db, keys, smt.bits, smt.zero, smt.set);
    console.log('  Insertion: %dms', Date.now() - now);

    now = Date.now();
    const r = await smt.rootHash(db, smt.bits, smt.zero);
    console.log('  Root Hash: %dms', Date.now() - now);

    assert.strictEqual(root.toString('hex'), r.toString('hex'));

    {
      const key = smt.hash.digest(Buffer.from('non-member'));
      now = Date.now();
      const ap = await smt.auditPath(db, smt.bits, smt.zero, key);
      console.log('  Non-membership Proof: %dms', Date.now() - now);
      assert(smt.verifyAuditPath(ap, key, smt.empty, root));
      assert(!smt.verifyAuditPath(ap, key, smt.set, root));
    }

    {
      const key = keys[keys.length - 1];
      now = Date.now();
      const ap = await smt.auditPath(db, smt.bits, smt.zero, key);
      console.log('  Membership Proof: %dms', Date.now() - now);
      assert(smt.verifyAuditPath(ap, key, smt.set, root));
      assert(!smt.verifyAuditPath(ap, key, smt.empty, root));
    }
  }
}

async function benchTrie(rounds, roundSize) {
  const sha256 = require('bcrypto/lib/sha256');
  const random = require('bcrypto/lib/random');
  const DB = require('../../test/util/db');
  const Trie = require('../../lib/trie');
  const db = new DB();
  const trie = new Trie(db);

  let now;

  for (let i = 0; i < rounds; i++) {
    const pairs = [];

    for (let i = 0; i < roundSize; i++)
      pairs.push([random.randomBytes(32), random.randomBytes(32)]);

    console.log('Trie Round %d (%d+%d items)', i, i * roundSize, roundSize);

    now = Date.now();

    for (const [key, value] of pairs)
      await trie.insert(key, value);

    trie.commit(db);
    db.flush();

    console.log('  Insertion: %dms', Date.now() - now);

    now = Date.now();
    const root = trie.hash('hex');
    console.log('  Root Hash: %dms', Date.now() - now);

    assert.strictEqual(root, trie.originalRoot.toString('hex'));

    {
      const key = sha256.digest(Buffer.from('non-member'));
      now = Date.now();
      const proof = await trie.prove(key);
      console.log('  Non-membership Proof: %dms', Date.now() - now);
      const [code, data] = trie.verify(trie.hash(), key, proof);
      assert(code === 0);
      assert(data === null);
    }

    {
      const key = pairs[pairs.length - 1][0];
      now = Date.now();
      const proof = await trie.prove(key);
      console.log('  Membership Proof: %dms', Date.now() - now);
      const [code, data] = trie.verify(trie.hash(), key, proof);
      assert(code === 0);
      assert(data !== null);
    }
  }
}

/*
 * Helpers
 */

function compare(a, b) {
  return a.compare(b);
}

function sortSet(items) {
  assert(Array.isArray(items));
  return items.sort(compare);
}

/*
 * Execute
 */

(async () => {
  const rounds = 5;
  const roundSize = 5000;
  await benchSMT(rounds, roundSize);
  await benchTrie(rounds, roundSize);
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
