'use strict';

const assert = require('bsert');
const {tmpdir} = require('os');
const path = require('path');
const fs = require('bfile');
const {Tree} = require('../lib/urkel');
const {sha256} = require('./util/util');

function key(bits) {
  return Buffer.from([bits]);
}

const FOO1 = key(0b00000000);
const FOO2 = key(0b10000000);
const FOO3 = key(0b11000000);
const FOO4 = key(0b10100000);

const BAR1 = Buffer.from([1]);
const BAR2 = Buffer.from([2]);
const BAR3 = Buffer.from([3]);
const BAR4 = Buffer.from([4]);

describe('Recovery Test', function() {
  let tree;
  let txn;
  let root1, root2;

  const location = path.join(tmpdir(), `urkel-recovery-test-${Date.now()}`);

  after(() => {
    fs.rimraf(location);
  });

  for (const dir of [location, null]) {
    describe(`${dir ? 'Disk': 'Memory'}`, function() {
      it('should init tree', async () => {
        tree = new Tree({
          hash: sha256,
          bits: 8,
          prefix: dir
        });
        await tree.open();
        txn = tree.transaction();
      });

      it('should add 3 records', async () => {
        await txn.insert(FOO1, BAR1);
        await txn.insert(FOO2, BAR2);
        await txn.insert(FOO3, BAR3);
      });

      it('should commit with 3 records and save root', async () => {
        root1 = await txn.commit();
      });

      it('should add one record after commit', async () => {
        await txn.insert(FOO4, BAR4);
      });

      it('should commit with 4 records and save root', async () => {
        root2 = await txn.commit();
      });

      it('should clear tree store rootCache', async () => {
        // Just do manual cache reset
        // clears cache and reset indexer ptr.
        tree.store.resetCache();
      });

      it('should restore tree from first saved root', async () => {
        await tree.inject(root1);
        txn = tree.transaction();
      });

      it('should only get 3 records after restoring from first root', async () => {
        assert.bufferEqual(await txn.get(FOO1), BAR1);
        assert.bufferEqual(await txn.get(FOO2), BAR2);
        assert.bufferEqual(await txn.get(FOO3), BAR3);

        assert.strictEqual(await txn.get(FOO4), null);
      });

      it('should restore tree from second saved root', async () => {
        await tree.inject(root2);
        txn = tree.transaction();
      });

      it('should get all records after restoring from second root', async () => {
        assert.bufferEqual(await txn.get(FOO1), BAR1);
        assert.bufferEqual(await txn.get(FOO2), BAR2);
        assert.bufferEqual(await txn.get(FOO3), BAR3);
        assert.bufferEqual(await txn.get(FOO4), BAR4);
      });
    });
  }
});
