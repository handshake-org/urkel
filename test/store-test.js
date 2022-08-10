'use strict';

const assert = require('bsert');
const {tmpdir} = require('os');
const path = require('path');
const fs = require('bfile');
const {BLAKE2b} = require('bcrypto');
const {Tree} = require('../lib/urkel');
const os = require('os');

let keyCounter = 0;

function getNextKey(bits) {
  assert(bits >= 32);
  const buffer = Buffer.alloc(bits / 8);
  buffer.writeUint32BE(keyCounter);
  keyCounter++;
  return buffer;
}

async function addEntries(txn, n, bits) {
  for (let i = 0; i < n; i++) {
    const key = getNextKey(bits);
    const value = Buffer.alloc(bits / 8);
    key.copy(value);
    await txn.insert(key, value);
  }

  return txn;
}

async function populateTree(tree, roots, perCommit) {
  const hashes = new Set();

  for (let i = 0; i < roots; i++) {
    const txn = tree.transaction();

    await addEntries(txn, perCommit, tree.bits);

    const hash = await txn.commit();
    hashes.add(hash.toString('binary'));
  }

  return hashes;
}

function mapHasSet(map, set) {
  if (map.size !== set.size)
    return false;

  for (const key of set.values()) {
    if (!map.has(key))
      return false;
  }

  return true;
}

describe('Store', function() {
  const treeOptions = {
    hash: BLAKE2b,
    bits: 256
  };

  for (const memory of [true, false]) {
    describe(`Cache ${memory ? 'Memory' : 'Disk'}`, function() {
      let options = null;
      let prefix = null;

      beforeEach(async () => {
        if (!memory)
          prefix = path.join(tmpdir(), `urkel-cache-test-${Date.now()}`);

        options = {
          ...treeOptions,
          prefix
        };
      });

      afterEach(async () => {
        if (!memory) {
          await fs.rimraf(prefix);
          prefix = null;
        }

        options = null;
      });

      it('should cache root hashes when added', async () => {
        const tree = new Tree(options);

        await tree.open();

        const {rootCache} = tree.store;
        assert.strictEqual(rootCache.size, 0);

        let roots = await populateTree(tree, 1, 2);
        assert.strictEqual(rootCache.size, 1);
        assert(mapHasSet(rootCache, roots));

        roots = new Set([...roots, ...await populateTree(tree, 1, 2)]);
        assert.strictEqual(rootCache.size, 2);
        assert(mapHasSet(rootCache, roots));

        await tree.close();

        // make sure root is cleared on close.
        assert.strictEqual(rootCache.size, 0);
      });

      it('should not recover hashes when initCacheSize is 0', async () => {
        const opts = {
          ...options,
          initCacheSize: 0
        };

        const tree = new Tree(opts);
        const {rootCache} = tree.store;

        let roots;

        // prepare tree
        await tree.open();
        roots = await populateTree(tree, 10, 2);
        assert.strictEqual(rootCache.size, 10);
        assert.strictEqual(rootCache.size, roots.size);
        assert(mapHasSet(rootCache, roots));
        await tree.close();

        // make sure root is cleared on close.
        assert.strictEqual(rootCache.size, 0);

        // test
        await tree.open();
        // last meta root itself is getting cached.
        assert.strictEqual(rootCache.size, 1);

        const {lastMeta, lastCachedMeta} = tree.store;

        // cache one more thing.
        roots = new Set([
          tree.root.hash().toString('binary'),
          ...await populateTree(tree, 1, 2)
        ]);

        assert.strictEqual(rootCache.size, 2);
        assert.strictEqual(rootCache.size, roots.size);
        assert(mapHasSet(rootCache, roots));

        // lastCachedMeta does not go forward
        assert.strictEqual(lastCachedMeta, tree.store.lastCachedMeta);
        assert.notStrictEqual(lastMeta, tree.store.lastMeta);

        await tree.close();
      });

      it('should recover all hashes when initCacheSize is -1', async () => {
        const opts = {
          ...options,
          initCacheSize: -1
        };

        const tree = new Tree(opts);
        const {rootCache} = tree.store;

        await tree.open();
        const roots = await populateTree(tree, 10, 2);
        assert.strictEqual(rootCache.size, 10);
        assert(mapHasSet(rootCache, roots));
        await tree.close();

        // make sure root is cleared on close.
        assert.strictEqual(rootCache.size, 0);

        await tree.open();
        assert.strictEqual(rootCache.size, 10);
        assert(mapHasSet(rootCache, roots));
        await tree.close();
      });

      it('should recover initCacheSize roots in the cache', async () => {
        const initCacheSize = 2;
        // on open we index first root.
        const initialRootCount = initCacheSize + 1;

        const opts = {
          ...options,
          initCacheSize
        };

        const tree = new Tree(opts);
        const {rootCache} = tree.store;

        await tree.open();
        const roots = await populateTree(tree, 10, 2);
        assert.strictEqual(rootCache.size, 10);
        assert(mapHasSet(rootCache, roots));
        await tree.close();

        // make sure root is cleared on close.
        assert.strictEqual(rootCache.size, 0);

        await tree.open();
        assert.strictEqual(rootCache.size, initialRootCount);
        const lastThree = new Set([...roots].slice(-initialRootCount));
        assert(mapHasSet(rootCache, lastThree));

        // make sure lastCachedMeta is correctly set to the last indexed root.
        const last = [...rootCache.values()][initialRootCount - 1].hash();
        const checkRoot = await tree.store.readRoot(tree.store.lastCachedMeta.rootPtr);
        assert.bufferEqual(last, checkRoot.hash());

        await tree.close();
      });

      it('should not lookup disk (cacheOnly)', async () => {
        const opts = {
          ...options,
          cacheOnly: true
        };

        const tree = new Tree(opts);
        const {rootCache} = tree.store;

        let rootHashes = [];

        await tree.open();
        const roots = await populateTree(tree, 10, 2);
        assert.strictEqual(rootCache.size, 10);
        assert(mapHasSet(rootCache, roots));

        rootHashes = [...rootCache.values()].map(n => n.hash(tree.hash));
        await tree.close();

        // make sure root is cleared on close.
        assert.strictEqual(rootCache.size, 0);

        await tree.open();
        const {lastMeta, lastCachedMeta} = tree.store;
        assert.strictEqual(rootCache.size, 1);

        const checkTreeRoots = [];
        for (const hash of rootHashes.reverse()) {
          const snap = tree.snapshot(hash);

          try {
            const root = await snap.getRoot();
            checkTreeRoots.push(root);
          } catch (e) {
            checkTreeRoots.push(null);
          }
        }

        assert.notStrictEqual(checkTreeRoots[0], null);
        for (let i = 1; i < checkTreeRoots.length; i++)
          assert.strictEqual(checkTreeRoots[i], null);

        // neither lastMeta nor lastCachedMeta should change.
        assert.strictEqual(lastMeta, tree.store.lastMeta);
        assert.strictEqual(lastCachedMeta, tree.store.lastCachedMeta);

        await tree.close();
      });

      it('should only lookup initial cached roots (cacheOnly)', async () => {
        const initCacheSize = 2;
        const initialRootCount = initCacheSize + 1;

        const opts = {
          ...options,
          initCacheSize,
          cacheOnly: true
        };

        const tree = new Tree(opts);
        const {rootCache} = tree.store;

        let rootHashes = [];

        await tree.open();
        const roots = await populateTree(tree, 10, 2);
        assert.strictEqual(rootCache.size, 10);
        assert(mapHasSet(rootCache, roots));

        rootHashes = [...rootCache.values()].map(n => n.hash(tree.hash));
        await tree.close();

        // make sure root is cleared on close.
        assert.strictEqual(rootCache.size, 0);

        await tree.open();
        const {lastMeta, lastCachedMeta} = tree.store;
        assert.strictEqual(rootCache.size, initialRootCount);

        const checkTreeRoots = [];
        for (const hash of rootHashes.reverse()) {
          const snap = tree.snapshot(hash);

          try {
            const root = await snap.getRoot();
            checkTreeRoots.push(root);
          } catch (e) {
            checkTreeRoots.push(null);
          }
        }

        for (let i = 0; i < initialRootCount; i++)
          assert.notStrictEqual(checkTreeRoots[i], null);

        for (let i = initialRootCount + 1; i < checkTreeRoots.length; i++)
          assert.strictEqual(checkTreeRoots[i], null);

        // neither lastMeta nor lastCachedMeta should change.
        assert.strictEqual(lastMeta, tree.store.lastMeta);
        assert.strictEqual(lastCachedMeta, tree.store.lastCachedMeta);

        await tree.close();
      });

      it('should only lookup initial cached roots (not cacheOnly)', async () => {
        const initCacheSize = 2;
        const initialRootCount = initCacheSize + 1;

        const opts = {
          ...options,
          initCacheSize,
          cacheOnly: false
        };

        const tree = new Tree(opts);
        const {rootCache} = tree.store;

        let rootHashes = [];

        // prepare tree
        {
          await tree.open();
          const {lastMeta} = tree.store;
          const roots = await populateTree(tree, 10, 2);
          assert.strictEqual(rootCache.size, 10);
          assert(mapHasSet(rootCache, roots));

          rootHashes = [...rootCache.values()].map(n => n.hash(tree.hash));

          // lastMeta should have moved on.
          assert.notStrictEqual(lastMeta, tree.store.lastMeta);
          await tree.close();
        }

        // make sure root is cleared on close.
        assert.strictEqual(rootCache.size, 0);

        // test
        await tree.open();
        const {lastMeta, lastCachedMeta} = tree.store;

        assert.strictEqual(rootCache.size, initialRootCount);

        const checkTreeRoots = [];
        for (const hash of rootHashes.reverse()) {
          const snap = tree.snapshot(hash);

          try {
            const root = await snap.getRoot();
            checkTreeRoots.push(root);
          } catch (e) {
            checkTreeRoots.push(null);
          }
        }

        for (let i = 0; i < checkTreeRoots.length; i++)
          assert.notStrictEqual(checkTreeRoots[i], null);

        assert.strictEqual(lastMeta, tree.store.lastMeta);
        assert.notStrictEqual(lastCachedMeta, tree.store.lastCachedMeta);

        await tree.close();
      });

      it('should compact tree', async function() {
        if (memory || os.platform() !== 'linux')
          this.skip();

        const initCacheSize = -1;
        const name = 'tree';
        const finalPrefix = path.join(prefix, name);
        const isRandomPath = (dirname) => {
          const regex = new RegExp(`^${name}\.[0-9a-v]+~$`);
          return regex.test(dirname);
        };

        const opts = {
          ...options,
          prefix: finalPrefix,
          initCacheSize
        };
        const tree = new Tree(opts);
        await tree.open();

        const changes = [];
        const watcher = fs.watch(prefix, {
          persistent: false
        }, (_, file) => {
          changes.push(file);
        });

        watcher.unref();

        await populateTree(tree, 10, 10);
        await tree.compact();

        assert.strictEqual(changes.length, 4);
        // we create tmp dir
        assert(isRandomPath(changes[0]));
        // we remove tree
        assert.strictEqual(changes[1], name);
        // we move tmp to tree
        assert(isRandomPath(changes[2]));
        assert.strictEqual(changes[3], name);

        watcher.close();
        await tree.close();
      });

      it('should compact tree custom prefix', async () => {
        if (memory || os.platform() !== 'linux')
          this.skip();

        const initCacheSize = -1;
        const name = 'tree';
        const custom = 'tree.tmp';
        const finalPrefix = path.join(prefix, name);

        const opts = {
          ...options,
          prefix: finalPrefix,
          initCacheSize
        };
        const tree = new Tree(opts);
        await tree.open();

        const changes = [];
        const watcher = fs.watch(prefix, {
          persistent: false
        }, (_, file) => {
          changes.push(file);
        });

        watcher.unref();

        await populateTree(tree, 10, 10);
        await tree.compact(path.join(prefix, custom));

        assert.strictEqual(changes.length, 4);
        // we create tmp dir
        assert.strictEqual(changes[0], custom);
        // we remove tree
        assert.strictEqual(changes[1], name);
        // we move tmp to tree
        assert.strictEqual(changes[2], custom);
        assert.strictEqual(changes[3], name);

        watcher.close();
        await tree.close();
      });
    });
  }
});
