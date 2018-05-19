/*!
 * trie.js - patricia merkle trie implementation
 * Copyright (c) 2017, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 *
 * Patricia Merkle Tries:
 *   https://github.com/ethereum/wiki/wiki/Patricia-Tree
 *
 * Parts of this software are based on go-ethereum:
 *   Copyright (C) 2014 The go-ethereum Authors.
 *   https://github.com/ethereum/go-ethereum/tree/master/trie
 */

'use strict';

const assert = require('assert');
const blake2b = require('bcrypto/lib/blake2b');
const nodes = require('./nodes');
const common = require('./common');
const proof = require('./proof');
const Hasher = require('./hasher');
const PruneList = require('./prunelist');
const {Iterator} = require('./iterator');
const {MissingNodeError} = require('./errors');

/*
 * Constants
 */

const {
  EMPTY_ROOT,
  ZERO_HASH,
  toNibbles,
  prefixLen,
  concat,
  startsWith,
  byte
} = common;

const {
  NodeFlags,
  HashNode,
  ShortNode,
  FullNode,
  ValueNode,
  NIL,
  decodeNode
} = nodes;

const {
  NULLNODE,
  HASHNODE,
  SHORTNODE,
  FULLNODE,
  VALUENODE
} = nodes.types;

/**
 * Patricia Merkle Trie
 */

class Trie {
  /**
   * Create a trie.
   * @constructor
   * @param {Object|null} db
   * @param {Number} [limit=4]
   * @param {Number} [pruneMode=0]
   */

  constructor(db, limit = 4, pruneMode = 0) {
    assert((limit >>> 0) === limit);
    assert((pruneMode >>> 0) === pruneMode);
    assert(pruneMode <= 2);

    this.db = db || null;
    this.originalRoot = EMPTY_ROOT;
    this.root = NIL;
    this.cacheGen = 0;
    this.cacheLimit = limit;
    this.pruneMode = pruneMode;
    this.pruneList = new PruneList();
  }

  flags() {
    return new NodeFlags(this.cacheGen, true);
  }

  shortNode(key, value) {
    return new ShortNode(key, value, this.flags());
  }

  fullNode() {
    return new FullNode(this.flags());
  }

  addPrune(n) {
    if (this.pruneMode !== 0)
      this.pruneList.add(n);
  }

  async open(root) {
    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    assert(!root || Buffer.isBuffer(root));

    // Try to retrieve best state.
    if (!root && this.db)
      root = await this.db.get(ZERO_HASH);

    if (root && !root.equals(EMPTY_ROOT)) {
      assert(root.length === 32);

      if (!this.db)
        throw new Error('Cannot use root without database.');

      if (!await this.db.has(root)) {
        throw new MissingNodeError({
          rootHash: root,
          nodeHash: root
        });
      }

      this.originalRoot = root;
      this.root = new HashNode(root);
    }
  }

  async close() {
    this.root = NIL;
    this.originalRoot = EMPTY_ROOT;
    this.cacheGen = 0;
  }

  async get(key) {
    const k = toNibbles(key);
    const [val, root, res] = await this._get(this.root, k, 0);

    if (res)
      this.root = root;

    return val;
  }

  async _get(n, key, pos) {
    assert(pos <= key.length);

    switch (n.type) {
      case NULLNODE: {
        return [null, NIL, false];
      }
      case VALUENODE: {
        return [n.data, n, false];
      }
      case SHORTNODE: {
        if (!startsWith(key, n.key, pos)) {
          // Key not found.
          return [null, n, false];
        }

        const [val, nn, res] =
          await this._get(n.value, key, pos + n.key.length);

        if (res) {
          n = n.clone();
          n.value = nn;
        }

        return [val, n, res];
      }
      case FULLNODE: {
        const c = n.children[key[pos]];
        const [val, nn, res] = await this._get(c, key, pos + 1);

        if (res) {
          n = n.clone();
          n.children[key[pos]] = nn;
        }

        return [val, n, res];
      }
      case HASHNODE: {
        const child = await this.resolveHash(n, key, pos);
        const [val, nn] = await this._get(child, key, pos);
        return [val, nn, true];
      }
      default: {
        throw new Error('Invalid node type.');
      }
    }
  }

  async insert(key, value) {
    assert(Buffer.isBuffer(value));

    const k = toNibbles(key);
    const node = new ValueNode(value);
    const [, root] = await this._insert(this.root, k, 0, node);

    this.root = root;
  }

  async _insert(n, key, pos, value) {
    assert(pos <= key.length);

    if (key.length - pos === 0) {
      if (n.isValue()) {
        const d = !n.data.equals(value.data);

        if (d)
          this.addPrune(n);

        return [d, value];
      }
      this.addPrune(n);
      return [true, value];
    }

    switch (n.type) {
      case SHORTNODE: {
        const ml = prefixLen(key, n.key, pos);

        if (ml === n.key.length) {
          const [d, nn] = await this._insert(n.value, key, pos + ml, value);

          if (!d)
            return [false, n];

          this.addPrune(n);

          return [true, this.shortNode(n.key, nn)];
        }

        this.addPrune(n);

        const branch = this.fullNode();

        const [, n1] = await this._insert(NIL, n.key, ml + 1, n.value);
        const [, n2] = await this._insert(NIL, key, pos + ml + 1, value);

        branch.children[n.key[ml]] = n1;
        branch.children[key[pos + ml]] = n2;

        if (ml === 0)
          return [true, branch];

        return [true, this.shortNode(key.slice(pos, pos + ml), branch)];
      }
      case FULLNODE: {
        const c = n.children[key[pos]];
        const [d, nn] = await this._insert(c, key, pos + 1, value);

        if (!d)
          return [false, n];

        this.addPrune(n);

        n = n.clone();
        n.children[key[pos]] = nn;
        n.flags.hash = null;
        n.flags.dirty = true;

        return [true, n];
      }
      case NULLNODE: {
        return [true, this.shortNode(key.slice(pos), value)];
      }
      case HASHNODE: {
        const rn = await this.resolveHash(n, key, pos);
        const [d, nn] = await this._insert(rn, key, pos, value);

        if (!d)
          return [false, rn];

        return [true, nn];
      }
      default: {
        throw new Error('Invalid node type.');
      }
    }
  }

  async remove(key) {
    const k = toNibbles(key);
    const [, root] = await this._remove(this.root, k, 0);

    this.root = root;
  }

  async _remove(n, key, pos) {
    assert(pos <= key.length);

    switch (n.type) {
      case SHORTNODE: {
        const ml = prefixLen(key, n.key, pos);

        if (ml < n.key.length)
          return [false, n];

        if (ml === key.length - pos) {
          this.addPrune(n);
          return [true, NIL];
        }

        const [d, nn] = await this._remove(n.value, key, pos + n.key.length);

        if (!d)
          return [false, n];

        this.addPrune(n);

        if (nn.isShort()) {
          const nk = concat(n.key, nn.key);
          return [true, this.shortNode(nk, nn.value)];
        }

        return [true, this.shortNode(n.key, nn)];
      }
      case FULLNODE: {
        const c = n.children[key[pos]];
        const [d, nn] = await this._remove(c, key, pos + 1);

        if (!d)
          return [false, n];

        this.addPrune(n);

        n = n.clone();
        n.children[key[pos]] = nn;
        n.flags.hash = null;
        n.flags.dirty = true;

        let index = -1;
        for (let i = 0; i < 17; i++) {
          const child = n.children[i];
          if (!child.isNull()) {
            if (index === -1) {
              index = i;
            } else {
              index = -2;
              break;
            }
          }
        }

        if (index >= 0) {
          if (index !== 16) {
            const child = await this.resolve(n.children[index], key, index);
            if (child.isShort()) {
              const nk = concat(byte(index), child.key);
              return [true, this.shortNode(nk, child.value)];
            }
          }

          return [true, this.shortNode(byte(index), n.children[index])];
        }

        return [true, n];
      }
      case VALUENODE: {
        this.addPrune(n);
        return [true, NIL];
      }
      case NULLNODE: {
        return [false, NIL];
      }
      case HASHNODE: {
        const rn = await this.resolveHash(n, key, pos);
        const [d, nn] = await this._remove(rn, key, pos);

        if (!d)
          return [false, rn];

        return [true, nn];
      }
      default: {
        throw new Error('Invalid node type.');
      }
    }
  }

  async resolve(n, key, index) {
    if (n.isHash()) {
      const k = concat(key, byte(index));
      const p = key.length;
      return this.resolveHash(n, k, p);
    }
    return n;
  }

  async resolveHash(n, key, pos = 0) {
    if (!this.db)
      throw new Error('Cannot resolve hash without database.');

    const raw = await this.db.get(n.data);

    if (!raw) {
      throw new MissingNodeError({
        rootHash: this.originalRoot,
        nodeHash: n.data,
        key: key,
        pos: pos
      });
    }

    return decodeNode(raw);
  }

  async getPruneList(root) {
    if (!this.db)
      throw new Error('Cannot read prune list without database.');

    if (this.pruneMode !== 2)
      throw new Error('Invalid prune mode.');

    const key = PruneList.key(root);
    const data = await this.db.get(key);

    if (!data) {
      throw new MissingNodeError({
        rootHash: root,
        nodeHash: key
      });
    }

    return [key, PruneList.decode(data)];
  }

  async pruneRoot(batch, root) {
    let key, list;

    try {
      [key, list] = await this.getPruneList(root);
    } catch (e) {
      if (e.code === 'MissingNodeError')
        return;
      throw e;
    }

    list.prune(batch);

    batch.del(key);
  }

  iterator() {
    return new Iterator(this);
  }

  hash(enc) {
    const [hash, cached] = this.hashRoot(null);

    this.root = cached;

    if (enc === 'hex')
      return hash.data.toString('hex');

    return hash.data;
  }

  commit(batch, enc) {
    assert(batch);

    const [hash, cached] = this.hashRoot(batch);

    // Write best state.
    batch.put(ZERO_HASH, hash.data);

    // Handle pruning if enabled.
    switch (this.pruneMode) {
      case 0:
        break;
      case 1:
        this.pruneList.prune(batch);
        break;
      case 2:
        this.pruneList.save(batch, hash.data);
        break;
      default:
        throw new Error('Unknown prune mode.');
    }

    this.originalRoot = hash.data;
    this.root = cached;
    this.cacheGen += 1;

    if (enc === 'hex')
      return hash.data.toString('hex');

    return hash.data;
  }

  hashRoot(batch) {
    if (this.root.isNull())
      return [new HashNode(EMPTY_ROOT), NIL];

    const {cacheGen, cacheLimit} = this;
    const hasher = new Hasher(cacheGen, cacheLimit);

    return hasher.hash(this.root, batch, true);
  }

  snapshot(root) {
    if (root == null)
      root = this.originalRoot;

    if (!this.db)
      throw new Error('Cannot snapshot without database.');

    const {db, cacheLimit, pruneMode} = this;
    const trie = new this.constructor(db, cacheLimit, pruneMode);

    return trie.inject(root);
  }

  inject(root) {
    if (root == null)
      root = this.originalRoot;

    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    assert(Buffer.isBuffer(root));
    assert(root.length === 32);

    this.originalRoot = EMPTY_ROOT;
    this.root = NIL;
    this.cacheGen = 0;

    if (!root.equals(EMPTY_ROOT)) {
      this.originalRoot = root;
      this.root = new HashNode(root);
    }

    return this;
  }

  async prove(key) {
    return proof.prove(this, key);
  }

  verify(root, key, nodes) {
    return proof.verify(root, key, nodes);
  }
}

/*
 * Expose
 */

module.exports = Trie;
