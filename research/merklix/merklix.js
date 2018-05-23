/*!
 * merklix.js - merklix tree
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 *
 * Merklix Trees:
 *   https://www.deadalnix.me/2016/09/24/introducing-merklix-tree-as-an-unordered-merkle-tree-on-steroid/
 *   https://www.deadalnix.me/2016/09/29/using-merklix-tree-to-checkpoint-an-utxo-set/
 */

/* eslint no-use-before-define: "off" */

// Merklix Tree
//
// Much like a patricia trie or a sparse merkle tree, the merklix tree follows
// a path down each key in order to find the target leaf node.
//
// Insertion explanation:
//
// We start with a simple insertion of leaf `1` (0000).
// It becomes the root (`R`) of the merkle tree.
//
//        R
//
// Say we insert leaf `2` (1100). The tree grows down
// and we are now 1 level deep. Note that we only went
// right once even though we have 3 extra bits in the key.
//
//        R
//       / \
//      /   \
//     1     2
//
// This next part is important to how the merklix tree handles key
// collisions. Say we insert leaf `3` (1101). You'll notice it has
// a three bit collision with leaf `2`. In order to maintain a proper key
// path within the tree, we grow down and add "null" nodes (represented by
// `x`) as children of internal nodes. This is basically a sign that there's
// a dead end in one of the internal nodes. This is the trick to keeping the
// merklix tree small and ever-growing, unlike a sparse merkle tree for
// example.
//
//        R
//       / \
//      /   \
//     1    /\
//         /  \
//        x   /\
//           /  \
//          /\   x
//         /  \
//        2    3
//
// If we add leaf 4 (1000), it is free to consume one of the "null" nodes.
//
//        R
//       / \
//      /   \
//     1    /\
//         /  \
//        4   /\
//           /  \
//          /\   x
//         /  \
//        2    3
//
// Proof explanation:
//
// The proof is a standard merkle proof, with some extra gotchas.
// The actual hash at a leaf is the computed as HASH(key | value).
// It is important to have the full key as part of the preimage. If a
// non-existence proof is necessary, we need to send the full preimage to
// prove that we are a leaf, and that we're also a different key that may
// have a colliding path with whatever key a peer is trying to get a proof
// for. On the other hand, if the key path stops at one of the "dead-end"
// nodes, we do not have to send any preimage! Even better, if there are any
// "dead-end" nodes up the branch when creating a proof, we can compress them
// since they are redundant zero-hashes.
//
// Removal:
// Removal is tricky when we have "dead-end" nodes in our branch. We need to
// revert all of the branch growing we just did.
//
// If we were to remove leaf 4 from the above tree, we _must_ replace it with
// a "dead-end". The general rule is: if the target node's sibling is an
// internal node, replace with a null node. If the sibling is another leaf,
// attempt to ungrow the branch by detecting key collisions.
//
// Removing leaf 4 (we _must_ replace with a dead-end):
//
//        R
//       / \
//      /   \
//     1    /\
//         /  \
//        x   /\
//           /  \
//          /\   x
//         /  \
//        2    3
//
// Removing leaf 3 (ungrow the branch):
//
//        R
//       / \
//      /   \
//     1     2
//
// And we're back to where we started.
//
// Optimizing a merklix tree on disk:
//
// Due to the sheer number of nodes, a flat-file store is necessary. The amount
// of database lookups would be overwhelming for something like leveldb. A
// merklix tree is much simpler than a patricia trie in that we need only store
// 2 nodes: internal nodes and leaves.
//
// Internal nodes are stored as:
// struct {
//   uint8_t left_hash[32];
//   uint16_t left_file;
//   uint32_t left_position;
//   uint8_t right_hash[32];
//   uint16_t right_file;
//   uint32_t right_position;
// }
//
// Leaf nodes are stored as:
// struct {
//   uint8_t leaf_hash[32];
//   uint8_t key[20];
//   uint16_t value_file;
//   uint32_t value_position;
//   uint32_t value_size;
// }
//
// The actual leaf data is stored at `value_position` in `value_file`.

'use strict';

const assert = require('assert');
const LRU = require('blru');
const fs = require('bfile');
const Path = require('path');
const {ensureHash} = require('../../lib/common');

/*
 * Constants
 */

const STATE_KEY = Buffer.from([0x73]);
const INTERNAL_PREFIX = Buffer.from([0x00]);
const LEAF_PREFIX = Buffer.from([0x01]);

/*
 * Error Codes
 */

const PROOF_OK = 0;
const PROOF_HASH_MISMATCH = 1;
const PROOF_MALFORMED_NODE = 2;
const PROOF_UNEXPECTED_NODE = 3;
const PROOF_EARLY_END = 4;
const PROOF_NO_RESULT = 5;

const NULL = 0;
const INTERNAL = 1;
const LEAF = 2;
const HASH = 3;

const NODE_SIZE = 77; // 1 + 32 + 2 + 4 + 32 + 2 + 4
const LEAF_SIZE = 63; // 1 + 32 + 20 + 2 + 4 + 4
const MAX_FILE_SIZE = 0x7fffffff;
const MAX_FILES = 0xffff;

class Node {
  constructor(type) {
    this.type = type;
    this.index = 0;
    this.pos = 0;
  }

  hash(ctx) {
    return ctx.constructor.zero;
  }

  encode(ctx) {
    throw new Error('Unimplemented.');
  }

  decode(data) {
    throw new Error('Unimplemented.');
  }

  async getLeft(store) {
    throw new Error('Unimplemented.');
  }

  async getRight(store) {
    throw new Error('Unimplemented.');
  }

  async getValue(store) {
    throw new Error('Unimplemented.');
  }

  async resolve(store) {
    return this;
  }

  static decode(data) {
    return new this().decode(data);
  }
}

class Null extends Node {
  constructor(type) {
    super(NULL);
  }

  inspect() {
    return '<NIL>';
  }
}

const NIL = new Null();
const SLAB = Buffer.allocUnsafe(NODE_SIZE);

class Internal extends Node {
  constructor(left, right) {
    super(INTERNAL);

    // Not serialized.
    this.data = null;
    this.index = 0;
    this.pos = 0;
    this.gen = 0;

    this.left = left || NIL;
    this.right = right || NIL;
  }

  hash(ctx) {
    if (!this.data) {
      const left = this.left.hash(ctx);
      const right = this.right.hash(ctx);

      this.data = hashInternal(ctx, left, right);
    }

    return this.data;
  }

  encode(ctx) {
    // const data = Buffer.allocUnsafe(NODE_SIZE);
    const data = SLAB;
    const left = this.left.hash(ctx);
    const right = this.right.hash(ctx);

    data[0] = INTERNAL;

    left.copy(data, 1);
    data.writeUInt16LE(this.left.index, 33);
    data.writeUInt32LE(this.left.pos, 35);

    right.copy(data, 39);
    data.writeUInt16LE(this.right.index, 71);
    data.writeUInt32LE(this.right.pos, 73);

    return data;
  }

  decode(data) {
    assert(data.length === NODE_SIZE);
    assert(data[0] === INTERNAL);

    const left = data.slice(1, 33);
    const right = data.slice(39, 71);

    if (!isZero(left)) {
      const leftIndex = data.readUInt16LE(33, true);
      const leftPos = data.readUInt32LE(35, true);

      this.left = new Hash(left, leftIndex, leftPos);
    }

    if (!isZero(right)) {
      const rightIndex = data.readUInt16LE(71, true);
      const rightPos = data.readUInt32LE(73, true);

      this.right = new Hash(right, rightIndex, rightPos);
    }

    return this;
  }

  async getLeft(store) {
    if (this.left.type === HASH)
      this.left = await this.left.resolve(store);

    return this.left;
  }

  async getRight(store) {
    if (this.right.type === HASH)
      this.right = await this.right.resolve(store);

    return this.right;
  }

  inspect() {
    return {
      left: this.left.inspect(),
      right: this.right.inspect()
    };
  }
}

class Leaf extends Node {
  constructor(leaf, key, value) {
    super(LEAF);

    // Not serialized.
    this.index = 0;
    this.pos = 0;
    this.value = value || null;

    this.data = leaf || null;
    this.key = key || null;
    this.vindex = 0;
    this.vpos = 0;
    this.vsize = 0;
  }

  hash() {
    assert(this.data);
    return this.data;
  }

  encode(ctx) {
    // const data = Buffer.allocUnsafe(NODE_SIZE);
    const data = SLAB;
    data[0] = LEAF;
    this.data.copy(data, 1);
    this.key.copy(data, 33);
    data.writeUInt16LE(this.vindex, 53, true);
    data.writeUInt32LE(this.vpos, 55, true);
    data.writeUInt32LE(this.vsize, 59, true);
    data.fill(0x00, LEAF_SIZE);
    return data;
  }

  decode(data) {
    assert(data.length === NODE_SIZE);
    assert(data[0] === LEAF);
    this.data = data.slice(1, 33);
    this.key = data.slice(33, 53);
    this.vindex = data.readUInt16LE(53, true);
    this.vpos = data.readUInt32LE(55, true);
    this.vsize = data.readUInt32LE(59, true);
    return this;
  }

  async getValue(store) {
    if (!this.value) {
      const {vindex, vpos, vsize} = this;
      this.value = await store.read(vindex, vpos, vsize);
    }

    return this.value;
  }

  inspect() {
    return `<Leaf: ${this.key.toString('hex')}>`;
  }
}

class Hash extends Node {
  constructor(hash, index, pos) {
    super(HASH);
    this.data = hash || null;
    this.index = index || 0;
    this.pos = pos || 0;
  }

  hash(ctx) {
    assert(this.data);
    return this.data;
  }

  async resolve(store) {
    const node = await store.readNode(this.index, this.pos);
    node.data = this.data;
    return node;
  }

  inspect() {
    return `<Hash: ${this.data.toString('hex')}>`;
  }
}

function decodeNode(data, index, pos) {
  let node;

  assert(data.length > 0);

  switch (data[0]) {
    case NULL:
      throw new Error('Database corruption.');
    case INTERNAL:
      node = Internal.decode(data);
      break;
    case LEAF:
      node = Leaf.decode(data);
      break;
    case HASH:
      throw new Error('Database corruption.');
    default:
      throw new Error('Database corruption.');
  }

  node.index = index;
  node.pos = pos;

  return node;
}

class File {
  constructor(index) {
    this.index = index;
    this.fd = -1;
    this.pos = 0;
  }

  async open(filename, flags) {
    if (this.fd !== -1)
      throw new Error('File already open.');

    this.fd = await fs.open(filename, flags, 0o660);

    const stat = await fs.fstat(this.fd);

    this.pos = stat.size;
  }

  async close() {
    if (this.fd === -1)
      throw new Error('File already closed.');

    await fs.fsync(this.fd);
    await fs.close(this.fd);

    this.fd = -1;
    this.pos = 0;
  }

  async read(pos, bytes) {
    const buf = Buffer.allocUnsafe(bytes);
    const r = await fs.read(this.fd, buf, 0, bytes, pos);
    assert.strictEqual(r, bytes);
    return buf;
  }

  async write(data) {
    const pos = this.pos;
    const w = await fs.write(this.fd, data, 0, data.length, null);
    assert.strictEqual(w, data.length);
    this.pos += w;
    return pos;
  }

  async sync() {
    return fs.fsync(this.fd);
  }
}

class FileStore {
  constructor(prefix) {
    this.prefix = prefix || '/';
    this.arena = new Arena();
    this.files = [];
    this.current = null;
    this.index = 0;
  }

  name(num) {
    assert((num >>> 0) === num);

    let name = num.toString(10);

    while (name.length < 10)
      name = '0' + name;

    return Path.resolve(this.prefix, name);
  }

  async ensure() {
    await fs.mkdirp(this.prefix, 0o770);

    const list = await fs.readdir(this.prefix);

    let index = 1;

    for (const name of list) {
      const num = parseU32(name);

      if (num > index)
        index = num;
    }

    return index;
  }

  async open() {
    if (this.index !== 0)
      throw new Error('Files already opened.');

    this.index = await this.ensure();
    this.current = await this.openFile(this.index, 'a+');
  }

  async close() {
    if (this.index === 0)
      throw new Error('File already closed.');

    for (let i = 0; i < this.files.length; i++)
      await this.closeFile(i);

    this.files.length = 0;
    this.current = null;
    this.index = 0;
  }

  async openFile(index, flags) {
    assert(index !== 0);

    while (index >= this.files.length)
      this.files.push(null);

    if (!this.files[index]) {
      const file = new File(index);
      const name = this.name(index);

      await file.open(name, flags);

      this.files[index] = file;
    }

    return this.files[index];
  }

  async closeFile(index) {
    const file = this.files[index];

    if (!file)
      return;

    await file.close();

    this.files[index] = null;
  }

  async unlinkFile(index) {
    assert(index !== this.index);
    await this.closeFile(index);
    await fs.unlink(this.name(index));
  }

  async read(index, pos, bytes) {
    const file = await this.openFile(index, 'r');
    return file.read(pos, bytes);
  }

  async write(data) {
    if (this.current.pos + data.length > MAX_FILE_SIZE) {
      await this.closeFile(this.index);
      this.current = await this.openFile(this.index + 1, 'a+');
      this.index += 1;
    }

    return this.current.write(data);
  }

  async sync() {
    return this.current.sync();
  }

  async readNode(index, pos) {
    const data = await this.read(index, pos, NODE_SIZE);
    return decodeNode(data, index, pos);
  }

  async readRoot() {
    if (this.current.pos < NODE_SIZE)
      return NIL;
    return this.readNode(this.current.pos - NODE_SIZE);
  }

  start() {
    this.arena.offset = this.current.pos;
    this.arena.index = this.current.index;
    this.arena.start = 0;
    this.arena.written = 0;
    return this;
  }

  writeNode(node, ctx) {
    const data = node.encode(ctx);
    node.pos = this.arena.write(data);
    node.index = this.arena.index;
    return node.pos;
  }

  writeValue(node) {
    assert(node.type === LEAF);
    assert(node.index === 0);
    node.vsize = node.value.length;
    node.vpos = this.arena.write(node.value);
    node.vindex = this.arena.index;
    return node.vpos;
  }

  async flush() {
    for (const chunk of this.arena.flush())
      await this.write(chunk);
  }
}

class Arena {
  constructor() {
    this.offset = 0;
    this.index = 0;
    this.start = 0;
    this.written = 0;
    this.data = Buffer.allocUnsafe(0);
    this.chunks = [];
  }

  position(written) {
    return this.offset + (written - this.start);
  }

  write(data) {
    if (this.data.length === 0)
      this.data = Buffer.allocUnsafe(1024);

    while (this.written + data.length > this.data.length) {
      const buf = Buffer.allocUnsafe(this.data.length * 2);
      this.data.copy(buf, 0);
      this.data = buf;
    }

    if (this.position(this.written) + data.length > MAX_FILE_SIZE) {
      this.chunks.push(this.render());
      this.start = this.written;
      this.offset = 0;
      this.index += 1;
    }

    const written = this.written;
    this.written += data.copy(this.data, this.written);

    return this.position(written);
  }

  render() {
    return this.data.slice(this.start, this.written);
  }

  flush() {
    const chunks = this.chunks;

    if (this.written > this.start)
      chunks.push(this.render());

    this.chunks = [];

    return chunks;
  }
}

class MemoryStore {
  constructor(prefix) {
    this.pos = 0;
    this.index = 0;
    this.data = Buffer.allocUnsafe(0);
  }

  async open() {
    if (this.index !== 0)
      throw new Error('Files already opened.');

    this.index = 1;
  }

  async close() {
    if (this.index === 0)
      throw new Error('File already closed.');

    this.index = 0;
  }

  async read(index, pos, bytes) {
    assert(pos + bytes <= this.pos);
    const buf = Buffer.allocUnsafe(bytes);
    this.data.copy(buf, 0, pos, pos + bytes);
    return buf;
  }

  write(data) {
    if (this.data.length === 0)
      this.data = Buffer.allocUnsafe(1024);

    while (this.pos + data.length > this.data.length) {
      const buf = Buffer.allocUnsafe(this.data.length * 2);
      this.data.copy(buf, 0);
      this.data = buf;
    }

    const pos = this.pos;
    this.pos += data.copy(this.data, this.pos);

    return pos;
  }

  async sync() {}

  async readNode(index, pos) {
    const data = await this.read(index, pos, NODE_SIZE);
    return decodeNode(data, index, pos);
  }

  async readRoot() {
    if (this.pos < NODE_SIZE)
      return NIL;
    return this.readNode(1, this.pos - NODE_SIZE);
  }

  start() {
    return this;
  }

  writeNode(node, ctx) {
    const data = node.encode(ctx);
    node.pos = this.write(data);
    node.index = 1;
    return node.pos;
  }

  writeValue(node) {
    assert(node.type === LEAF);
    assert(node.index === 0);
    node.vsize = node.value.length;
    node.vpos = this.write(node.value);
    node.vindex = 1;
    return node.vpos;
  }

  async flush() {}
}

/**
 * Merklix
 */

class Merklix {
  /**
   * Create a merklix tree.
   * @constructor
   * @param {Object} hash
   * @param {Number} bits
   * @param {String} prefix
   * @param {Number} [limit=4]
   */

  constructor(hash, bits, prefix, limit) {
    if (limit == null)
      limit = 4;

    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(!prefix || typeof prefix === 'string');
    assert((limit >>> 0) === limit);

    this.hash = ensureHash(hash);
    this.bits = bits;
    this.store = prefix ? new FileStore(prefix) : new MemoryStore();
    this.originalRoot = this.hash.zero;
    this.root = NIL;
    this.cacheGen = 0;
    this.cacheLimit = limit;
    this.context = null;
  }

  isKey(key) {
    if (!Buffer.isBuffer(key))
      return false;
    return key.length === (this.bits >>> 3);
  }

  isHash(hash) {
    if (!Buffer.isBuffer(hash))
      return false;
    return hash.length === this.hash.size;
  }

  ctx() {
    if (!this.context)
      this.context = this.hash.hash();
    return this.context;
  }

  hashInternal(left, right) {
    return hashInternal(this.ctx(), left, right);
  }

  hashLeaf(key, value) {
    return hashLeaf(this.ctx(), key, value);
  }

  async open() {
    await this.store.open();

    this.root = await this.store.readRoot();
    this.originalRoot = this.root.hash(this.ctx());
  }

  async close() {
    await this.store.close();
  }

  async getRoot() {
    if (this.root.type === HASH)
      this.root = await this.root.resolve(this.store);
    return this.root;
  }

  async _get(root, key) {
    let node = root;
    let depth = 0;

    // Traverse bits left to right.
    for (;;) {
      // Empty (sub)tree.
      if (node.type === NULL)
        break;

      // Leaf node.
      if (node.type === LEAF) {
        // Prefix collision.
        if (!key.equals(node.key))
          node = NIL;
        break;
      }

      if (depth === this.bits) {
        throw new MissingNodeError({
          rootHash: root.hash(this.ctx()),
          key,
          depth
        });
      }

      assert(node.type === INTERNAL);

      // Internal node.
      if (hasBit(key, depth))
        node = await node.getRight(this.store);
      else
        node = await node.getLeft(this.store);

      depth += 1;
    }

    if (node.type === NULL)
      return null;

    return node.getValue(this.store);
  }

  async get(key) {
    assert(this.isKey(key));

    const root = await this.getRoot();

    return this._get(root, key);
  }

  async _insert(root, key, value) {
    const leaf = this.hashLeaf(key, value);
    const nodes = [];

    let node = root;
    let depth = 0;
    let next;

    // Traverse bits left to right.
    for (;;) {
      // Empty (sub)tree.
      if (node.type === NULL) {
        // Replace the empty node.
        break;
      }

      // Leaf node.
      if (node.type === LEAF) {
        // Current key.
        const other = node.key;

        if (key.equals(other)) {
          // Exact leaf already exists.
          if (leaf.equals(node.data))
            return root;

          // The branch doesn't grow.
          // Replace the current node.
          break;
        }

        assert(depth !== this.bits);

        // Insert placeholder leaves to grow
        // the branch if we have bit collisions.
        while (hasBit(key, depth) === hasBit(other, depth)) {
          // Child-less sidenode.
          nodes.push(NIL);
          depth += 1;
        }

        // Leaf is our sibling.
        nodes.push(node);
        depth += 1;

        break;
      }

      if (depth === this.bits) {
        throw new MissingNodeError({
          rootHash: root.hash(this.ctx()),
          key,
          depth
        });
      }

      assert(node.type === INTERNAL);

      // Internal node.
      if (hasBit(key, depth)) {
        nodes.push(node.left);
        node = await node.getRight(this.store);
      } else {
        nodes.push(node.right);
        node = await node.getLeft(this.store);
      }

      depth += 1;
    }

    // Start at the leaf.
    next = new Leaf(leaf, key, value);

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      depth -= 1;

      if (hasBit(key, depth))
        next = new Internal(node, next);
      else
        next = new Internal(next, node);
    }

    return next;
  }

  async insert(key, value) {
    assert(this.isKey(key));

    const root = await this.getRoot();

    this.root = await this._insert(root, key, value);

    return this.root;
  }

  async _remove(root, key) {
    const nodes = [];

    let node = root;
    let depth = 0;
    let next;

    // Traverse bits left to right.
    for (;;) {
      // Empty (sub)tree.
      if (node.type === NULL)
        return root;

      // Leaf node.
      if (node.type === LEAF) {
        // Current key.
        const other = node.key;

        if (!key.equals(other))
          return root;

        // Root can be a leaf.
        if (depth === 0) {
          // Remove the root.
          return NIL;
        }

        // Sibling.
        let s = nodes.pop();
        depth -= 1;

        if (s.type === HASH)
          s = await s.resolve(this.store);

        // Shrink the subtree if we're a leaf.
        if (s.type === LEAF) {
          // Sanity check (last comparison should have been different).
          assert(hasBit(key, depth) !== hasBit(s.key, depth));

          while (depth > 0) {
            const side = nodes[depth - 1];

            if (hasBit(key, depth - 1) !== hasBit(s.key, depth - 1))
              break;

            if (side.type !== NULL)
              break;

            nodes.pop();
            depth -= 1;
          }

          next = s;
        } else {
          assert(s.type === INTERNAL);
          nodes.push(s);
          depth += 1;
          next = NIL;
        }

        break;
      }

      if (depth === this.bits) {
        throw new MissingNodeError({
          rootHash: root.hash(this.ctx()),
          key,
          depth
        });
      }

      assert(node.type === INTERNAL);

      // Internal node.
      if (hasBit(key, depth)) {
        nodes.push(node.left);
        node = await node.getRight(this.store);
      } else {
        nodes.push(node.right);
        node = await node.getLeft(this.store);
      }

      depth += 1;
    }

    // Traverse bits right to left.
    while (nodes.length > 0) {
      const node = nodes.pop();

      depth -= 1;

      if (hasBit(key, depth))
        next = new Internal(node, next);
      else
        next = new Internal(next, node);
    }

    return next;
  }

  async remove(key) {
    assert(this.isKey(key));

    const root = await this.getRoot();

    this.root = await this._remove(root, key);

    return this.root;
  }

  rootHash(enc) {
    const ctx = this.ctx();
    const hash = this.root.hash(ctx);

    if (enc === 'hex')
      return hash.toString('hex');

    return hash;
  }

  async commit(_, enc) {
    this.store.start();

    this.root = this._commit(this.root, this.ctx());

    await this.store.flush();

    this.originalRoot = this.rootHash();

    if (enc === 'hex')
      return this.originalRoot.toString('hex');

    return this.originalRoot;
  }

  _commit(node, ctx) {
    switch (node.type) {
      case NULL: {
        assert(node.index === 0);
        return node;
      }

      case INTERNAL: {
        node.left = this._commit(node.left, ctx);
        node.right = this._commit(node.right, ctx);

        if (node.index === 0)
          this.store.writeNode(node, ctx);

        assert(node.index !== 0);

        if (node.gen === this.cacheLimit)
          return new Hash(node.hash(ctx), node.index, node.pos);

        node.gen += 1;

        return node;
      }

      case LEAF: {
        if (node.index === 0) {
          assert(node.value);
          this.store.writeValue(node);
          this.store.writeNode(node, ctx);
        }

        assert(node.index !== 0);

        return new Hash(node.hash(ctx), node.index, node.pos);
      }

      case HASH: {
        return node;
      }
    }

    throw new Error('Unknown node.');
  }

  snapshot(root) {
    if (root == null)
      root = this.originalRoot;

    if (!this.db)
      throw new Error('Cannot snapshot without database.');

    const {hash, bits, cacheLimit} = this;
    const tree = new this.constructor(hash, bits, null, cacheLimit);
    tree.store = this.store;
    tree.context = this.context;

    return tree.inject(root);
  }

  inject(root) {
    if (root == null)
      root = this.originalRoot;

    if (typeof root === 'string')
      root = Buffer.from(root, 'hex');

    assert(Buffer.isBuffer(root));
    assert(root.length === this.hash.size);

    this.root = root;
    this.originalRoot = root;

    return this;
  }

  async prove(root, key) {
    if (key == null) {
      key = root;
      root = await this.getRoot();
    }
    return proofs.prove(this, root, key);
  }

  verify(root, key, proof) {
    return proofs.verify(this.hash, this.bits, root, key, proof);
  }

  static get proof() {
    return proofs;
  }

  static get Proof() {
    return Proof;
  }
}

/**
 * Proofs
 */

const proofs = {};

proofs.prove = async function prove(tree, root, key) {
  assert(tree instanceof Merklix);
  assert(root instanceof Node);
  assert(tree.isKey(key));

  const nodes = [];
  const ctx = tree.ctx();

  let node = root;
  let depth = 0;
  let k = null;
  let v = null;

  // Traverse bits left to right.
  for (;;) {
    // Empty (sub)tree.
    if (node.type === NULL) {
      nodes.push(node.hash(ctx));
      break;
    }

    // Leaf node.
    if (node.type === LEAF) {
      nodes.push(node.hash(ctx));

      if (!key.equals(node.key))
        k = node.key;

      v = await node.getValue(tree.store);

      break;
    }

    if (depth === tree.bits) {
      throw new MissingNodeError({
        rootHash: root.hash(ctx),
        key,
        depth
      });
    }

    assert(node.type === INTERNAL);

    // Internal node.
    if (hasBit(key, depth)) {
      nodes.push(node.left.hash(ctx));
      node = await node.getRight(tree.store);
    } else {
      nodes.push(node.right.hash(ctx));
      node = await node.getLeft(tree.store);
    }

    depth += 1;
  }

  return new Proof(nodes, k, v);
};

proofs.verify = function verify(hash, bits, root, key, proof) {
  assert(hash && typeof hash.digest === 'function');
  assert((bits >>> 0) === bits);
  assert(bits > 0 && (bits & 7) === 0);
  assert(Buffer.isBuffer(root));
  assert(Buffer.isBuffer(key));
  assert(root.length === hash.size);
  assert(key.length === (bits >>> 3));
  assert(proof instanceof Proof);

  const nodes = proof.nodes;

  if (nodes.length === 0)
    return [PROOF_EARLY_END, null];

  if (nodes.length > bits)
    return [PROOF_MALFORMED_NODE, null];

  const ctx = hash.hash();
  const leaf = nodes[nodes.length - 1];

  let next = leaf;
  let depth = nodes.length - 2;

  // Traverse bits right to left.
  while (depth >= 0) {
    const node = nodes[depth];

    if (hasBit(key, depth))
      next = hashInternal(ctx, node, next);
    else
      next = hashInternal(ctx, next, node);

    depth -= 1;
  }

  if (!next.equals(root))
    return [PROOF_HASH_MISMATCH, null];

  // Two types of NX proofs.

  // Type 1: Non-existent leaf.
  if (leaf.equals(hash.zero)) {
    if (proof.key)
      return [PROOF_UNEXPECTED_NODE, null];

    if (proof.value)
      return [PROOF_UNEXPECTED_NODE, null];

    return [PROOF_OK, null];
  }

  // Type 2: Prefix collision.
  // We have to provide the full preimage
  // to prove we're a leaf, and also that
  // we are indeed a different key.
  if (proof.key) {
    if (!proof.value)
      return [PROOF_UNEXPECTED_NODE, null];

    if (proof.key.equals(key))
      return [PROOF_UNEXPECTED_NODE, null];

    const h = hashLeaf(ctx, proof.key, proof.value);

    if (!h.equals(leaf))
      return [PROOF_HASH_MISMATCH, null];

    return [PROOF_OK, null];
  }

  // Otherwise, we should have a value.
  if (!proof.value)
    return [PROOF_NO_RESULT, null];

  const h = hashLeaf(ctx, key, proof.value);

  if (!h.equals(leaf))
    return [PROOF_HASH_MISMATCH, null];

  return [PROOF_OK, proof.value];
};

/**
 * Proof
 */

class Proof {
  constructor(nodes, key, value) {
    this.nodes = [];
    this.key = null;
    this.value = null;
    this.from(nodes, key, value);
  }

  from(nodes, key, value) {
    if (nodes != null) {
      assert(Array.isArray(nodes));
      this.nodes = nodes;
    }

    if (key != null) {
      assert(Buffer.isBuffer(key));
      this.key = key;
    }

    if (value != null) {
      assert(Buffer.isBuffer(value));
      this.value = value;
    }

    return this;
  }

  getSize(hashSize, bits) {
    assert((hashSize >>> 0) === hashSize);
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    let size = 0;

    size += 1;
    size += (this.nodes.length + 7) / 8 | 0;

    const zeroHash = Buffer.alloc(hashSize, 0x00);

    for (const node of this.nodes) {
      if (!node.equals(zeroHash))
        size += node.length;
    }

    size += 2;

    if (this.key)
      size += bits >>> 3;

    if (this.value)
      size += this.value.length;

    return size;
  }

  encode(hashSize, bits) {
    assert((hashSize >>> 0) === hashSize);
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    const zeroHash = Buffer.alloc(hashSize, 0x00);
    const size = this.getSize(hashSize, bits);
    const bsize = (this.nodes.length + 7) / 8 | 0;
    const data = Buffer.alloc(size);

    let pos = 0;

    assert(this.nodes.length > 0);
    assert(this.nodes.length <= bits);

    data[pos] = this.nodes.length - 1;

    pos += 1;

    // data.fill(0x00, pos, pos + bsize);

    pos += bsize;

    for (let i = 0; i < this.nodes.length; i++) {
      const node = this.nodes[i];

      if (node.equals(zeroHash))
        setBit(data, 8 + i);
      else
        pos += node.copy(data, pos);
    }

    let field = 0;

    if (this.key)
      field |= 1 << 15;

    if (this.value) {
      // 16kb max
      assert(this.value.length < (1 << 14));
      field |= 1 << 14;
      field |= this.value.length;
    }

    data[pos] = field & 0xff;
    pos += 1;
    data[pos] |= field >>> 8;
    pos += 1;

    if (this.key)
      pos += this.key.copy(data, pos);

    if (this.value)
      pos += this.value.copy(data, pos);

    assert(pos === data.length);

    return data;
  }

  decode(data, hashSize, bits) {
    assert(Buffer.isBuffer(data));
    assert((hashSize >>> 0) === hashSize);
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    let pos = 0;

    assert(pos + 1 <= data.length);

    const count = data[pos] + 1;
    const bsize = (count + 7) / 8 | 0;

    pos += 1;
    pos += bsize;

    assert(pos <= data.length);

    const zeroHash = Buffer.alloc(hashSize, 0x00);

    for (let i = 0; i < count; i++) {
      if (hasBit(data, 8 + i)) {
        this.nodes.push(zeroHash);
      } else {
        assert(pos + hashSize <= data.length);
        const hash = data.slice(pos, pos + hashSize);
        this.nodes.push(hash);
        pos += hashSize;
      }
    }

    assert(pos + 2 <= data.length);

    let field = 0;
    field |= data[pos];
    field |= data[pos + 1] << 8;
    pos += 2;

    if (field & (1 << 15)) {
      const keySize = bits >>> 3;
      assert(pos + keySize <= data.length);
      this.key = data.slice(pos, pos + keySize);
      pos += keySize;
    }

    if (field & (1 << 14)) {
      const size = field & ((1 << 14) - 1);
      assert(pos + size <= data.length);
      this.value = data.slice(pos, pos + size);
      pos += size;
    }

    return this;
  }

  static decode(data, hashSize, bits) {
    return new this().decode(data, hashSize, bits);
  }
}

/**
 * Missing Node Error
 */

class MissingNodeError extends Error {
  /**
   * Create an error.
   * @constructor
   * @param {Object?} options
   */

  constructor(options = {}) {
    super();
    this.type = 'MissingNodeError';
    this.name = 'MissingNodeError';
    this.code = 'ERR_MISSING_NODE';
    this.rootHash = options.rootHash || null;
    this.nodeHash = options.nodeHash || null;
    this.key = options.key || null;
    this.depth = options.depth >>> 0;
    this.message = 'Missing node.';

    if (this.nodeHash)
      this.message = `Missing node: ${this.nodeHash.toString('hex')}.`;

    if (Error.captureStackTrace)
      Error.captureStackTrace(this, MissingNodeError);
  }
}

/**
 * Assertion Error
 */

class AssertionError extends assert.AssertionError {
  constructor(message) {
    super({ message });
  }
}

/*
 * Helpers
 */

function hasBit(key, index) {
  const oct = index >>> 3;
  const bit = index & 7;
  return (key[oct] >>> (7 - bit)) & 1;
}

function setBit(key, index) {
  const oct = index >>> 3;
  const bit = index & 7;
  key[oct] |= 1 << (7 - bit);
}

function hashInternal(ctx, left, right) {
  ctx.init();
  ctx.update(INTERNAL_PREFIX);
  ctx.update(left);
  ctx.update(right);
  return ctx.final();
}

function hashLeaf(ctx, key, value) {
  ctx.init();
  ctx.update(LEAF_PREFIX);
  ctx.update(key);
  ctx.update(value);
  return ctx.final();
}

function isZero(hash) {
  for (let i = 0; i < hash.length; i++) {
    if (hash[i] !== 0x00)
      return false;
  }
  return true;
}

function parseU32(name) {
  if (name.length !== 10)
    return -1;

  let num = 0;

  for (let i = 0; i < 10; i++) {
    const ch = name.charCodeAt(i);

    if (ch < 0x30 || ch > 0x39)
      return -1;

    num *= 10;
    num += ch - 0x30;
  }

  if (num > MAX_FILES)
    return -1;

  return num;
}

/*
 * Expose
 */

module.exports = Merklix;
