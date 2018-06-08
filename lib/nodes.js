/*!
 * nodes.js - tree nodes
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

/* eslint no-use-before-define: "off" */

'use strict';

const assert = require('assert');
const common = require('./common');
const {AssertionError} = require('./errors');

const {
  hashInternal,
  readU16,
  readU32,
  writeU16,
  writeU32
} = common;

/*
 * Constants
 */

const NULL = 0;
const INTERNAL = 1;
const LEAF = 2;
const HASH = 3;

const types = {
  NULL,
  INTERNAL,
  LEAF,
  HASH
};

const typesByVal = [
  'NULL',
  'INTERNAL',
  'LEAF',
  'HASH'
];

/**
 * Node
 */

class Node {
  constructor() {
    this.index = 0;
    this.pos = 0;
  }

  type() {
    return -1;
  }

  hash(hash) {
    return hash.zero;
  }

  getSize(hash, bits) {
    return Internal.getSize(hash, bits);
  }

  write(data, off, hash, bits) {
    throw new AssertionError('Unimplemented.');
  }

  encode(hash, bits) {
    const size = this.getSize(hash, bits);
    const data = Buffer.allocUnsafe(size);
    this.write(data, 0, hash, bits);
    return data;
  }

  decode(data, hash, bits) {
    throw new AssertionError('Unimplemented.');
  }

  toHash(hash) {
    return new Hash(this.hash(hash), this.index, this.pos);
  }

  async getLeft(store) {
    throw new AssertionError('Unimplemented.');
  }

  async getRight(store) {
    throw new AssertionError('Unimplemented.');
  }

  async getValue(store) {
    throw new AssertionError('Unimplemented.');
  }

  async resolve(store) {
    return this;
  }

  static decode(data, hash, bits) {
    return new this().decode(data, hash, bits);
  }

  isNull() {
    return false;
  }

  isInternal() {
    return false;
  }

  isLeaf() {
    return false;
  }

  isHash() {
    return false;
  }

  static getSize(hash, bits) {
    throw new AssertionError('Unimplemented.');
  }
}

/**
 * Null
 */

class Null extends Node {
  constructor() {
    super();
  }

  type() {
    return NULL;
  }

  isNull() {
    return true;
  }

  inspect() {
    return '<NIL>';
  }
}

/**
 * Internal
 */

class Internal extends Node {
  constructor(left, right) {
    super();

    // Not serialized.
    this.data = null;

    this.left = left || exports.NIL;
    this.right = right || exports.NIL;
  }

  type() {
    return INTERNAL;
  }

  isInternal() {
    return true;
  }

  hash(hash) {
    if (!this.data) {
      const left = this.left.hash(hash);
      const right = this.right.hash(hash);

      this.data = hashInternal(hash, left, right);
    }

    return this.data;
  }

  write(data, off, hash, bits) {
    const left = this.left.hash(hash);
    const right = this.right.hash(hash);

    data[off] = INTERNAL;
    off += 1;

    off += left.copy(data, off);
    off = writeU16(data, this.left.index, off);
    off = writeU32(data, this.left.pos, off);

    off += right.copy(data, off);
    off = writeU16(data, this.right.index, off);
    off = writeU32(data, this.right.pos, off);

    return off;
  }

  decode(data, hash, bits) {
    const nodeSize = Internal.getSize(hash, bits);

    assert(data.length === nodeSize);
    assert(data[0] === INTERNAL);

    let off = 1;

    const left = data.slice(off, off + hash.size);
    off += hash.size;

    if (!left.equals(hash.zero)) {
      const leftIndex = readU16(data, off);
      off += 2;

      const leftPos = readU32(data, off);
      off += 4;

      this.left = new Hash(left, leftIndex, leftPos);
    } else {
      off += 2 + 4;
    }

    const right = data.slice(off, off + hash.size);
    off += hash.size;

    if (!right.equals(hash.zero)) {
      const rightIndex = readU16(data, off);
      off += 2;

      const rightPos = readU32(data, off);
      off += 4;

      this.right = new Hash(right, rightIndex, rightPos);
    } else {
      off += 2 + 4;
    }

    return this;
  }

  async getLeft(store) {
    if (this.left.isHash())
      this.left = await this.left.resolve(store);

    return this.left;
  }

  getLeftSync(store) {
    if (this.left.isHash())
      this.left = this.left.resolveSync(store);

    return this.left;
  }

  async getRight(store) {
    if (this.right.isHash())
      this.right = await this.right.resolve(store);

    return this.right;
  }

  getRightSync(store) {
    if (this.right.isHash())
      this.right = this.right.resolveSync(store);

    return this.right;
  }

  inspect() {
    return {
      left: this.left,
      right: this.right
    };
  }

  static getSize(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    return 1 + (hash.size + 2 + 4) * 2;
  }
}

/**
 * Leaf
 */

class Leaf extends Node {
  constructor(leaf, key, value) {
    super();

    this.data = leaf || null; // Not serialized.
    this.key = key || null;

    this.value = value || null; // Not serialized.
    this.vindex = 0;
    this.vpos = 0;
    this.vsize = 0;
  }

  type() {
    return LEAF;
  }

  isLeaf() {
    return true;
  }

  hash(hash) {
    assert(this.data);
    return this.data;
  }

  write(data, off, hash, bits) {
    const leafSize = Leaf.getSize(hash, bits);
    const nodeSize = Internal.getSize(hash, bits);
    const left = nodeSize - leafSize;

    data[off] = LEAF;
    off += 1;

    off += this.key.copy(data, off);

    off = writeU16(data, this.vindex, off);
    off = writeU32(data, this.vpos, off);
    off = writeU32(data, this.vsize, off);

    data.fill(0x00, off, off + left);
    off += left;

    return off;
  }

  decode(data, hash, bits) {
    const nodeSize = Internal.getSize(hash, bits);

    assert(data.length === nodeSize);
    assert(data[0] === LEAF);

    let off = 1;

    this.key = data.slice(off, off + (bits >>> 3));
    off += bits >>> 3;

    this.vindex = readU16(data, off);
    off += 2;

    this.vpos = readU32(data, off);
    off += 4;

    this.vsize = readU32(data, off);
    off += 4;

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

  static getSize(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    return 1 + (bits >>> 3) + 2 + 4 + 4;
  }
}

/**
 * Hash
 */

class Hash extends Node {
  constructor(data, index, pos) {
    super();
    this.data = data || null;
    this.index = index || 0;
    this.pos = pos || 0;
  }

  type() {
    return HASH;
  }

  isHash() {
    return true;
  }

  hash(hash) {
    assert(this.data);
    return this.data;
  }

  async resolve(store) {
    const node = await store.readNode(this.index, this.pos);
    node.data = this.data;
    return node;
  }

  resolveSync(store) {
    const node = store.readNodeSync(this.index, this.pos);
    node.data = this.data;
    return node;
  }

  inspect() {
    return `<Hash: ${this.data.toString('hex')}>`;
  }
}

/*
 * Helpers
 */

function decodeNode(data, hash, bits, index, pos) {
  let node;

  assert(data.length > 0);

  switch (data[0]) {
    case NULL:
      return exports.NIL;
    case INTERNAL:
      node = Internal.decode(data, hash, bits);
      break;
    case LEAF:
      node = Leaf.decode(data, hash, bits);
      break;
    default:
      throw new AssertionError('Database corruption.');
  }

  node.index = index;
  node.pos = pos;

  return node;
}

/*
 * Expose
 */

exports.types = types;
exports.typesByVal = typesByVal;
exports.Node = Node;
exports.Null = Null;
exports.Internal = Internal;
exports.Leaf = Leaf;
exports.Hash = Hash;
exports.NIL = new Null();
exports.decodeNode = decodeNode;
