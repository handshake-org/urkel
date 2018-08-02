/*!
 * nodes.js - tree nodes
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

/* eslint no-use-before-define: "off" */

'use strict';

const assert = require('bsert');
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
  constructor(index, flags, data) {
    this.index = index;
    this.flags = flags;
    this.data = data;
  }

  get pos() {
    return this.flags >>> 1;
  }

  set pos(pos) {
    this.flags = pos * 2 + this.leaf;
  }

  get leaf() {
    return this.flags & 1;
  }

  set leaf(bit) {
    this.flags = (this.flags & ~1) >>> 0;
    this.flags += bit;
  }

  type() {
    throw new AssertionError('Unimplemented.');
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

  hash(hash) {
    return hash.zero;
  }

  toHash(hash) {
    assert(this.index !== 0);
    return new Hash(this.hash(hash), this.index, this.flags);
  }

  getSize(hash, bits) {
    throw new AssertionError('Unimplemented.');
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

  static getSize(hash, bits) {
    throw new AssertionError('Unimplemented.');
  }

  static decode(data, hash, bits) {
    throw new AssertionError('Unimplemented.');
  }
}

/**
 * Null
 */

class Null extends Node {
  constructor() {
    super(0, 0, null);
  }

  type() {
    return NULL;
  }

  isNull() {
    return true;
  }

  toHash(hash) {
    return this;
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
    super(0, 0, null);
    this.left = left;
    this.right = right;
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

  getSize(hash, bits) {
    return Internal.getSize(hash, bits);
  }

  write(data, off, hash, bits) {
    const {left, right} = this;

    off = writeU16(data, left.index * 2, off);
    off = writeU32(data, left.flags, off);
    off += left.hash(hash).copy(data, off);

    off = writeU16(data, right.index, off);
    off = writeU32(data, right.flags, off);
    off += right.hash(hash).copy(data, off);

    return off;
  }

  decode(data, hash, bits) {
    let off = 0;
    let index;

    index = readU16(data, off);
    off += 2;

    // Sanity check.
    if ((index & 1) !== 0)
      throw new AssertionError('Database corruption.');

    index >>>= 1;

    if (index !== 0) {
      const flags = readU32(data, off);
      off += 4;

      const lhash = data.slice(off, off + hash.size);
      off += hash.size;

      this.left = new Hash(lhash, index, flags);
    } else {
      off += 4 + hash.size;
    }

    // Unused bit here.
    index = readU16(data, off);
    off += 2;

    if (index !== 0) {
      const flags = readU32(data, off);
      off += 4;

      const rhash = data.slice(off, off + hash.size);
      off += hash.size;

      this.right = new Hash(rhash, index, flags);
    } else {
      off += 4 + hash.size;
    }

    return this;
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
    return (2 + 4 + hash.size) * 2;
  }

  static decode(data, hash, bits) {
    const NIL = exports.NIL;
    const node = new this(NIL, NIL);
    return node.decode(data, hash, bits);
  }
}

/**
 * Leaf
 */

class Leaf extends Node {
  constructor(data, key, value) {
    super(0, 0, data);
    this.key = key;
    this.value = value;
    this.vindex = 0;
    this.vpos = 0;
    this.vsize = 0;
  }

  get leaf() {
    return 1;
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

  getSize(hash, bits) {
    return Leaf.getSize(hash, bits);
  }

  write(data, off, hash, bits) {
    off = writeU16(data, this.vindex * 2 + 1, off);
    off = writeU32(data, this.vpos, off);
    off = writeU16(data, this.vsize, off);
    off += this.key.copy(data, off);
    return off;
  }

  decode(data, hash, bits) {
    let off = 0;

    this.vindex = readU16(data, off);
    off += 2;

    // Sanity check.
    if ((this.vindex & 1) !== 1)
      throw new AssertionError('Database corruption.');

    this.vindex >>>= 1;

    // Unused bit here.
    this.vpos = readU32(data, off);
    off += 4;

    this.vsize = readU16(data, off);
    off += 2;

    this.key = data.slice(off, off + (bits >>> 3));
    off += bits >>> 3;

    return this;
  }

  inspect() {
    return `<Leaf: ${this.key.toString('hex')}>`;
  }

  static getSize(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    return 2 + 4 + 2 + (bits >>> 3);
  }

  static decode(data, hash, bits) {
    const node = new this(null, null, null);
    return node.decode(data, hash, bits);
  }
}

/**
 * Hash
 */

class Hash extends Node {
  constructor(data, index, flags) {
    super(index, flags, data);
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

  toHash(hash) {
    assert(this.data);
    return this;
  }

  inspect() {
    return `<Hash: ${this.data.toString('hex')}>`;
  }
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
