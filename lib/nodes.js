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

  toHash(hash) {
    assert(this.index !== 0);
    return new Hash(
      this.hash(hash),
      this.index,
      this.pos,
      this.hasLeaf()
    );
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

  hasLeaf() {
    return 0;
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

  getSize(hash, bits) {
    return Internal.getSize(hash, bits);
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
    const {left, right} = this;
    const lpos = (left.pos * 2) + left.hasLeaf();
    const lhash = left.hash(hash);

    off = writeU16(data, left.index * 2, off);
    off = writeU32(data, lpos, off);
    off += lhash.copy(data, off);

    const rpos = (right.pos * 2) + right.hasLeaf();
    const rhash = right.hash(hash);

    off = writeU16(data, right.index, off);
    off = writeU32(data, rpos, off);
    off += rhash.copy(data, off);

    return off;
  }

  decode(data, hash, bits) {
    let off = 0;
    let index;

    index = readU16(data, off);
    off += 2;

    // Sanity check.
    assert((index & 1) === 0);
    index >>>= 1;

    if (index !== 0) {
      const flags = readU32(data, off);
      off += 4;

      const pos = flags >>> 1;
      const leaf = flags & 1;

      const lhash = data.slice(off, off + hash.size);
      off += hash.size;

      this.left = new Hash(lhash, index, pos, leaf);
    } else {
      off += 4 + hash.size;
    }

    // Note: we have an extra bit
    // here we're not using for
    // anything.
    index = readU16(data, off);
    off += 2;

    if (index !== 0) {
      const flags = readU32(data, off);
      off += 4;

      const pos = flags >>> 1;
      const leaf = flags & 1;

      const rhash = data.slice(off, off + hash.size);
      off += hash.size;

      this.right = new Hash(rhash, index, pos, leaf);
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

  hasLeaf() {
    return 1;
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
    off = writeU32(data, this.vsize, off);
    off += this.key.copy(data, off);
    return off;
  }

  decode(data, hash, bits) {
    let off = 0;

    this.vindex = readU16(data, off);
    off += 2;

    // Sanity check.
    assert((this.vindex & 1) === 1);
    this.vindex >>>= 1;

    this.vpos = readU32(data, off);
    off += 4;

    this.vsize = readU32(data, off);
    off += 4;

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
    return (bits >>> 3) + 2 + 4 + 4;
  }
}

/**
 * Hash
 */

class Hash extends Node {
  constructor(data, index, pos, leaf) {
    super();
    this.data = data || null;
    this.index = index || 0;
    this.pos = pos || 0;
    this.leaf = leaf || 0;
  }

  type() {
    return HASH;
  }

  isHash() {
    return true;
  }

  hasLeaf() {
    return this.leaf;
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
