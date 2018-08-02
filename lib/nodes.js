/*!
 * nodes.js - tree nodes
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

/* eslint no-use-before-define: "off" */

'use strict';

const assert = require('bsert');
const Bits = require('./bits');
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

const PTR_SIZE = 7;

/**
 * Pointer
 */

class Pointer {
  constructor() {
    this.flags = 0;
    this.pos = 0;
  }

  set index(index) {
    this.flags = index * 0x400 + this.size;
  }

  get index() {
    return this.flags >>> 10;
  }

  set size(size) {
    this.flags = this.index * 0x400 + size;
  }

  get size() {
    return this.flags & 0x3ff;
  }

  clone() {
    const copy = new this.constructor();
    copy.inject(this);
    return copy;
  }

  inject(ptr) {
    this.flags = ptr.flags;
    this.pos = ptr.pos;
    return this;
  }

  getSize() {
    return this.constructor.getSize();
  }

  write(data, off) {
    const size = this.size;
    const bits = size >>> 8;
    const hi = bits >>> 1;
    const lo = bits & 1;

    off = writeU16(data, this.index * 2 + hi, off);
    off = writeU32(data, this.pos * 2 + lo, off);
    data[off] = size;
    off += 1;

    return off;
  }

  read(data, off) {
    const index = readU16(data, off);
    const pos = readU32(data, off + 2);
    const size = data[off + 6];
    const hi = index & 1;
    const lo = pos & 1;

    this.index = index >>> 1;
    this.pos = pos >>> 1;
    this.size = size + hi * 0x200 + lo * 0x100;

    return this;
  }

  encode() {
    const size = this.getSize();
    const data = Buffer.allocUnsafe(size);
    this.write(data, 0);
    return data;
  }

  decode(data) {
    return this.read(data, 0);
  }

  inspect() {
    return {
      index: this.index,
      pos: this.pos,
      size: this.size
    };
  }

  static getSize() {
    return PTR_SIZE;
  }

  static read(data, off) {
    return new this().read(data, off);
  }

  static decode(data) {
    return new this().decode(data);
  }

  static from(index, pos, size) {
    const ptr = new this();
    ptr.index = index;
    ptr.pos = pos;
    ptr.size = size;
    return ptr;
  }
}

/**
 * Node
 */

class Node {
  constructor(ptr, data) {
    this.ptr = ptr;
    this.data = data;
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

  mark(index, pos, size) {
    this.ptr = Pointer.from(index, pos, size);
    return this;
  }

  toHash(hash) {
    assert(this.ptr);
    return new Hash(this.hash(hash), this.ptr);
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

  static decode(data, hash, bits) {
    throw new AssertionError('Unimplemented.');
  }
}

/**
 * Null
 */

class Null extends Node {
  constructor() {
    super(new Pointer(), null);
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
    super(null, null);
    this.left = left;
    this.right = right;
  }

  type() {
    return INTERNAL;
  }

  isInternal() {
    return true;
  }

  get(bit) {
    switch (bit) {
      case 0:
        return this.left;
      case 1:
        return this.right;
      default:
        throw new AssertionError('Invalid bit.');
    }
  }

  set(bit, node) {
    switch (bit) {
      case 0:
        this.left = node;
        break;
      case 1:
        this.right = node;
        break;
      default:
        throw new AssertionError('Invalid bit.');
    }
  }

  hash(hash) {
    if (!this.data) {
      const left = this.left.hash(hash);
      const right = this.right.hash(hash);

      this.data = hashInternal(hash, left, right);
    }

    return this.data;
  }

  flags() {
    let flags = 0;

    if (!this.left.isNull())
      flags += 1;

    if (!this.right.isNull())
      flags += 2;

    return flags;
  }

  getSize(hash, bits) {
    let size = 1;

    if (!this.left.isNull()) {
      size += PTR_SIZE;
      size += hash.size;
    }

    if (!this.right.isNull()) {
      size += PTR_SIZE;
      size += hash.size;
    }

    return size;
  }

  write(data, off, hash, bits) {
    data[off] = this.flags() * 16 + INTERNAL;
    off += 1;

    if (!this.left.isNull()) {
      assert(this.left.ptr);
      off = this.left.ptr.write(data, off);
      off += this.left.hash(hash).copy(data, off);
    }

    if (!this.right.isNull()) {
      assert(this.right.ptr);
      off = this.right.ptr.write(data, off);
      off += this.right.hash(hash).copy(data, off);
    }

    return off;
  }

  decode(data, hash, bits) {
    let off = 0;

    const type = data[off];
    const flags = type >>> 4;
    off += 1;

    assert((type & 15) === INTERNAL);

    if (flags & 1) {
      const ptr = Pointer.read(data, off);
      off += PTR_SIZE;

      const buf = data.slice(off, off + hash.size);
      off += hash.size;

      this.left = new Hash(buf, ptr);
    }

    if (flags & 2) {
      const ptr = Pointer.read(data, off);
      off += PTR_SIZE;

      const buf = data.slice(off, off + hash.size);
      off += hash.size;

      this.right = new Hash(buf, ptr);
    }

    return this;
  }

  inspect() {
    return {
      left: this.left,
      right: this.right
    };
  }

  static decode(data, hash, bits) {
    const NIL = exports.NIL;
    const node = new this(NIL, NIL);
    return node.decode(data, hash, bits);
  }

  static from(x, y, bit) {
    const NIL = exports.NIL;
    const node = new this(NIL, NIL);
    node.set(bit, x);
    node.set(bit ^ 1, y);
    return node;
  }
}

/**
 * Leaf
 */

class Leaf extends Node {
  constructor(data, key, value) {
    super(null, data);
    this.key = key;
    this.value = value;
    this.vptr = null;
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

  get bits() {
    return Bits.from(this.key);
  }

  save(index, pos, size) {
    this.vptr = Pointer.from(index, pos, size);
    return this;
  }

  getSize(hash, bits) {
    return 1 + PTR_SIZE + (bits >>> 3);
  }

  write(data, off, hash, bits) {
    assert(this.vptr);
    data[off] = LEAF;
    off += 1;
    off = this.vptr.write(data, off);
    off += this.key.copy(data, off);
    return off;
  }

  decode(data, hash, bits) {
    let off = 0;

    assert(data[off] === LEAF);
    off += 1;

    this.vptr = Pointer.read(data, off);
    off += PTR_SIZE;

    this.key = data.slice(off, off + (bits >>> 3));
    off += bits >>> 3;

    return this;
  }

  inspect() {
    return `<Leaf: ${this.key.toString('hex')}>`;
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
  constructor(data, ptr) {
    super(ptr, data);
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
 * Helpers
 */

function decodeNode(data, hash, bits) {
  switch (data[0] & 15) {
    case INTERNAL:
      return Internal.decode(data, hash, bits);
    case LEAF:
      return Leaf.decode(data, hash, bits);
    default:
      throw new AssertionError('Invalid node type.');
  }
}

/*
 * Expose
 */

exports.types = types;
exports.typesByVal = typesByVal;
exports.Pointer = Pointer;
exports.Node = Node;
exports.Null = Null;
exports.Internal = Internal;
exports.Leaf = Leaf;
exports.Hash = Hash;
exports.NIL = new Null();
exports.decodeNode = decodeNode;
