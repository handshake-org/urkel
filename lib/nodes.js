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

const PTR_SIZE = 6;
const NODE_PTR_SIZE = PTR_SIZE;
const VALUE_PTR_SIZE = PTR_SIZE + 2;

/**
 * Pointer
 */

class Pointer {
  constructor() {
    this.index = 0;
    this.pos = 0;
  }

  clone() {
    const copy = new this.constructor();
    copy.inject(this);
    return copy;
  }

  inject(ptr) {
    this.index = ptr.index;
    this.pos = ptr.pos;
    return this;
  }

  getSize() {
    return this.constructor.getSize();
  }

  write(data, off) {
    off = writeU16(data, this.index, off);
    off = writeU32(data, this.pos, off);
    return off;
  }

  read(data, off) {
    this.index = readU16(data, off);
    this.pos = readU32(data, off + 2);
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
      pos: this.pos
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

  static from(index, pos) {
    const ptr = new this();
    ptr.index = index;
    ptr.pos = pos;
    return ptr;
  }
}

/**
 * NodePointer
 */

class NodePointer extends Pointer {
  constructor() {
    super();
    this.type = 0;
  }

  size(store) {
    switch (this.type) {
      case INTERNAL:
        return store.internalSize;
      case LEAF:
        return store.leafSize;
    }
    throw new AssertionError('Bad node type.');
  }

  ctor() {
    switch (this.type) {
      case INTERNAL:
        return Internal;
      case LEAF:
        return Leaf;
    }
    throw new AssertionError('Bad node type.');
  }

  inject(ptr) {
    this.type = ptr.type;
    this.index = ptr.index;
    this.pos = ptr.pos;
    return this;
  }

  write(data, off) {
    const hi = this.type >>> 1;
    const lo = this.type & 1;

    off = writeU16(data, this.index * 2 + hi, off);
    off = writeU32(data, this.pos * 2 + lo, off);

    return off;
  }

  read(data, off) {
    const index = readU16(data, off);
    const pos = readU32(data, off + 2);
    const hi = index & 1;
    const lo = pos & 1;

    this.type = hi * 2 + lo;
    this.index = index >>> 1;
    this.pos = pos >>> 1;

    return this;
  }

  inspect() {
    return {
      type: this.type,
      index: this.index,
      pos: this.pos
    };
  }

  static getSize() {
    return NODE_PTR_SIZE;
  }

  static from(type, index, pos) {
    const ptr = new this();
    ptr.type = type;
    ptr.index = index;
    ptr.pos = pos;
    return ptr;
  }
}

/**
 * ValuePointer
 */

class ValuePointer extends Pointer {
  constructor() {
    super();
    this.size = 0;
  }

  inject(ptr) {
    this.index = ptr.index;
    this.pos = ptr.pos;
    this.size = ptr.size;
    return this;
  }

  write(data, off) {
    off = writeU16(data, this.index, off);
    off = writeU32(data, this.pos, off);
    off = writeU16(data, this.size, off);
    return off;
  }

  read(data, off) {
    this.index = readU16(data, off);
    this.pos = readU32(data, off + 2);
    this.size = readU16(data, off + 6);
    return this;
  }

  inspect() {
    return {
      index: this.index,
      pos: this.pos,
      size: this.size
    };
  }

  static getSize() {
    return VALUE_PTR_SIZE;
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

  get leaf() {
    return this.isLeaf();
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

  set(index, pos) {
    this.ptr = NodePointer.from(this.type(), index, pos);
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
    super(null, null);
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
    if (this.left.ptr) {
      off = this.left.ptr.write(data, off);
    } else {
      data.fill(0x00, off, off + NODE_PTR_SIZE);
      off += NODE_PTR_SIZE;
    }

    off += this.left.hash(hash).copy(data, off);

    if (this.right.ptr) {
      off = this.right.ptr.write(data, off);
    } else {
      data.fill(0x00, off, off + NODE_PTR_SIZE);
      off += NODE_PTR_SIZE;
    }

    off += this.right.hash(hash).copy(data, off);

    return off;
  }

  decode(data, hash, bits) {
    let off = 0;

    const lptr = NodePointer.read(data, off);
    off += NODE_PTR_SIZE;

    if (lptr.index !== 0) {
      const lhash = data.slice(off, off + hash.size);
      off += hash.size;
      this.left = new Hash(lhash, lptr);
    } else {
      off += hash.size;
    }

    const rptr = NodePointer.read(data, off);
    off += NODE_PTR_SIZE;

    if (rptr.index !== 0) {
      const rhash = data.slice(off, off + hash.size);
      off += hash.size;
      this.right = new Hash(rhash, rptr);
    } else {
      off += hash.size;
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
    return (NODE_PTR_SIZE + hash.size) * 2;
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

  save(index, pos, size) {
    this.vptr = ValuePointer.from(index, pos, size);
    return this;
  }

  getSize(hash, bits) {
    return Leaf.getSize(hash, bits);
  }

  write(data, off, hash, bits) {
    assert(this.vptr);
    off = this.vptr.write(data, off);
    off += this.key.copy(data, off);
    return off;
  }

  decode(data, hash, bits) {
    let off = 0;

    this.vptr = ValuePointer.read(data, off);
    off += VALUE_PTR_SIZE;

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
    return VALUE_PTR_SIZE + (bits >>> 3);
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

  get leaf() {
    return this.ptr.type === LEAF;
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
exports.Pointer = Pointer;
exports.NodePointer = NodePointer;
exports.ValuePointer = ValuePointer;
exports.Node = Node;
exports.Null = Null;
exports.Internal = Internal;
exports.Leaf = Leaf;
exports.Hash = Hash;
exports.NIL = new Null();
