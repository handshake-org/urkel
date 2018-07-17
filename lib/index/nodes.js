/*!
 * nodes.js - tree nodes
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

/* eslint no-use-before-define: "off" */

'use strict';

const assert = require('assert');
const common = require('../common');
const {AssertionError} = require('../errors');
const {readU32, writeU32} = common;

/*
 * Constants
 */

const NULL = 0;
const INTERNAL = 1;
const LEAF = 2;
const POSITION = 3;

const types = {
  NULL,
  INTERNAL,
  LEAF,
  POSITION
};

const INTERNAL_SIZE = 17;
const LEAF_SIZE = 1 + (1 + 64) + 9;

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

  getSize() {
    throw new AssertionError('Unimplemented.');
  }

  write(data, off) {
    throw new AssertionError('Unimplemented.');
  }

  encode() {
    const size = this.getSize();
    const data = Buffer.allocUnsafe(size);
    this.write(data, 0);
    return data;
  }

  decode(data) {
    throw new AssertionError('Unimplemented.');
  }

  toPosition() {
    assert(this.index !== 0);
    return new Position(this.index, this.pos, this.hasLeaf());
  }

  static decode(data) {
    return new this().decode(data);
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

  isPosition() {
    return false;
  }

  hasLeaf() {
    return 0;
  }

  static getSize() {
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

  toPosition() {
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
    this.left = left || exports.NIL;
    this.right = right || exports.NIL;
  }

  type() {
    return INTERNAL;
  }

  isInternal() {
    return true;
  }

  getSize() {
    return INTERNAL_SIZE;
  }

  write(data, off) {
    data[off] = INTERNAL;
    off += 1;

    const leftPos = (this.left.pos * 2) + this.left.hasLeaf();

    off = writeU32(data, this.left.index, off);
    off = writeU32(data, leftPos, off);

    const rightPos = (this.right.pos * 2) + this.right.hasLeaf();

    off = writeU32(data, this.right.index, off);
    off = writeU32(data, rightPos, off);

    return off;
  }

  decode(data) {
    assert(data.length === INTERNAL_SIZE);
    assert(data[0] === INTERNAL);

    let off = 1;
    let index;

    index = readU32(data, off);
    off += 4;

    if (index !== 0) {
      const flags = readU32(data, off);
      const pos = flags >>> 1;
      const leaf = flags & 1;
      off += 4;
      this.left = new Position(index, pos, leaf);
    } else {
      off += 4;
    }

    index = readU32(data, off);
    off += 4;

    if (index !== 0) {
      const flags = readU32(data, off);
      const pos = flags >>> 1;
      const leaf = flags & 1;
      off += 4;
      this.right = new Position(index, pos, leaf);
    } else {
      off += 4;
    }

    return this;
  }

  inspect() {
    return {
      left: this.left,
      right: this.right
    };
  }
}

/**
 * Leaf
 */

class Leaf extends Node {
  constructor(key, value) {
    super();

    this.key = key || null;
    this.value = value || null;
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

  getSize() {
    return LEAF_SIZE;
  }

  write(data, off) {
    assert(this.key && this.key.length <= 64);
    assert(this.value && this.value.length === 9);

    const left = 64 - this.key.length;

    data[off] = LEAF;
    off += 1;

    data[off] = this.key.length;
    off += 1;

    off += this.key.copy(data, off);

    data.fill(0x00, off, off + left);
    off += left;

    off += this.value.copy(data, off);

    return off;
  }

  decode(data) {
    assert(data.length === LEAF_SIZE);
    assert(data[0] === LEAF);

    let off = 1;

    const size = data[off];
    off += 1;

    this.key = data.slice(off, off + size);
    off += 64;

    this.value = data.slice(off, off + 9);
    off += 9;

    return this;
  }

  inspect() {
    return `<Leaf: ${this.key.toString('hex')}>`;
  }
}

/**
 * Position
 */

class Position extends Node {
  constructor(index, pos, leaf) {
    super();
    this.index = index || 0;
    this.pos = pos || 0;
    this.leaf = leaf || 0;
  }

  type() {
    return POSITION;
  }

  isPosition() {
    return true;
  }

  hasLeaf() {
    return this.leaf;
  }

  toPosition() {
    return this;
  }

  inspect() {
    return `<Position: ${this.index}:${this.pos}>`;
  }
}

/*
 * Helpers
 */

function decodeNode(data, index, pos) {
  let node;

  assert(data.length > 0);

  switch (data[0]) {
    case INTERNAL:
      node = Internal.decode(data);
      break;
    case LEAF:
      node = Leaf.decode(data);
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

exports.INTERNAL_SIZE = INTERNAL_SIZE;
exports.LEAF_SIZE = LEAF_SIZE;
exports.types = types;
exports.Node = Node;
exports.Null = Null;
exports.Internal = Internal;
exports.Leaf = Leaf;
exports.Position = Position;
exports.NIL = new Null();
exports.decodeNode = decodeNode;
