/*!
 * nodes.js - patricia merkle trie nodes
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
const bio = require('bufio');
const common = require('./common');
const {encoding} = bio;

/*
 * Constants
 */

const {
  EMPTY,
  compressSize,
  compress,
  decompress
} = common;

const NULLNODE = 0;
const HASHNODE = 1;
const SHORTNODE = 2;
const FULLNODE = 3;
const VALUENODE = 4;

const types = {
  NULLNODE,
  HASHNODE,
  SHORTNODE,
  FULLNODE,
  VALUENODE
};

const typesByVal = [
  'NULLNODE',
  'HASHNODE',
  'SHORTNODE',
  'FULLNODE',
  'VALUENODE'
];

/**
 * Node Flags
 */

class NodeFlags {
  constructor(gen, dirty) {
    this.hash = null;
    this.gen = gen || 0;
    this.dirty = dirty || false;
  }

  clone() {
    const flags = new NodeFlags(this.gen, this.dirty);
    flags.hash = this.hash;
    return flags;
  }

  canUnload(cacheGen, cacheLimit) {
    return !this.dirty && cacheGen - this.gen >= cacheLimit;
  }

  inspect() {
    return {
      hash: this.hash,
      gen: this.gen,
      dirty: this.dirty
    };
  }
}

/**
 * Node
 * @abstract
 */

class Node {
  constructor(type) {
    assert(this instanceof Node);
    this.type = type;
  }

  getType() {
    assert((this.type & 0xff) === this.type);
    assert(this.type < typesByVal.length);
    return typesByVal[this.type];
  }

  canUnload() {
    return false;
  }

  cache() {
    return [null, true];
  }

  hash() {
    assert(false, 'Abstract method.');
  }

  isNull() {
    return this.type === NULLNODE;
  }

  isHash() {
    return this.type === HASHNODE;
  }

  isShort() {
    return this.type === SHORTNODE;
  }

  isFull() {
    return this.type === FULLNODE;
  }

  isValue() {
    return this.type === VALUENODE;
  }

  clone() {
    assert(false, 'Abstract method.');
  }

  size(hash) {
    return 1;
  }

  encode(hash) {
    const size = this.size(hash);
    const bw = bio.write(size);
    this.write(bw, hash);
    return bw.render();
  }

  write(bw, hash) {
    assert(false, 'Abstract method.');
  }

  static decode(data, hash) {
    return this.read(bio.read(data), hash);
  }

  static read(br, hash) {
    assert(false, 'Abstract method.');
  }
}

/**
 * Null Node
 * @extends Node
 */

class NullNode extends Node {
  constructor() {
    super(NULLNODE);
  }

  hash() {
    throw new Error('Cannot hash NULLNODE.');
  }

  clone() {
    return this;
  }

  write(bw, hash) {
    bw.writeU8(this.type);
    return bw;
  }

  static read(br, hash) {
    const n = exports.NIL;
    assert(br.readU8() === NULLNODE);
    return n;
  }

  inspect() {
    return '<NIL>';
  }
}

/**
 * Hash Node
 * @extends Node
 */

class HashNode extends Node {
  constructor(key, hash) {
    super(HASHNODE);

    this.data = key || hash.zero;
  }

  hash() {
    return this.data;
  }

  clone() {
    return new HashNode(this.data);
  }

  size(hash) {
    return 1 + hash.size;
  }

  write(bw, hash) {
    bw.writeU8(this.type);
    bw.writeBytes(this.data);
    return bw;
  }

  static read(br, hash) {
    const n = new HashNode(null, hash);
    assert(br.readU8() === HASHNODE);
    n.data = br.readBytes(hash.size);
    return n;
  }

  inspect() {
    return `<HashNode: ${this.data.toString('hex')}>`;
  }
}

/**
 * Short Node
 * @extends Node
 */

class ShortNode extends Node {
  constructor(key, value, flags) {
    super(SHORTNODE);

    this.key = key || EMPTY;
    this.value = value || exports.NIL;
    this.flags = flags || new NodeFlags();
    this.id = null;
  }

  canUnload(gen, limit) {
    return this.flags.canUnload(gen, limit);
  }

  cache() {
    return [this.flags.hash, this.flags.dirty];
  }

  hash() {
    if (this.flags.hash)
      return this.flags.hash.data;

    return null;
  }

  clone() {
    return new ShortNode(this.key, this.value, this.flags.clone());
  }

  size(hash) {
    const len = compressSize(this.key);
    return 1 + encoding.sizeVarlen(len) + this.value.size(hash);
  }

  write(bw, hash) {
    bw.writeU8(this.type);
    bw.writeVarBytes(compress(this.key));
    this.value.write(bw);
    return bw;
  }

  static read(br, hash) {
    const n = new ShortNode();

    assert(br.readU8() === SHORTNODE);

    n.key = decompress(br.readVarBytes(true));
    n.value = readNode(br, hash);

    if (br.data.length >= hash.size)
      n.id = hash.digest(br.data);

    return n;
  }

  inspect() {
    return {
      type: this.getType(),
      key: this.key.toString('hex'),
      value: this.value,
      flags: this.flags
    };
  }
}

/**
 * Full Node
 * @extends Node
 */

class FullNode extends Node {
  constructor(flags) {
    super(FULLNODE);

    const children = new Array(17);

    for (let i = 0; i < 17; i++)
      children[i] = exports.NIL;

    this.children = children;
    this.flags = flags || new NodeFlags();
    this.id = null;
  }

  canUnload(gen, limit) {
    return this.flags.canUnload(gen, limit);
  }

  cache() {
    return [this.flags.hash, this.flags.dirty];
  }

  hash() {
    if (this.flags.hash)
      return this.flags.hash.data;

    return null;
  }

  clone() {
    const n = new FullNode(this.flags.clone());
    n.children = this.children.slice();
    return n;
  }

  size(hash) {
    let size = 1;

    for (const n of this.children)
      size += n.size(hash);

    return size;
  }

  write(bw, hash) {
    bw.writeU8(this.type);

    for (const n of this.children)
      n.write(bw, hash);

    return bw;
  }

  static read(br, hash) {
    const n = new FullNode();

    assert(br.readU8() === FULLNODE);

    for (let i = 0; i < 17; i++)
      n.children[i] = readNode(br, hash);

    if (br.data.length >= hash.size)
      n.id = hash.digest(br.data);

    return n;
  }

  inspect() {
    return {
      type: this.getType(),
      children: this.children,
      flags: this.flags
    };
  }
}

/**
 * Value Node
 * @extends Node
 */

class ValueNode extends Node {
  constructor(data, hash) {
    super(VALUENODE);

    this.data = data || EMPTY;
    this.id = null;
  }

  hash() {
    return null;
  }

  clone() {
    return new ValueNode(this.data);
  }

  size(hash) {
    return 1 + encoding.sizeVarlen(this.data.length);
  }

  write(bw, hash) {
    bw.writeU8(this.type);
    bw.writeVarBytes(this.data);
    return bw;
  }

  static read(br, hash) {
    const n = new ValueNode(null);

    assert(br.readU8() === VALUENODE);

    n.data = br.readVarBytes();

    if (br.data.length >= hash.size)
      n.id = hash.digest(br.data);

    return n;
  }

  inspect() {
    let str;

    if (this.data.length > 64)
      str = this.data.toString('hex', 0, 64) + '...';
    else
      str = this.data.toString('hex');

    return `<ValueNode: ${str}>`;
  }
}

/*
 * Helpers
 */

function decodeNode(data, hash) {
  assert(data.length > 0);

  switch (data[0]) {
    case NULLNODE:
      return NullNode.decode(data, hash);
    case HASHNODE:
      return HashNode.decode(data, hash);
    case SHORTNODE:
      return ShortNode.decode(data, hash);
    case FULLNODE:
      return FullNode.decode(data, hash);
    case VALUENODE:
      return ValueNode.decode(data, hash);
    default:
      throw new Error('Invalid node type.');
  }
}

function readNode(br, hash) {
  const type = br.readU8();
  br.seek(-1);
  switch (type) {
    case NULLNODE:
      return NullNode.read(br, hash);
    case HASHNODE:
      return HashNode.read(br, hash);
    case SHORTNODE:
      return ShortNode.read(br, hash);
    case FULLNODE:
      return FullNode.read(br, hash);
    case VALUENODE:
      return ValueNode.read(br, hash);
    default:
      throw new Error('Invalid node type.');
  }
}

/*
 * Expose
 */

exports.types = types;
exports.typesByVal = typesByVal;
exports.NodeFlags = NodeFlags;
exports.Node = Node;
exports.NullNode = NullNode;
exports.HashNode = HashNode;
exports.ShortNode = ShortNode;
exports.FullNode = FullNode;
exports.ValueNode = ValueNode;
exports.NIL = new NullNode();
exports.decodeNode = decodeNode;
exports.readNode = readNode;
