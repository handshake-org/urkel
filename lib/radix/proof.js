/*!
 * proof.js - tree proofs
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('bsert');
const Bits = require('./bits');
const common = require('./common');
const errors = require('./errors');

const {
  EMPTY,
  hasBit,
  setBit,
  hashInternal,
  hashLeaf,
  hashValue,
  readU16,
  writeU16
} = common;

const {
  AssertionError,
  EncodingError
} = errors;

/*
 * Constants
 */

const TYPE_DEADEND = 0;
const TYPE_SHORT = 1;
const TYPE_COLLISION = 2;
const TYPE_EXISTS = 3;
const TYPE_UNKNOWN = 4;

const PROOF_OK = 0;
const PROOF_HASH_MISMATCH = 1;
const PROOF_SAME_KEY = 2;
const PROOF_SAME_PATH = 3;
const PROOF_NEG_DEPTH = 4;
const PROOF_PATH_MISMATCH = 5;
const PROOF_TOO_DEEP = 6;
const PROOF_UNKNOWN_ERROR = 7;

/**
 * Proof types.
 * @enum {Number}
 */

const types = {
  TYPE_DEADEND,
  TYPE_SHORT,
  TYPE_COLLISION,
  TYPE_EXISTS,
  TYPE_UNKNOWN
};

/**
 * Proof types (strings).
 * @const {String[]}
 * @default
 */

const typesByVal = [
  'TYPE_DEADEND',
  'TYPE_SHORT',
  'TYPE_COLLISION',
  'TYPE_EXISTS',
  'TYPE_UNKNOWN'
];

/**
 * Verification error codes.
 * @enum {Number}
 */

const codes = {
  PROOF_OK,
  PROOF_HASH_MISMATCH,
  PROOF_SAME_KEY,
  PROOF_SAME_PATH,
  PROOF_NEG_DEPTH,
  PROOF_PATH_MISMATCH,
  PROOF_TOO_DEEP,
  PROOF_UNKNOWN_ERROR
};

/**
 * Verification error codes (strings).
 * @const {String[]}
 * @default
 */

const codesByVal = [
  'PROOF_OK',
  'PROOF_HASH_MISMATCH',
  'PROOF_SAME_KEY',
  'PROOF_SAME_PATH',
  'PROOF_NEG_DEPTH',
  'PROOF_PATH_MISMATCH',
  'PROOF_TOO_DEEP',
  'PROOF_UNKNOWN_ERROR'
];

/**
 * Proof
 */

class Proof {
  constructor() {
    this.type = TYPE_DEADEND;
    this.depth = 0;
    this.nodes = [];
    this.prefix = null;
    this.left = null;
    this.right = null;
    this.key = null;
    this.hash = null;
    this.value = null;
  }

  isSane(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    if (this.depth > bits)
      return false;

    if (this.nodes.length > bits)
      return false;

    for (const {prefix} of this.nodes) {
      if (prefix.size > bits)
        return false;
    }

    switch (this.type) {
      case TYPE_DEADEND:
        if (this.prefix)
          return false;

        if (this.left)
          return false;

        if (this.right)
          return false;

        if (this.key)
          return false;

        if (this.hash)
          return false;

        if (this.value)
          return false;

        break;
      case TYPE_SHORT:
        if (!this.prefix)
          return false;

        if (!this.left)
          return false;

        if (!this.right)
          return false;

        if (this.key)
          return false;

        if (this.hash)
          return false;

        if (this.value)
          return false;

        if (this.prefix.size === 0)
          return false;

        if (this.prefix.size > bits)
          return false;

        if (this.left.length !== hash.size)
          return false;

        if (this.right.length !== hash.size)
          return false;

        break;
      case TYPE_COLLISION:
        if (this.prefix)
          return false;

        if (this.left)
          return false;

        if (this.right)
          return false;

        if (!this.key)
          return false;

        if (!this.hash)
          return false;

        if (this.value)
          return false;

        if (this.key.length !== (bits >>> 3))
          return false;

        if (this.hash.length !== hash.size)
          return false;

        break;
      case TYPE_EXISTS:
        if (this.prefix)
          return false;

        if (this.left)
          return false;

        if (this.right)
          return false;

        if (this.key)
          return false;

        if (this.hash)
          return false;

        if (!this.value)
          return false;

        if (this.value.length > 0xffff)
          return false;

        break;
      default:
        return false;
    }

    return true;
  }

  push(prefix, node) {
    this.nodes.push(ProofNode.from(prefix, node));
    return this;
  }

  verify(root, key, hash, bits) {
    assert(Buffer.isBuffer(root));
    assert(Buffer.isBuffer(key));
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(root.length === hash.size);
    assert(key.length === (bits >>> 3));

    if (!this.isSane(hash, bits))
      return [PROOF_UNKNOWN_ERROR, null];

    let leaf = null;

    // Re-create the leaf.
    switch (this.type) {
      case TYPE_DEADEND:
        leaf = hash.zero;
        break;
      case TYPE_SHORT:
        if (this.prefix.has(key, this.depth))
          return [PROOF_SAME_PATH, null];
        leaf = hashInternal(hash, this.prefix, this.left, this.right);
        break;
      case TYPE_COLLISION:
        if (this.key.equals(key))
          return [PROOF_SAME_KEY, null];
        leaf = hashLeaf(hash, this.key, this.hash);
        break;
      case TYPE_EXISTS:
        leaf = hashValue(hash, key, this.value);
        break;
    }

    assert(leaf);

    let next = leaf;
    let depth = this.depth;

    // Traverse bits right to left.
    for (let i = this.nodes.length - 1; i >= 0; i--) {
      const {prefix, node} = this.nodes[i];

      if (depth < prefix.size + 1)
        return [PROOF_NEG_DEPTH, null];

      depth -= 1;

      if (hasBit(key, depth))
        next = hashInternal(hash, prefix, node, next);
      else
        next = hashInternal(hash, prefix, next, node);

      depth -= prefix.size;

      if (!prefix.has(key, depth))
        return [PROOF_PATH_MISMATCH, null];
    }

    if (depth !== 0)
      return [PROOF_TOO_DEEP, null];

    if (!next.equals(root))
      return [PROOF_HASH_MISMATCH, null];

    return [PROOF_OK, this.value];
  }

  getSize(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(bits < (1 << 14));
    assert(this.isSane(hash, bits));

    let size = 0;

    size += 2;
    size += 2;
    size += (this.nodes.length + 7) >>> 3;

    for (const {prefix, node} of this.nodes) {
      if (prefix.size !== 0)
        size += prefix.getSize();

      size += node.length;
    }

    switch (this.type) {
      case TYPE_DEADEND:
        break;
      case TYPE_SHORT:
        size += this.prefix.getSize();
        size += hash.size;
        size += hash.size;
        break;
      case TYPE_COLLISION:
        size += bits >>> 3;
        size += hash.size;
        break;
      case TYPE_EXISTS:
        size += 2;
        size += this.value.length;
        break;
    }

    return size;
  }

  write(data, off, hash, bits) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);

    const size = this.getSize(hash, bits);
    const count = this.nodes.length;
    const bsize = (count + 7) >>> 3;
    const field = (this.type << 14) | this.depth;

    let pos = off;

    checkWrite(pos + size <= data.length, pos);

    pos = writeU16(data, field, pos);
    pos = writeU16(data, count, pos);

    data.fill(0x00, pos, pos + bsize);
    pos += bsize;

    for (let i = 0; i < this.nodes.length; i++) {
      const {prefix, node} = this.nodes[i];

      if (prefix.size !== 0) {
        setBit(data, (off + 4) * 8 + i, 1);
        pos = prefix.write(data, pos);
      }

      pos += node.copy(data, pos);
    }

    switch (this.type) {
      case TYPE_DEADEND:
        break;
      case TYPE_SHORT:
        pos = this.prefix.write(data, pos);
        pos += this.left.copy(data, pos);
        pos += this.right.copy(data, pos);
        break;
      case TYPE_COLLISION:
        pos += this.key.copy(data, pos);
        pos += this.hash.copy(data, pos);
        break;
      case TYPE_EXISTS:
        pos = writeU16(data, this.value.length, pos);
        pos += this.value.copy(data, pos);
        break;
    }

    checkWrite((pos - off) === size, pos);

    return pos;
  }

  writeBW(bw, hash, bits) {
    assert(bw && typeof bw.writeU8 === 'function');
    if (bw.data)
      bw.offset = this.write(bw.data, bw.offset, hash, bits);
    else
      bw.writeBytes(this.encode(hash, bits));
    return bw;
  }

  read(data, off, hash, bits) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(bits < (1 << 14));

    let pos = off;

    checkRead(pos + 2 <= data.length, pos);

    const field = readU16(data, pos);
    pos += 2;

    this.type = field >>> 14;
    this.depth = field & ~(3 << 14);

    if (this.depth > bits)
      throw new EncodingError(pos, 'Invalid depth');

    checkRead(pos + 2 <= data.length, pos);

    const count = readU16(data, pos);
    const bsize = (count + 7) >>> 3;
    pos += 2;

    if (count > bits)
      throw new EncodingError(pos, 'Proof too large');

    checkRead(pos + bsize <= data.length, pos);
    pos += bsize;

    for (let i = 0; i < count; i++) {
      checkRead(pos + 2 <= data.length, pos);

      let prefix = Bits.EMPTY;

      if (hasBit(data, (off + 4) * 8 + i)) {
        prefix = Bits.read(data, pos);

        if (prefix.size === 0 || prefix.size > bits)
          throw new EncodingError(pos, 'Invalid prefix size');

        pos += prefix.getSize();
      }

      const node = copy(data, pos, hash.size);
      pos += hash.size;

      this.push(prefix, node);
    }

    switch (this.type) {
      case TYPE_DEADEND: {
        break;
      }

      case TYPE_SHORT: {
        this.prefix = Bits.read(data, pos);

        if (this.prefix.size === 0 || this.prefix.size > bits)
          throw new EncodingError(pos, 'Invalid prefix size');

        pos += this.prefix.getSize();

        this.left = copy(data, pos, hash.size);
        pos += hash.size;

        this.right = copy(data, pos, hash.size);
        pos += hash.size;

        break;
      }

      case TYPE_COLLISION: {
        this.key = copy(data, pos, bits >>> 3);
        pos += bits >>> 3;

        this.hash = copy(data, pos, hash.size);
        pos += hash.size;

        break;
      }

      case TYPE_EXISTS: {
        checkRead(pos + 2 <= data.length, pos);

        const size = readU16(data, pos);
        pos += 2;

        this.value = copy(data, pos, size);
        pos += size;

        break;
      }

      default: {
        throw new AssertionError('Invalid type.');
      }
    }

    return pos;
  }

  readBR(br, hash, bits) {
    assert(br && typeof br.readU8 === 'function');
    br.offset = this.read(br.data, br.offset, hash, bits);
    return this;
  }

  encode(hash, bits) {
    const size = this.getSize(hash, bits);
    const data = Buffer.allocUnsafe(size);
    this.write(data, 0, hash, bits);
    return data;
  }

  decode(data, hash, bits) {
    this.read(data, 0, hash, bits);
    return this;
  }

  toJSON() {
    return {
      type: typesByVal[this.type],
      depth: this.depth,
      nodes: this.nodes.map(node => node.toJSON()),
      prefix: this.prefix ? this.prefix.toString() : undefined,
      left: this.left ? this.left.toString('hex') : undefined,
      right: this.right ? this.right.toString('hex') : undefined,
      key: this.key ? this.key.toString('hex') : undefined,
      hash: this.hash ? this.hash.toString('hex') : undefined,
      value: this.value ? this.value.toString('hex') : undefined
    };
  }

  fromJSON(json, hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(bits < (1 << 14));

    if (!json || typeof json !== 'object')
      throw new EncodingError(0, 'Invalid JSON object');

    if (typeof json.type !== 'string' || !types.hasOwnProperty(json.type))
      throw new EncodingError(0, 'Type field must be a string');

    const type = types[json.type];

    if (!Number.isSafeInteger(json.depth))
      throw new EncodingError(0, 'Depth must be an integer');

    if (json.depth < 0 || json.depth > bits)
      throw new EncodingError(0, 'Invalid depth');

    const depth = json.depth;

    if (!Array.isArray(json.nodes))
      throw new EncodingError(0, 'Nodes field must be an array');

    if (json.nodes.length > bits)
      throw new EncodingError(0, 'Proof too large');

    this.type = type;
    this.depth = depth;

    for (const item of json.nodes)
      this.nodes.push(ProofNode.fromJSON(item, hash, bits));

    switch (type) {
      case TYPE_DEADEND:
        break;
      case TYPE_SHORT:
        this.prefix = Bits.fromString(json.prefix);
        if (this.prefix.size === 0 || this.prefix.size > bits)
          throw new EncodingError(0, 'Invalid prefix size');
        this.left = parseHex(json.left, hash.size);
        this.right = parseHex(json.right, hash.size);
        break;
      case TYPE_COLLISION:
        this.key = parseHex(json.key, bits >>> 3);
        this.hash = parseHex(json.hash, hash.size);
        break;
      case TYPE_EXISTS:
        this.value = parseHex(json.value, -1);
        break;
      case TYPE_UNKNOWN:
        throw new EncodingError(0, 'Invalid type');
      default:
        throw new AssertionError('Invalid type.');
    }

    return this;
  }

  static type(value) {
    assert((value & 0xff) === value);

    if (value >= typesByVal.length)
      value = TYPE_UNKNOWN;

    return typesByVal[value];
  }

  static code(value) {
    assert((value & 0xff) === value);

    if (value >= codesByVal.length)
      value = PROOF_UNKNOWN_ERROR;

    return codesByVal[value];
  }

  static read(data, off, hash, bits) {
    return new this().read(data, off, hash, bits);
  }

  static readBR(br, hash, bits) {
    return new this().readBR(br, hash, bits);
  }

  static decode(data, hash, bits) {
    return new this().decode(data, hash, bits);
  }

  static fromJSON(json, hash, bits) {
    return new this().fromJSON(json, hash, bits);
  }
}

Proof.types = types;
Proof.typesByVal = typesByVal;
Proof.codes = codes;
Proof.codesByVal = codesByVal;

Proof.TYPE_DEADEND = TYPE_DEADEND;
Proof.TYPE_EXISTS = TYPE_EXISTS;
Proof.TYPE_COLLISION = TYPE_COLLISION;
Proof.TYPE_SHORT = TYPE_SHORT;
Proof.TYPE_UNKNOWN = TYPE_UNKNOWN;

/**
 * ProofNode
 */

class ProofNode {
  constructor() {
    this.prefix = Bits.EMPTY;
    this.node = EMPTY;
  }

  from(prefix, node) {
    assert(prefix instanceof Bits);
    assert(Buffer.isBuffer(node));
    this.prefix = prefix;
    this.node = node;
    return this;
  }

  toJSON() {
    return [this.prefix.toString(), this.node.toString('hex')];
  }

  fromJSON(json, hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    if (!Array.isArray(json) || json.length !== 2)
      throw new EncodingError(0, 'Item must be an array');

    const [prefix, node] = json;

    if (typeof prefix !== 'string')
      throw new EncodingError(0, 'Prefix must be a string');

    if (prefix.length > bits)
      throw new EncodingError(0, 'Proof too large');

    this.prefix = Bits.fromString(prefix);
    this.node = parseHex(node, hash.size);

    return this;
  }

  inspect() {
    const prefix = this.prefix.toString();
    const node = this.node.toString('hex');

    return `<ProofNode: ${prefix}:${node}>`;
  }

  static from(prefix, node) {
    return new this().from(prefix, node);
  }

  static fromJSON(json, hash, bits) {
    return new this().fromJSON(json, hash, bits);
  }
}

/*
 * Helpers
 */

function copy(data, pos, size) {
  checkRead(pos + size <= data.length, pos, copy);

  const buf = Buffer.allocUnsafe(size);

  data.copy(buf, 0, pos, pos + size);

  return buf;
}

function checkWrite(ok, offset, start) {
  if (!ok) {
    throw new EncodingError(offset,
      'Out of bounds write',
      start || checkWrite);
  }
}

function checkRead(ok, offset, start) {
  if (!ok) {
    throw new EncodingError(offset,
      'Out of bounds read',
      start || checkRead);
  }
}

function parseHex(str, size) {
  assert(size === -1 || (size >>> 0) === size);

  if (typeof str !== 'string')
    throw new EncodingError(0, 'Field must be a string');

  if ((str.length >>> 1) > 0xffff)
    throw new EncodingError(0, 'Hex string too large');

  if (size !== -1 && (str.length >>> 1) !== size)
    throw new EncodingError(0, 'Unexpected hex string size');

  const data = Buffer.from(str, 'hex');

  if (data.length !== (str.length >>> 1))
    throw new EncodingError(0, 'Invalid hex string');

  return data;
}

/*
 * Expose
 */

module.exports = Proof;
