/*!
 * proof.js - tree proofs
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('bsert');
const common = require('./common');
const errors = require('./errors');

const {
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

const TYPE_EXISTS = 0;
const TYPE_DEADEND = 1;
const TYPE_COLLISION = 2;
const TYPE_UNKNOWN = 3;

const PROOF_OK = 0;
const PROOF_HASH_MISMATCH = 1;
const PROOF_SAME_KEY = 2;
const PROOF_UNKNOWN_ERROR = 3;

/**
 * Proof types.
 * @enum {Number}
 */

const types = {
  TYPE_EXISTS,
  TYPE_DEADEND,
  TYPE_COLLISION,
  TYPE_UNKNOWN
};

/**
 * Proof types (strings).
 * @const {String[]}
 * @default
 */

const typesByVal = [
  'TYPE_EXISTS',
  'TYPE_DEADEND',
  'TYPE_COLLISION',
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
  'PROOF_UNKNOWN_ERROR'
];

/**
 * Proof
 */

class Proof {
  constructor() {
    this.nodes = [];
    this.value = null;
    this.key = null;
    this.hash = null;
  }

  get type() {
    if (this.value) {
      assert(this.value.length <= 0xffff);
      assert(!this.key);
      assert(!this.hash);
      return TYPE_EXISTS;
    }

    if (this.key) {
      assert(this.hash);
      assert(!this.value);
      return TYPE_COLLISION;
    }

    return TYPE_DEADEND;
  }

  verify(root, key, hash, bits) {
    assert(Buffer.isBuffer(root));
    assert(Buffer.isBuffer(key));
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(root.length === hash.size);
    assert(key.length === (bits >>> 3));
    assert(this.nodes.length <= bits);

    let leaf = null;

    // Re-create the leaf.
    switch (this.type) {
      case TYPE_EXISTS:
        leaf = hashValue(hash, key, this.value);
        break;
      case TYPE_DEADEND:
        leaf = hash.zero;
        break;
      case TYPE_COLLISION:
        if (this.key.equals(key))
          return [PROOF_SAME_KEY, null];
        leaf = hashLeaf(hash, this.key, this.hash);
        break;
    }

    assert(leaf);

    let next = leaf;
    let depth = this.nodes.length - 1;

    // Traverse bits right to left.
    while (depth >= 0) {
      const node = this.nodes[depth];

      if (hasBit(key, depth))
        next = hashInternal(hash, node, next);
      else
        next = hashInternal(hash, next, node);

      depth -= 1;
    }

    if (!next.equals(root))
      return [PROOF_HASH_MISMATCH, null];

    return [PROOF_OK, this.value];
  }

  getSize(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(this.nodes.length <= bits);
    assert(bits < (1 << 14));

    let size = 0;

    size += 2;
    size += (this.nodes.length + 7) >>> 3;

    for (const node of this.nodes) {
      if (!node.equals(hash.zero))
        size += node.length;
    }

    switch (this.type) {
      case TYPE_EXISTS:
        size += 2;
        size += this.value.length;
        break;
      case TYPE_DEADEND:
        break;
      case TYPE_COLLISION:
        assert(this.key.length === (bits >>> 3));
        assert(this.hash.length === hash.size);
        size += bits >>> 3;
        size += hash.size;
        break;
      default:
        throw new AssertionError('Invalid type.');
    }

    return size;
  }

  write(data, off, hash, bits) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);

    const size = this.getSize(hash, bits);
    const count = this.nodes.length;
    const bsize = (count + 7) >>> 3;
    const field = (this.type << 14) | count;

    let pos = off;

    checkWrite(pos + size <= data.length, pos);

    pos = writeU16(data, field, pos);

    data.fill(0x00, pos, pos + bsize);

    pos += bsize;

    for (let i = 0; i < this.nodes.length; i++) {
      const node = this.nodes[i];

      if (node.equals(hash.zero))
        setBit(data, (off * 8) + 16 + i);
      else
        pos += node.copy(data, pos);
    }

    switch (this.type) {
      case TYPE_EXISTS:
        pos = writeU16(data, this.value.length, pos);
        pos += this.value.copy(data, pos);
        break;
      case TYPE_DEADEND:
        break;
      case TYPE_COLLISION:
        pos += this.key.copy(data, pos);
        pos += this.hash.copy(data, pos);
        break;
      default:
        throw new AssertionError('Invalid type.');
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

    const type = field >>> 14;
    const count = field & ~(3 << 14);

    if (count > bits)
      throw new EncodingError(pos, 'Proof too large');

    const bsize = (count + 7) >>> 3;

    checkRead(pos + bsize <= data.length, pos);

    pos += bsize;

    for (let i = 0; i < count; i++) {
      if (hasBit(data, (off * 8) + 16 + i)) {
        this.nodes.push(hash.zero);
      } else {
        const node = copy(data, pos, hash.size);
        this.nodes.push(node);
        pos += hash.size;
      }
    }

    switch (type) {
      case TYPE_EXISTS: {
        checkRead(pos + 2 <= data.length, pos);

        const size = readU16(data, pos);
        pos += 2;

        this.value = copy(data, pos, size);
        pos += size;

        break;
      }

      case TYPE_DEADEND: {
        break;
      }

      case TYPE_COLLISION: {
        this.key = copy(data, pos, bits >>> 3);
        pos += bits >>> 3;

        this.hash = copy(data, pos, hash.size);
        pos += hash.size;

        break;
      }

      case TYPE_UNKNOWN: {
        throw new EncodingError(pos, 'Invalid proof type');
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
      nodes: this.nodes.map(n => n.toString('hex')),
      value: this.value ? this.value.toString('hex') : undefined,
      key: this.key ? this.key.toString('hex') : undefined,
      hash: this.hash ? this.hash.toString('hex') : undefined
    };
  }

  fromJSON(json, hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(bits < (1 << 14));

    if (!json || typeof json !== 'object')
      throw new EncodingError(0, 'Invalid JSON object');

    if (!Array.isArray(json.nodes))
      throw new EncodingError(0, 'Nodes field must be an array');

    if (json.nodes.length > bits)
      throw new EncodingError(0, 'Proof too large');

    for (const node of json.nodes) {
      if (typeof node !== 'string')
        throw new EncodingError(0, 'Node must be a string');

      this.nodes.push(parseHex(node, hash.size));
    }

    if (json.value != null && typeof json.value !== 'string')
      throw new EncodingError(0, 'Value field must be a string');

    if (json.key != null && typeof json.key !== 'string')
      throw new EncodingError(0, 'Key field must be a string');

    if (json.hash != null && typeof json.hash !== 'string')
      throw new EncodingError(0, 'Hash field must be a string');

    if (json.value != null) {
      if (json.key != null || json.hash != null)
        throw new EncodingError(0, 'Invalid proof type');

      this.value = parseHex(json.value, -1);
    } else if (json.key != null || json.hash != null) {
      this.key = parseHex(json.key, bits >>> 3);
      this.hash = parseHex(json.hash, hash.size);
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
