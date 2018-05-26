/*!
 * proof.js - merklix tree proofs
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const common = require('./common');
const {MissingNodeError} = require('./errors');

const {
  ensureHash,
  hasBit,
  setBit,
  hashInternal,
  hashLeaf,
  hashValue
} = common;

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
 * Get proof type as a string.
 * @param {Number} value
 * @returns {String}
 */

function type(value) {
  assert((value & 0xff) === value);

  if (value >= typesByVal.length)
    value = TYPE_UNKNOWN;

  return typesByVal[value];
}

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
 * Get verification error code as a string.
 * @param {Number} value
 * @returns {String}
 */

function code(value) {
  assert((value & 0xff) === value);

  if (value >= codesByVal.length)
    value = PROOF_UNKNOWN_ERROR;

  return codesByVal[value];
}

/*
 * Proofs
 */

async function prove(tree, root, key) {
  assert(tree);
  assert(tree.isHash(root));
  assert(tree.isKey(key));

  const {hash, store, bits} = tree;
  const proof = new Proof();

  let node = await tree.getRoot(root);
  let depth = 0;

  // Traverse bits left to right.
  for (;;) {
    // Empty (sub)tree.
    if (node.isNull())
      break;

    // Leaf node.
    if (node.isLeaf()) {
      const value = await node.getValue(store);

      if (node.key.equals(key)) {
        proof.value = value;
      } else {
        proof.key = node.key;
        proof.hash = hash.digest(value);
      }

      break;
    }

    if (depth === bits) {
      throw new MissingNodeError({
        rootHash: root.hash(hash),
        key,
        depth
      });
    }

    assert(node.isInternal());

    // Internal node.
    if (hasBit(key, depth)) {
      const h = node.left.hash(hash);
      proof.nodes.push(h);
      node = await node.getRight(store);
    } else {
      const h = node.right.hash(hash);
      proof.nodes.push(h);
      node = await node.getLeft(store);
    }

    depth += 1;
  }

  return proof;
}

function verify(hash, bits, root, key, proof) {
  assert(hash && typeof hash.digest === 'function');
  assert((bits >>> 0) === bits);
  assert(bits > 0 && (bits & 7) === 0);
  assert(Buffer.isBuffer(root));
  assert(Buffer.isBuffer(key));
  assert(root.length === hash.size);
  assert(key.length === (bits >>> 3));
  assert(proof instanceof Proof);
  assert(proof.nodes.length <= bits);

  hash = ensureHash(hash);

  let leaf = null;

  // Re-create the leaf.
  switch (proof.type) {
    case TYPE_EXISTS:
      leaf = hashValue(hash, key, proof.value);
      break;
    case TYPE_DEADEND:
      leaf = hash.zero;
      break;
    case TYPE_COLLISION:
      if (proof.key.equals(key))
        return [PROOF_SAME_KEY, null];
      leaf = hashLeaf(hash, proof.key, proof.hash);
      break;
  }

  assert(leaf);

  let next = leaf;
  let depth = proof.nodes.length - 1;

  // Traverse bits right to left.
  while (depth >= 0) {
    const node = proof.nodes[depth];

    if (hasBit(key, depth))
      next = hashInternal(hash, node, next);
    else
      next = hashInternal(hash, next, node);

    depth -= 1;
  }

  if (!next.equals(root))
    return [PROOF_HASH_MISMATCH, null];

  return [PROOF_OK, proof.value];
}

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

  getSize(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(this.nodes.length <= bits);
    assert(bits < (1 << 14));

    hash = ensureHash(hash);

    let size = 0;

    size += 2;
    size += (this.nodes.length + 7) / 8 | 0;

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
    }

    return size;
  }

  write(data, off, hash, bits) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);

    const size = this.getSize(hash, bits);
    const bsize = (this.nodes.length + 7) / 8 | 0;

    assert(off + size <= data.length);

    let pos = off;

    let field = this.type << 14;
    field |= this.nodes.length;

    pos = data.writeUInt16LE(field, pos, true);

    data.fill(0x00, pos, pos + bsize);

    pos += bsize;

    for (let i = 0; i < this.nodes.length; i++) {
      const node = this.nodes[i];

      if (node.equals(hash.zero))
        setBit(data, 16 + i);
      else
        pos += node.copy(data, pos);
    }

    switch (this.type) {
      case TYPE_EXISTS:
        pos = data.writeUInt16LE(field, pos);
        pos += this.value.copy(data, pos);
        break;
      case TYPE_DEADEND:
        break;
      case TYPE_COLLISION:
        pos += this.key.copy(data, pos);
        pos += this.hash.copy(data, pos);
        break;
    }

    assert((pos - off) === size);

    return pos;
  }

  read(data, off, hash, bits) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    assert(bits < (1 << 14));

    hash = ensureHash(hash);

    let pos = off;
    let count = 0;

    assert(pos + 2 <= data.length);

    const field = data.readUInt16LE(pos, true);
    pos += 2;

    const type = field >>> 14;
    const count = field & ~(3 << 14);

    if (count > bits)
      throw new Error('Proof too large.');

    const bsize = (count + 7) / 8 | 0;

    assert(pos + bsize <= data.length);

    pos += bsize;

    for (let i = 0; i < count; i++) {
      if (hasBit(data, 16 + i)) {
        this.nodes.push(hash.zero);
      } else {
        const h = copy(data, pos, hash.size);
        this.nodes.push(h);
        pos += hash.size;
      }
    }

    switch (type) {
      case TYPE_EXISTS: {
        assert(pos + 2 <= data.length);

        const size = data.readUInt16LE(pos, true);
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
        throw new Error('Invalid type.');
      }
      default: {
        assert(false);
        break;
      }
    }

    return pos;
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

  static decode(data, hash, bits) {
    return new this().decode(data, hash, bits);
  }
}

/*
 * Helpers
 */

function copy(data, pos, size) {
  assert(pos + size <= data.length);
  const buf = Buffer.allocUnsafe(size);
  data.copy(buf, 0, pos, pos + size);
  return buf;
}

/*
 * Expose
 */

exports.types = types;
exports.typesByVal = typesByVal;
exports.type = type;
exports.codes = codes;
exports.codesByVal = codesByVal;
exports.code = code;
exports.prove = prove;
exports.verify = verify;
exports.Proof = Proof;
