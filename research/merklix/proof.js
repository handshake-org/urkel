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
  hashLeaf
} = common;

/*
 * Constants
 */

const PROOF_OK = 0;
const PROOF_HASH_MISMATCH = 1;
const PROOF_MALFORMED_NODE = 2;
const PROOF_UNEXPECTED_NODE = 3;
const PROOF_EARLY_END = 4;
const PROOF_NO_RESULT = 5;
const PROOF_UNKNOWN_ERROR = 6;

/**
 * Verification error codes.
 * @enum {Number}
 */

const codes = {
  PROOF_OK,
  PROOF_HASH_MISMATCH,
  PROOF_MALFORMED_NODE,
  PROOF_UNEXPECTED_NODE,
  PROOF_EARLY_END,
  PROOF_NO_RESULT,
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
  'PROOF_MALFORMED_NODE',
  'PROOF_UNEXPECTED_NODE',
  'PROOF_EARLY_END',
  'PROOF_NO_RESULT',
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

  const nodes = [];
  const ctx = tree.ctx();

  let node = await tree.getRoot(root);
  let depth = 0;
  let k = null;
  let v = null;

  // Traverse bits left to right.
  for (;;) {
    // Empty (sub)tree.
    if (node.isNull()) {
      nodes.push(node.hash(ctx));
      break;
    }

    // Leaf node.
    if (node.isLeaf()) {
      nodes.push(node.hash(ctx));

      if (!key.equals(node.key))
        k = node.key;

      v = await node.getValue(tree.store);

      break;
    }

    if (depth === tree.bits) {
      throw new MissingNodeError({
        rootHash: root.hash(ctx),
        key,
        depth
      });
    }

    assert(node.isInternal());

    // Internal node.
    if (hasBit(key, depth)) {
      nodes.push(node.left.hash(ctx));
      node = await node.getRight(tree.store);
    } else {
      nodes.push(node.right.hash(ctx));
      node = await node.getLeft(tree.store);
    }

    depth += 1;
  }

  return new Proof(nodes, k, v);
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

  hash = ensureHash(hash);

  const nodes = proof.nodes;

  if (nodes.length === 0)
    return [PROOF_EARLY_END, null];

  if (nodes.length > bits)
    return [PROOF_MALFORMED_NODE, null];

  const ctx = hash.hash();
  const leaf = nodes[nodes.length - 1];

  let next = leaf;
  let depth = nodes.length - 2;

  // Traverse bits right to left.
  while (depth >= 0) {
    const node = nodes[depth];

    if (hasBit(key, depth))
      next = hashInternal(ctx, node, next);
    else
      next = hashInternal(ctx, next, node);

    depth -= 1;
  }

  if (!next.equals(root))
    return [PROOF_HASH_MISMATCH, null];

  // Two types of NX proofs.

  // Type 1: Non-existent leaf.
  if (leaf.equals(hash.zero)) {
    if (proof.key)
      return [PROOF_UNEXPECTED_NODE, null];

    if (proof.value)
      return [PROOF_UNEXPECTED_NODE, null];

    return [PROOF_OK, null];
  }

  // Type 2: Prefix collision.
  // We have to provide the full preimage
  // to prove we're a leaf, and also that
  // we are indeed a different key.
  if (proof.key) {
    if (!proof.value)
      return [PROOF_UNEXPECTED_NODE, null];

    if (proof.key.equals(key))
      return [PROOF_UNEXPECTED_NODE, null];

    const h = hashLeaf(ctx, proof.key, proof.value);

    if (!h.equals(leaf))
      return [PROOF_HASH_MISMATCH, null];

    return [PROOF_OK, null];
  }

  // Otherwise, we should have a value.
  if (!proof.value)
    return [PROOF_NO_RESULT, null];

  const h = hashLeaf(ctx, key, proof.value);

  if (!h.equals(leaf))
    return [PROOF_HASH_MISMATCH, null];

  return [PROOF_OK, proof.value];
}

/**
 * Proof
 */

class Proof {
  constructor(nodes, key, value) {
    this.nodes = [];
    this.key = null;
    this.value = null;
    this.from(nodes, key, value);
  }

  from(nodes, key, value) {
    if (nodes != null) {
      assert(Array.isArray(nodes));
      this.nodes = nodes;
    }

    if (key != null) {
      assert(Buffer.isBuffer(key));
      this.key = key;
    }

    if (value != null) {
      assert(Buffer.isBuffer(value));
      this.value = value;
    }

    return this;
  }

  getSize(hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    hash = ensureHash(hash);

    let size = 0;

    size += 1;
    size += (this.nodes.length + 7) / 8 | 0;

    for (const node of this.nodes) {
      if (!node.equals(hash.zero))
        size += node.length;
    }

    size += 2;

    if (this.key)
      size += bits >>> 3;

    if (this.value)
      size += this.value.length;

    return size;
  }

  write(data, off, hash, bits) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);

    const size = this.getSize(hash, bits);
    const bsize = (this.nodes.length + 7) / 8 | 0;

    assert(off + size <= data.length);

    let pos = off;

    assert(this.nodes.length > 0);
    assert(this.nodes.length <= bits);
    assert(bits <= 256);

    data[pos] = this.nodes.length - 1;

    pos += 1;

    data.fill(0x00, pos, pos + bsize);

    pos += bsize;

    for (let i = 0; i < this.nodes.length; i++) {
      const node = this.nodes[i];

      if (node.equals(hash.zero))
        setBit(data, 8 + i);
      else
        pos += node.copy(data, pos);
    }

    let field = 0;

    if (this.key)
      field |= 1 << 15;

    if (this.value) {
      // 16kb max
      assert(this.value.length < (1 << 14));
      field |= 1 << 14;
      field |= this.value.length;
    }

    pos = data.writeUInt16LE(field, pos);

    if (this.key)
      pos += this.key.copy(data, pos);

    if (this.value)
      pos += this.value.copy(data, pos);

    assert((pos - off) === size);

    return pos;
  }

  read(data, off, hash, bits) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    hash = ensureHash(hash);

    let pos = off;

    assert(pos + 1 <= data.length);

    const count = data[pos] + 1;
    const bsize = (count + 7) / 8 | 0;

    pos += 1;
    pos += bsize;

    assert(pos <= data.length);

    for (let i = 0; i < count; i++) {
      if (hasBit(data, 8 + i)) {
        this.nodes.push(hash.zero);
      } else {
        const h = copy(data, pos, hash.size);
        this.nodes.push(h);
        pos += hash.size;
      }
    }

    assert(pos + 2 <= data.length);

    const field = data.readUInt16LE(pos, true);
    pos += 2;

    if (field & (1 << 15)) {
      const size = bits >>> 3;
      this.key = copy(data, pos, size);
      pos += size;
    }

    if (field & (1 << 14)) {
      const size = field & ((1 << 14) - 1);
      this.value = copy(data, pos, size);
      pos += size;
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

exports.codes = codes;
exports.codesByVal = codesByVal;
exports.code = code;
exports.prove = prove;
exports.verify = verify;
exports.Proof = Proof;
