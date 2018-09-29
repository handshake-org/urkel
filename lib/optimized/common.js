/*!
 * common.js - tree common functions
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('bsert');

/*
 * Constants
 */

const INTERNAL_PREFIX = Buffer.from([0x01]);
const LEAF_PREFIX = Buffer.from([0x00]);
const EMPTY = Buffer.alloc(0);

/*
 * Common
 */

function hasBit(key, index) {
  const oct = index >>> 3;
  const bit = index & 7;
  return (key[oct] >>> (7 - bit)) & 1;
}

function setBit(key, index, b) {
  const oct = index >>> 3;
  const bit = index & 7;
  key[oct] |= b << (7 - bit);
}

function countBits(a, b, depth) {
  let i = depth;

  while (hasBit(a, i) === hasBit(b, i))
    i += 1;

  return i - depth;
}

function hashInternal(hash, left, right) {
  return hash.multi(INTERNAL_PREFIX, left, right);
}

function hashLeaf(hash, key, valueHash) {
  return hash.multi(LEAF_PREFIX, key, valueHash);
}

function hashValue(hash, key, value) {
  const valueHash = hash.digest(value);
  return hashLeaf(hash, key, valueHash);
}

function parseU32(name) {
  assert(typeof name === 'string');

  if (name.length !== 10)
    return -1;

  let num = 0;

  for (let i = 0; i < 10; i++) {
    const ch = name.charCodeAt(i);

    if (ch < 0x30 || ch > 0x39)
      return -1;

    num *= 10;
    num += ch - 0x30;

    if (num > 0xffffffff)
      return -1;
  }

  return num;
}

function serializeU32(num) {
  assert((num >>> 0) === num);

  let str = num.toString(10);

  while (str.length < 10)
    str = '0' + str;

  return str;
}

function randomString() {
  const m = Number.MAX_SAFE_INTEGER;
  const n = Math.random() * m;
  const s = Math.floor(n);
  return s.toString(32);
}

function randomPath(path) {
  assert(typeof path === 'string');

  while (path.length > 1) {
    const ch = path[path.length - 1];

    if (ch !== '/' && ch !== '\\')
      break;

    path = path.slice(0, -1);
  }

  return `${path}.${randomString()}~`;
}

function readU16(data, off) {
  return data[off++] + data[off] * 0x100;
}

function readU24(data, off) {
  return (data[off++]
    + data[off++] * 0x100
    + data[off] * 0x10000);
}

function readU32(data, off) {
  return (data[off++]
    + data[off++] * 0x100
    + data[off++] * 0x10000
    + data[off] * 0x1000000);
}

function writeU16(dst, num, off) {
  dst[off++] = num;
  dst[off++] = num >>> 8;
  return off;
}

function writeU24(dst, num, off) {
  dst[off++] = num;
  num >>>= 8;
  dst[off++] = num;
  num >>>= 8;
  dst[off++] = num;
  return off;
}

function writeU32(dst, num, off) {
  dst[off++] = num;
  num >>>= 8;
  dst[off++] = num;
  num >>>= 8;
  dst[off++] = num;
  num >>>= 8;
  dst[off++] = num;
  return off;
}

function randomBytes(size) {
  assert((size & 0xffff) === size);

  const bytes = Buffer.allocUnsafe(size);

  // Does not need to be cryptographically
  // strong, just needs to be _different_
  // from everyone else to make an attack
  // not worth trying. Predicting one user's
  // key does nothing to help an attacker.
  for (let i = 0; i < bytes.length; i++)
    bytes[i] = (Math.random() * 0x100) >>> 0;

  return bytes;
}

function checksum(hash, data, key) {
  switch (hash.name) {
    case 'BLAKE2b160':
    case 'BLAKE2b256':
    case 'BLAKE2b384':
    case 'BLAKE2b512':
    case 'BLAKE2s160':
    case 'BLAKE2s224':
    case 'BLAKE2s256':
      // Hack.
      hash = hash.__proto__;
      break;
  }

  // Special case for blake2.
  if (hash.name === 'BLAKE2b' || hash.name === 'BLAKE2s')
    return hash.digest(data, 20, key);

  assert(hash.size >= 20);

  return hash.multi(data, key).slice(0, 20);
}

/*
 * Expose
 */

exports.EMPTY = EMPTY;
exports.hasBit = hasBit;
exports.setBit = setBit;
exports.countBits = countBits;
exports.hashInternal = hashInternal;
exports.hashLeaf = hashLeaf;
exports.hashValue = hashValue;
exports.parseU32 = parseU32;
exports.serializeU32 = serializeU32;
exports.randomString = randomString;
exports.randomPath = randomPath;
exports.readU16 = readU16;
exports.readU24 = readU24;
exports.readU32 = readU32;
exports.writeU16 = writeU16;
exports.writeU24 = writeU24;
exports.writeU32 = writeU32;
exports.randomBytes = randomBytes;
exports.checksum = checksum;
