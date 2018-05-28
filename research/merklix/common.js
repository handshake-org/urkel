/*!
 * common.js - merklix tree common functions
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');

/*
 * Constants
 */

const INTERNAL_PREFIX = Buffer.from([0x01]);
const LEAF_PREFIX = Buffer.from([0x00]);
const EMPTY = Buffer.alloc(0);

/*
 * Common
 */

function ensureHash(hash) {
  assert(hash);
  assert(typeof hash.name === 'string');
  assert(typeof hash.digest === 'function');

  if (hash.multi)
    return hash;

  hash.id = hash.name.toLowerCase();
  hash.size = hash.digest(EMPTY).length;
  hash.bits = hash.size * 8;
  hash.zero = Buffer.alloc(hash.size, 0x00);
  hash.ctx = hash.hash();
  hash.multi = function multi(one, two, three, a, b) {
    const ctx = hash.ctx;
    ctx.init(a);
    ctx.update(one);
    ctx.update(two);
    if (three)
      ctx.update(three);
    return ctx.final(b);
  };

  ({ __proto__: hash });

  return hash;
}

function hasBit(key, index) {
  const oct = index >>> 3;
  const bit = index & 7;
  return (key[oct] >>> (7 - bit)) & 1;
}

function setBit(key, index) {
  const oct = index >>> 3;
  const bit = index & 7;
  key[oct] |= 1 << (7 - bit);
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

function fromRecord(data) {
  assert(Buffer.isBuffer(data));
  assert(data.length === 6);

  return [
    data.readUInt16LE(0, true),
    data.readUInt32LE(2, true)
  ];
}

function toRecord(index, pos) {
  assert((index & 0xffff) === index);
  assert((pos >>> 0) === pos);

  const buf = Buffer.allocUnsafe(6);

  buf.writeUInt16LE(index, 0, true);
  buf.writeUInt32LE(pos, 2, true);

  return buf;
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
  const first = data[off];
  const last = data[off + 1];

  return first + last * 2 ** 8;
}

function readU32(data, off) {
  const first = data[off];
  const last = data[off + 3];

  return first +
    data[++off] * 2 ** 8 +
    data[++off] * 2 ** 16 +
    last * 2 ** 24;
}

function writeU16(dst, num, off) {
  dst[off++] = num;
  dst[off++] = (num >>> 8);
  return off;
}

function writeU32(dst, num, off) {
  dst[off++] = num;
  num = num >>> 8;
  dst[off++] = num;
  num = num >>> 8;
  dst[off++] = num;
  num = num >>> 8;
  dst[off++] = num;
  return off;
}

/*
 * Expose
 */

exports.EMPTY = EMPTY;
exports.ensureHash = ensureHash;
exports.hasBit = hasBit;
exports.setBit = setBit;
exports.hashInternal = hashInternal;
exports.hashLeaf = hashLeaf;
exports.hashValue = hashValue;
exports.parseU32 = parseU32;
exports.serializeU32 = serializeU32;
exports.fromRecord = fromRecord;
exports.toRecord = toRecord;
exports.randomString = randomString;
exports.randomPath = randomPath;
exports.readU16 = readU16;
exports.readU32 = readU32;
exports.writeU16 = writeU16;
exports.writeU32 = writeU32;
