/*!
 * common.js - tree common functions
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
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

// https://stackoverflow.com/questions/664014
function phf32(x) {
  x = ((x >>> 16) ^ x) * 0x45d9f3b;
  x = ((x >>> 16) ^ x) * 0x45d9f3b;
  x = (x >>> 16) ^ x;
  return x >>> 0;
}

function hashPerfect(data) {
  assert(Buffer.isBuffer(data));

  const blocks = data.length >>> 2;
  const trail = data.length & 3;
  const need = trail > 0 ? 4 - trail : 0;
  const size = data.length + need;
  const hash = Buffer.allocUnsafe(size);

  for (let b = 0; b < blocks; b++) {
    const n = readU32(data, b * 4);
    writeU32(hash, phf32(n), b * 4);
  }

  if (trail === 0)
    return hash;

  const i = blocks * 4;

  let n = 0;

  switch (trail) {
    case 3:
      n |= data[i + 2] << 16;
    case 2:
      n |= data[i + 1] << 8;
    case 1:
      n |= data[i];
  }

  writeU32(hash, phf32(n), i);

  return hash;
}

function randomBytes(size) {
  assert((size & 0xffff) === size);

  const bytes = Buffer.allocUnsafe(size);

  for (let i = 0; i < bytes.length; i++)
    bytes[i] = (Math.random() * 0x100) >>> 0;

  return bytes;
}

/*
 * Expose
 */

exports.EMPTY = EMPTY;
exports.hasBit = hasBit;
exports.setBit = setBit;
exports.hashInternal = hashInternal;
exports.hashLeaf = hashLeaf;
exports.hashValue = hashValue;
exports.parseU32 = parseU32;
exports.serializeU32 = serializeU32;
exports.randomString = randomString;
exports.randomPath = randomPath;
exports.readU16 = readU16;
exports.readU32 = readU32;
exports.writeU16 = writeU16;
exports.writeU32 = writeU32;
exports.hashPerfect = hashPerfect;
exports.randomBytes = randomBytes;
