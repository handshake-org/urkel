/*!
 * store.js - merklix tree storage
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const Path = require('path');
const fs = require('bfile');
const common = require('./common');
const File = require('./file');
const nodes = require('./nodes');
const {ensureHash, parseU32} = common;
const {decodeNode, NIL, Internal} = nodes;

/*
 * Constants
 */

const MAX_FILE_SIZE = 0x7fffffff;
const MAX_FILES = 0xffff;

/**
 * File Store
 */

class FileStore {
  constructor(prefix, hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    this.hash = ensureHash(hash);
    this.bits = bits;
    this.nodeSize = Internal.getSize(hash, bits);
    this.prefix = prefix || '/';
    this.arena = new Arena();
    this.files = [];
    this.current = null;
    this.index = 0;
    this.context = null;
  }

  ctx() {
    if (!this.context)
      this.context = this.hash.hash();
    return this.context;
  }

  name(num) {
    assert((num >>> 0) === num);

    let name = num.toString(10);

    while (name.length < 10)
      name = '0' + name;

    return Path.resolve(this.prefix, name);
  }

  async ensure() {
    await fs.mkdirp(this.prefix, 0o770);

    const list = await fs.readdir(this.prefix);

    let index = 1;

    for (const name of list) {
      const num = parseU32(name);

      if (num > MAX_FILES)
        continue;

      if (num > index)
        index = num;
    }

    return index;
  }

  async open() {
    if (this.index !== 0)
      throw new Error('Files already opened.');

    this.index = await this.ensure();
    this.current = await this.openFile(this.index, 'a+');
  }

  async close() {
    if (this.index === 0)
      throw new Error('File already closed.');

    for (let i = 0; i < this.files.length; i++)
      await this.closeFile(i);

    this.files.length = 0;
    this.current = null;
    this.index = 0;
  }

  async openFile(index, flags) {
    assert(index !== 0);

    while (index >= this.files.length)
      this.files.push(null);

    if (!this.files[index]) {
      const file = new File(index);
      const name = this.name(index);

      await file.open(name, flags);

      this.files[index] = file;
    }

    return this.files[index];
  }

  async closeFile(index) {
    const file = this.files[index];

    if (!file)
      return;

    await file.close();

    this.files[index] = null;
  }

  async unlinkFile(index) {
    assert(index !== this.index);
    await this.closeFile(index);
    await fs.unlink(this.name(index));
  }

  async read(index, pos, bytes) {
    const file = await this.openFile(index, 'r');
    return file.read(pos, bytes);
  }

  async write(data) {
    if (this.current.pos + data.length > MAX_FILE_SIZE) {
      await this.closeFile(this.index);
      this.current = await this.openFile(this.index + 1, 'a+');
      this.index += 1;
    }

    return this.current.write(data);
  }

  async sync() {
    return this.current.sync();
  }

  async readNode(index, pos) {
    const data = await this.read(index, pos, this.nodeSize);
    return decodeNode(data, this.hash, this.bits, index, pos);
  }

  async readRoot() {
    if (this.current.pos < this.nodeSize)
      return NIL;
    return this.readNode(this.current.pos - this.nodeSize);
  }

  start() {
    this.arena.offset = this.current.pos;
    this.arena.index = this.current.index;
    this.arena.start = 0;
    this.arena.written = 0;
    return this;
  }

  writeNode_(node) {
    const data = node.encode(this.ctx(), this.hash, this.bits);
    node.pos = this.arena.write(data);
    node.index = this.arena.index;
    return node.pos;
  }

  writeNode(node) {
    this.arena.expand(this.nodeSize);

    const written = this.arena.written;

    node.write(
      this.arena.data,
      this.arena.written,
      this.ctx(),
      this.hash,
      this.bits
    );

    this.arena.written += this.nodeSize;

    node.pos = this.arena.position(written);
    node.index = this.arena.index;

    return node.pos;
  }

  writeValue(node) {
    assert(node.isLeaf());
    assert(node.index === 0);
    node.vsize = node.value.length;
    node.vpos = this.arena.write(node.value);
    node.vindex = this.arena.index;
    return node.vpos;
  }

  async flush() {
    for (const chunk of this.arena.flush())
      await this.write(chunk);
  }
}

/**
 * Arena
 */

class Arena {
  constructor() {
    this.offset = 0;
    this.index = 0;
    this.start = 0;
    this.written = 0;
    this.data = Buffer.allocUnsafe(0);
    this.chunks = [];
  }

  position(written) {
    return this.offset + (written - this.start);
  }

  expand(size) {
    if (this.data.length === 0)
      this.data = Buffer.allocUnsafe(1024);

    while (this.written + size > this.data.length) {
      const buf = Buffer.allocUnsafe(this.data.length * 2);
      this.data.copy(buf, 0);
      this.data = buf;
    }

    if (this.position(this.written) + size > MAX_FILE_SIZE) {
      this.chunks.push(this.render());
      this.start = this.written;
      this.offset = 0;
      this.index += 1;
    }
  }

  write(data) {
    this.expand(data.length);

    const written = this.written;
    this.written += data.copy(this.data, this.written);

    return this.position(written);
  }

  render() {
    return this.data.slice(this.start, this.written);
  }

  flush() {
    const chunks = this.chunks;

    if (this.written > this.start)
      chunks.push(this.render());

    this.chunks = [];

    return chunks;
  }
}

/**
 * Memory Store
 */

class MemoryStore {
  constructor(prefix, hash, bits) {
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);
    this.hash = ensureHash(hash);
    this.bits = bits;
    this.nodeSize = Internal.getSize(hash, bits);
    this.pos = 0;
    this.index = 0;
    this.data = Buffer.allocUnsafe(0);
    this.context = null;
  }

  ctx() {
    if (!this.context)
      this.context = this.hash.hash();
    return this.context;
  }

  async open() {
    if (this.index !== 0)
      throw new Error('Files already opened.');

    this.index = 1;
  }

  async close() {
    if (this.index === 0)
      throw new Error('File already closed.');

    this.index = 0;
  }

  async read(index, pos, bytes) {
    assert(pos + bytes <= this.pos);
    const buf = Buffer.allocUnsafe(bytes);
    this.data.copy(buf, 0, pos, pos + bytes);
    return buf;
  }

  write(data) {
    if (this.data.length === 0)
      this.data = Buffer.allocUnsafe(1024);

    while (this.pos + data.length > this.data.length) {
      const buf = Buffer.allocUnsafe(this.data.length * 2);
      this.data.copy(buf, 0);
      this.data = buf;
    }

    const pos = this.pos;
    this.pos += data.copy(this.data, this.pos);

    return pos;
  }

  async sync() {}

  async readNode(index, pos) {
    const data = await this.read(index, pos, this.nodeSize);
    return decodeNode(data, this.hash, this.bits, index, pos);
  }

  async readRoot() {
    if (this.pos < this.nodeSize)
      return NIL;
    return this.readNode(1, this.pos - this.nodeSize);
  }

  start() {
    return this;
  }

  writeNode(node) {
    const data = node.encode(this.ctx(), this.hash, this.bits);
    node.pos = this.write(data);
    node.index = 1;
    return node.pos;
  }

  writeValue(node) {
    assert(node.isLeaf());
    assert(node.index === 0);
    node.vsize = node.value.length;
    node.vpos = this.write(node.value);
    node.vindex = 1;
    return node.vpos;
  }

  async flush() {}
}

/*
 * Expose
 */

exports.FileStore = FileStore;
exports.Arena = Arena;
exports.MemoryStore = MemoryStore;
