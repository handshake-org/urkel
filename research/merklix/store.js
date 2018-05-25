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

const {
  ensureHash,
  parseU32,
  serializeU32,
  EMPTY
} = common;

const {
  decodeNode,
  NIL,
  Internal
} = nodes;

/*
 * Constants
 */

const MAX_FILE_SIZE = 0x7fffffff;
const MAX_FILES = 0xffff;
const MAX_OPEN_FILES = 64;

/**
 * File Store
 */

class FileStore {
  constructor(prefix, hash, bits) {
    assert(typeof prefix === 'string');
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    this.hash = ensureHash(hash);
    this.bits = bits;
    this.nodeSize = Internal.getSize(hash, bits);
    this.prefix = prefix;
    this.wb = new WriteBuffer();
    this.files = [];
    this.current = null;
    this.index = 0;
    this.total = 0;
  }

  name(index) {
    const name = serializeU32(index);
    return Path.resolve(this.prefix, name);
  }

  async readdir() {
    const names = await fs.readdir(this.prefix);
    const files = [];

    for (const name of names) {
      const index = parseU32(name);

      if (index === -1)
        continue;

      if (index === 0)
        continue;

      if (index > MAX_FILES)
        continue;

      const path = Path.resolve(this.prefix, name);

      files.push({
        index,
        path
      });
    }

    return files.sort((a, b) => a.index - b.index);
  }

  async stat() {
    const files = await this.readdir();

    let size = 0;

    for (const {path} of files) {
      const stat = await fs.stat(path);
      size += stat.size;
    }

    return {
      files: files.length,
      size
    };
  }

  async ensure() {
    if (this.index !== 0)
      throw new Error('Store already opened.');

    await fs.mkdirp(this.prefix, 0o770);

    const files = await this.readdir();

    let best = 1;

    for (const {index} of files) {
      if (index > best)
        best = index;
    }

    return best;
  }

  async open() {
    if (this.index !== 0)
      throw new Error('Store already opened.');

    this.index = await this.ensure();
    this.current = await this.openFile(this.index, 'a+');
  }

  async close() {
    if (this.index === 0)
      throw new Error('Store already closed.');

    for (let i = 0; i < this.files.length; i++)
      await this.closeFile(i);

    this.files.length = 0;
    this.current = null;
    this.index = 0;
    this.total = 0;
  }

  async destroy() {
    if (this.index !== 0)
      throw new Error('Store is opened.');

    const files = await this.readdir();

    for (const {path} of files)
      await fs.unlink(path);

    return fs.rmdir(this.prefix);
  }

  async rename(prefix) {
    assert(typeof prefix === 'string');

    if (this.index !== 0)
      throw new Error('Store is opened.');

    await fs.rename(this.prefix, prefix);

    this.prefix = prefix;
  }

  async openFile(index, flags) {
    assert((index >>> 0) === index);
    assert(index !== 0);
    assert(typeof flags === 'string');

    if (this.index === 0)
      throw new Error('Store is closed.');

    // Temporary?
    assert((index & 0xffff) === index);

    while (index >= this.files.length)
      this.files.push(null);

    if (!this.files[index]) {
      const file = new File(index);
      const name = this.name(index);

      if (this.total >= MAX_OPEN_FILES)
        this.evict();

      await file.open(name, flags);

      this.files[index] = file;
      this.total += 1;
    }

    return this.files[index];
  }

  async closeFile(index) {
    assert((index >>> 0) === index);

    if (this.index === 0)
      throw new Error('Store is closed.');

    const file = this.files[index];

    if (!file)
      return;

    this.files[index] = null;
    this.total -= 1;

    await file.close();
  }

  async unlinkFile(index) {
    assert((index >>> 0) === index);
    assert(index !== this.index);

    if (this.index === 0)
      throw new Error('Store is closed.');

    await this.closeFile(index);
    await fs.unlink(this.name(index));
  }

  async prune(max) {
    assert((max >>> 0) === max);

    if (this.index === 0)
      throw new Error('Store is closed.');

    for (let i = 0; i < this.files.length; i++) {
      const file = this.files[i];

      if (!file)
        continue;

      if (file.index === this.current.index)
        continue;

      await this.closeFile(file.index);
    }

    const files = await this.readdir();

    for (const {path, index} of files) {
      if (index > max)
        continue;

      await fs.unlink(path);
    }
  }

  async advance() {
    if (this.index === 0)
      throw new Error('Store is closed.');

    await this.current.sync();
    await this.closeFile(this.index);

    this.current = await this.openFile(this.index + 1, 'a+');
    this.index += 1;

    return this.index - 1;
  }

  evict() {
    if (this.index === 0)
      throw new Error('Store is closed.');

    const files = [];

    for (let i = 0; i < this.files.length; i++) {
      const file = this.files[i];

      if (!file)
        continue;

      if (file.index === this.current.index)
        continue;

      if (file.reads > 0)
        continue;

      files.push(file);
    }

    if (files.length === 0)
      return;

    const i = Math.random() * files.length | 0;
    const file = files[i];

    this.files[file.index] = null;
    this.total -= 1;

    file.closeSync();
  }

  async read(index, pos, size) {
    if (this.index === 0)
      throw new Error('Store is closed.');

    const file = await this.openFile(index, 'r');

    return file.read(pos, size);
  }

  async write(data) {
    if (this.index === 0)
      throw new Error('Store is closed.');

    if (this.current.pos + data.length > MAX_FILE_SIZE) {
      await this.current.sync();
      await this.closeFile(this.index);
      this.current = await this.openFile(this.index + 1, 'a+');
      this.index += 1;
    }

    return this.current.write(data);
  }

  async sync() {
    if (this.index === 0)
      throw new Error('Store is closed.');

    return this.current.sync();
  }

  async readNode(index, pos) {
    const data = await this.read(index, pos, this.nodeSize);
    return decodeNode(data, this.hash, this.bits, index, pos);
  }

  async readRoot() {
    if (this.index === 0)
      throw new Error('Store is closed.');

    if (this.current.pos < this.nodeSize)
      return NIL;

    return this.readNode(this.current.pos - this.nodeSize);
  }

  start() {
    this.wb.offset = this.current.pos;
    this.wb.index = this.current.index;
    this.wb.start = 0;
    this.wb.written = 0;
    return this;
  }

  writeNode(node) {
    assert(node.index === 0);

    this.wb.expand(this.nodeSize);

    const written = this.wb.written;

    node.write(
      this.wb.data,
      this.wb.written,
      this.hash,
      this.bits
    );

    this.wb.written += this.nodeSize;

    node.pos = this.wb.position(written);
    node.index = this.wb.index;

    return node.pos;
  }

  writeValue(node) {
    assert(node.isLeaf());
    assert(node.index === 0);
    node.vsize = node.value.length;
    node.vpos = this.wb.write(node.value);
    node.vindex = this.wb.index;
    return node.vpos;
  }

  async flush() {
    for (const chunk of this.wb.flush())
      await this.write(chunk);
  }
}

/**
 * Write Buffer
 */

class WriteBuffer {
  constructor() {
    this.offset = 0;
    this.index = 0;
    this.start = 0;
    this.written = 0;
    this.data = EMPTY;
    this.chunks = [];
  }

  position(written) {
    return this.offset + (written - this.start);
  }

  expand(size) {
    if (this.data.length === 0)
      this.data = Buffer.allocUnsafe(8192);

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
    this.written = 0;
    this.index = 0;
    this.data = EMPTY;
  }

  async stat() {
    return {
      files: 1,
      size: this.written
    };
  }

  async open() {
    if (this.index !== 0)
      throw new Error('Store already opened.');

    this.index = 1;
  }

  async close() {
    if (this.index === 0)
      throw new Error('Store already closed.');

    this.index = 0;
  }

  async destroy() {
    if (this.index !== 0)
      throw new Error('Store is opened.');
  }

  async rename(prefix) {
    assert(typeof prefix === 'string');

    if (this.index !== 0)
      throw new Error('Store is opened.');
  }

  async prune(max) {
    assert((max >>> 0) === max);

    if (this.index === 0)
      throw new Error('Store is closed.');
  }

  async advance() {
    if (this.index === 0)
      throw new Error('Store closed.');
  }

  async read(index, pos, bytes) {
    if (this.index === 0)
      throw new Error('Store is closed.');

    assert(pos + bytes <= this.written);

    const buf = Buffer.allocUnsafe(bytes);
    this.data.copy(buf, 0, pos, pos + bytes);

    return buf;
  }

  expand(size) {
    if (this.data.length === 0)
      this.data = Buffer.allocUnsafe(8192);

    while (this.written + size > this.data.length) {
      const buf = Buffer.allocUnsafe(this.data.length * 2);
      this.data.copy(buf, 0);
      this.data = buf;
    }
  }

  write(data) {
    if (this.index === 0)
      throw new Error('Store is closed.');

    this.expand(data.length);

    const pos = this.written;

    this.written += data.copy(this.data, this.written);

    return pos;
  }

  async sync() {
    if (this.index === 0)
      throw new Error('Store closed.');
  }

  async readNode(index, pos) {
    const data = await this.read(index, pos, this.nodeSize);
    return decodeNode(data, this.hash, this.bits, index, pos);
  }

  async readRoot() {
    if (this.index === 0)
      throw new Error('Store is closed.');

    if (this.written < this.nodeSize)
      return NIL;

    return this.readNode(1, this.written - this.nodeSize);
  }

  start() {
    return this;
  }

  writeNode(node) {
    assert(node.index === 0);

    this.expand(this.nodeSize);

    const written = this.written;

    node.write(
      this.data,
      this.written,
      this.hash,
      this.bits
    );

    this.written += this.nodeSize;

    node.pos = written;
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
exports.WriteBuffer = WriteBuffer;
exports.MemoryStore = MemoryStore;
