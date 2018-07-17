/*!
 * store.js - tree storage
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('assert');
const Path = require('path');
const {Lock, MapLock} = require('bmutex');
const common = require('../common');
const errors = require('../errors');
const File = require('../file');

const {
  parseU32,
  serializeU32,
  EMPTY,
  randomPath
} = common;

const {
  AssertionError
} = errors;

/*
 * Constants
 */

// Max read size on linux, and lower than off_t max
// (libuv will use a 32 bit off_t on 32 bit systems).
const MAX_FILE_SIZE = 0x7ffff000; // File max = 2 GB

/**
 * BaseStore
 */

class BaseStore {
  constructor(fs, prefix) {
    assert(fs && typeof fs.write === 'function');
    assert(typeof prefix === 'string');

    this.fs = fs;
    this.prefix = prefix;
    this.openLock = MapLock.create();
    this.readLock = Lock.create();

    this.maxFiles = 0xffff;
    this.maxOpenFiles = 32;
    this.maxBufferSize = 120 << 20;

    this.buffer = new WriteBuffer();
    this.files = new FileMap(this.maxFiles);
    this.current = null;
    this.index = 0;
  }

  path(index) {
    const name = serializeU32(index);
    return Path.resolve(this.prefix, name);
  }

  async readdir() {
    const names = await this.fs.readdir(this.prefix);
    const files = [];

    for (const name of names) {
      const index = parseU32(name);

      if (index === -1)
        continue;

      if (index === 0)
        continue;

      if (index > this.maxFiles)
        continue;

      const path = Path.resolve(this.prefix, name);
      const stat = await this.fs.lstat(path);

      if (!stat.isFile())
        continue;

      files.push({
        index,
        name,
        path,
        size: stat.size
      });
    }

    files.sort((a, b) => a.index - b.index);

    for (let i = 0; i < files.length; i++) {
      const file = files[i];

      if (i > 0 && file.index !== files[i - 1].index + 1)
        throw new Error('Missing files.');
    }

    return files;
  }

  async stat() {
    const files = await this.readdir();

    let total = 0;

    for (const {size} of files)
      total += size;

    return {
      files: files.length,
      size: total
    };
  }

  async ensure() {
    if (this.index !== 0)
      throw new Error('Store already opened.');

    await this.fs.mkdirp(this.prefix, 0o750);

    const files = await this.readdir();

    if (files.length === 0)
      return 0;

    return files[files.length - 1].index;
  }

  async close() {
    if (this.index === 0)
      throw new Error('Store already closed.');

    const files = this.files;

    this.buffer = new WriteBuffer();
    this.files = new FileMap();
    this.current = null;
    this.index = 0;

    for (const file of files.values())
      await file.close();
  }

  async destroy() {
    if (this.index !== 0)
      throw new Error('Store is opened.');

    const files = await this.readdir();

    for (const {path} of files)
      await this.fs.unlink(path);

    try {
      await this.fs.rmdir(this.prefix);
    } catch (e) {
      if (e.code === 'ENOTEMPTY' || e.errno === -39) {
        const path = randomPath(this.prefix);
        return this.rename(path);
      }
      throw e;
    }

    return undefined;
  }

  async rename(prefix) {
    assert(typeof prefix === 'string');

    if (this.index !== 0)
      throw new Error('Store is opened.');

    await this.fs.rename(this.prefix, prefix);

    this.prefix = prefix;
  }

  async openFile(index, flags) {
    assert((index >>> 0) === index);
    assert(typeof flags === 'string');

    if (this.index === 0)
      throw new Error('Store is closed.');

    if (index === 0 || index > this.index + 1)
      throw new Error('Invalid file index.');

    const file = this.files.get(index);

    if (file)
      return file;

    return this.reallyOpen(index, flags);
  }

  async reallyOpen(index, flags) {
    const unlock = await this.openLock(index);
    try {
      return await this._reallyOpen(index, flags);
    } finally {
      unlock();
    }
  }

  async _reallyOpen(index, flags) {
    if (this.index === 0)
      throw new Error('Store is closed.');

    const cache = this.files.get(index);

    if (cache)
      return cache;

    const file = new File(this.fs, index);
    const path = this.path(index);

    await file.open(path, flags);

    // No race conditions.
    assert(!this.files.has(index));

    if (this.files.size >= this.maxOpenFiles)
      await this.evict();

    this.files.set(index, file);

    return file;
  }

  openFileSync(index, flags) {
    assert((index >>> 0) === index);
    assert(typeof flags === 'string');

    if (this.index === 0)
      throw new Error('Store is closed.');

    if (index === 0 || index > this.index + 1)
      throw new Error('Invalid file index.');

    const cache = this.files.get(index);

    if (cache)
      return cache;

    const file = new File(this.fs, index);
    const path = this.path(index);

    file.openSync(path, flags);

    if (this.files.size >= this.maxOpenFiles)
      this.evictSync();

    this.files.set(index, file);

    return file;
  }

  async closeFile(index) {
    assert((index >>> 0) === index);

    if (this.index === 0)
      throw new Error('Store is closed.');

    const file = this.files.get(index);

    if (!file)
      return undefined;

    if (file.reads > 0)
      return undefined;

    this.files.delete(index);

    return file.close();
  }

  closeFileSync(index) {
    assert((index >>> 0) === index);

    if (this.index === 0)
      throw new Error('Store is closed.');

    const file = this.files.get(index);

    if (!file)
      return;

    if (file.reads > 0)
      return;

    this.files.delete(index);

    file.closeSync();
  }

  evictIndex() {
    if (this.index === 0)
      throw new Error('Store is closed.');

    let total = 0;

    for (const [index, file] of this.files) {
      if (index === this.index)
        continue;

      if (file.reads > 0)
        continue;

      total += 1;
    }

    if (total === 0)
      return -1;

    let i = Math.random() * total | 0;

    for (const [index, file] of this.files) {
      if (index === this.index)
        continue;

      if (file.reads > 0)
        continue;

      if (i === 0)
        return index;

      i -= 1;
    }

    throw new AssertionError('Eviction index not found.');
  }

  async evict() {
    const index = this.evictIndex();

    if (index === -1)
      return undefined;

    return this.closeFile(index);
  }

  evictSync() {
    const index = this.evictIndex();

    if (index === -1)
      return;

    this.closeFileSync(index);
  }

  async read(index, pos, size) {
    if (this.index === 0)
      throw new Error('Store is closed.');

    const file = await this.openFile(index, 'r');

    return file.read(pos, size);
  }

  readSync(index, pos, size) {
    if (this.index === 0)
      throw new Error('Store is closed.');

    const file = this.openFileSync(index, 'r');

    return file.readSync(pos, size);
  }

  async write(data) {
    if (this.index === 0)
      throw new Error('Store is closed.');

    if (this.current.size + data.length > MAX_FILE_SIZE) {
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

  start() {
    assert(this.buffer.written === 0);
    assert(this.buffer.start === 0);
    this.buffer.offset = this.current.size;
    this.buffer.index = this.current.index;
    return this;
  }

  needsFlush() {
    return this.buffer.written >= this.maxBufferSize;
  }

  async flush() {
    for (const chunk of this.buffer.flush())
      await this.write(chunk);

    this.start();
  }
}

/**
 * Write Buffer
 */

class WriteBuffer {
  constructor() {
    // Start position in file.
    this.offset = 0;

    // Current file index.
    this.index = 0;

    // Where the current file starts in memory.
    this.start = 0;

    // Current buffer position.
    this.written = 0;

    this.chunks = [];
    this.data = EMPTY;
  }

  reset() {
    this.offset = 0;
    this.index = 0;
    this.start = 0;
    this.written = 0;
    this.chunks = [];
  }

  get pos() {
    return this.position(this.written);
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

  pad(size) {
    this.data.fill(0x00, this.written, this.written + size);
    this.written += size;
  }

  render() {
    return this.data.slice(this.start, this.written);
  }

  flush() {
    const chunks = this.chunks;

    if (this.written > this.start)
      chunks.push(this.render());

    this.reset();

    return chunks;
  }
}

/**
 * File Map
 * Notion: a sparse array is faster than a hash table.
 */

class FileMap {
  constructor(maxFiles) {
    this.maxFiles = maxFiles;
    this.items = [];
    this.size = 0;
  }

  has(index) {
    return this.get(index) !== null;
  }

  get(index) {
    assert(index < this.maxFiles);

    if (index >= this.items.length)
      return null;

    const file = this.items[index];

    if (!file)
      return null;

    return file;
  }

  set(index, file) {
    assert(index < this.maxFiles);

    while (index >= this.items.length)
      this.items.push(null);

    if (!this.items[index])
      this.size += 1;

    this.items[index] = file;

    return this;
  }

  delete(index) {
    assert(index < this.maxFiles);

    if (index >= this.items.length)
      return false;

    if (this.items[index]) {
      this.items[index] = null;
      this.size -= 1;
      return true;
    }

    return false;
  }

  clear() {
    this.items.length = 0;
    this.size = 0;
  }

  [Symbol.iterator]() {
    return this.entries();
  }

  *entries() {
    for (let i = 0; i < this.items.length; i++) {
      const file = this.items[i];

      if (file)
        yield [i, file];
    }
  }

  *keys() {
    for (let i = 0; i < this.items.length; i++) {
      const file = this.items[i];

      if (file)
        yield i;
    }
  }

  *values() {
    for (let i = 0; i < this.items.length; i++) {
      const file = this.items[i];

      if (file)
        yield file;
    }
  }
}

/*
 * Expose
 */

module.exports = BaseStore;
