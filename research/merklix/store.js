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
  EMPTY,
  randomPath
} = common;

const {
  decodeNode,
  NIL,
  Internal
} = nodes;

/*
 * Constants
 */

const MAX_FILE_SIZE = 0x7ffff000;
const MAX_FILES = 0xffff;
const MAX_OPEN_FILES = 32;
const META_SIZE = 4 + 2 + 4 + 2 + 4 + 20;
const META_MAGIC = 0x6d6b6c78; // "mklx"
const READ_BUFFER = 1 << 20;
const SLAB_SIZE = READ_BUFFER - (READ_BUFFER % META_SIZE);

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
    this.state = new Meta();
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
      const stat = await fs.lstat(path);

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

      if (i > 0 && file.index !== files[i].index + 1)
        throw new Error('Missing merklix tree files.');
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

    await fs.mkdirp(this.prefix, 0o770);

    const files = await this.readdir();

    if (files.length === 0)
      return 0;

    return files[files.length - 1].index;
  }

  async open() {
    if (this.index !== 0)
      throw new Error('Store already opened.');

    const index = await this.ensure();

    // const state = await this.recoverState(index);
    // this.state = state;
    // this.index = state.metaIndex;

    this.index = index || 1;
    this.current = await this.openFile(this.index, 'a+');
  }

  async close() {
    if (this.index === 0)
      throw new Error('Store already closed.');

    const files = this.files;

    this.files = [];
    this.current = null;
    this.state = new Meta();
    this.index = 0;
    this.total = 0;

    for (const file of files)
      await file.close();
  }

  async destroy() {
    if (this.index !== 0)
      throw new Error('Store is opened.');

    const files = await this.readdir();

    for (const {path} of files)
      await fs.unlink(path);

    try {
      await fs.rmdir(this.prefix);
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

      await file.open(name, flags);

      // Handle race condition.
      if (this.files[index]) {
        await file.close();
        return this.files[index];
      }

      if (this.total >= MAX_OPEN_FILES)
        this.evict();

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
      return undefined;

    this.files[index] = null;
    this.total -= 1;

    return file.close();
  }

  async unlinkFile(index) {
    assert((index >>> 0) === index);
    assert(index !== this.index);

    if (this.index === 0)
      throw new Error('Store is closed.');

    await this.closeFile(index);

    return fs.unlink(this.name(index));
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

    let total = 0;

    for (const file of this.files) {
      if (!file)
        continue;

      if (file.index === this.current.index)
        continue;

      if (file.reads > 0)
        continue;

      total += 1;
    }

    if (total === 0)
      return false;

    let i = Math.random() * total | 0;

    for (const file of this.files) {
      if (!file)
        continue;

      if (file.index === this.current.index)
        continue;

      if (file.reads > 0)
        continue;

      if (i === 0) {
        i = file.index;
        break;
      }

      i -= 1;
    }

    const file = this.files[i];

    this.files[file.index] = null;
    this.total -= 1;

    file.closeSync();

    return true;
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

  start() {
    assert(this.wb.written === 0);
    assert(this.wb.start === 0);
    this.wb.offset = this.current.pos;
    this.wb.index = this.current.index;
    return this;
  }

  writeNull() {
    this.wb.expand(this.nodeSize);
    this.wb.pad(this.nodeSize);
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

  async commit() {
    this.writeMeta();
    return this.flush();
  }

  writeMeta() {
    if (this.wb.pos < this.nodeSize)
      return;

    this.state.rootIndex = this.wb.index;
    this.state.rootPos = this.wb.pos - this.nodeSize;

    const padding = META_SIZE - (this.wb.pos % META_SIZE);

    this.wb.expand(padding + META_SIZE);
    this.wb.pad(padding);

    this.state.write(
      this.wb.data,
      this.wb.written,
      this.hash
    );

    this.wb.written += META_SIZE;

    this.state.metaIndex = this.wb.index;
    this.state.metaPos = this.wb.pos - META_SIZE;
  }

  parseMeta(data, off) {
    try {
      return Meta.read(data, off, this.hash);
    } catch (e) {
      return null;
    }
  }

  async recoverState(index) {
    assert((index >>> 0) === index);

    if (this.index !== 0)
      throw new Error('Store is open.');

    let slab = null;

    while (index >= 1) {
      const name = this.name(index);
      const file = new File(index);

      await file.open(name, 'r+');

      let off = file.pos - (file.pos % META_SIZE);

      if (!slab)
        slab = Buffer.allocUnsafe(SLAB_SIZE);

      while (off >= META_SIZE) {
        let pos = 0;
        let size = off;

        if (off >= slab.length) {
          pos = off - slab.length;
          size = slab.length;
        }

        const data = await file.rawRead(pos, size, slab);

        while (size >= META_SIZE) {
          size -= META_SIZE;
          off -= META_SIZE;

          if (data.readUInt32LE(size, true) !== META_MAGIC)
            continue;

          const meta = this.parseMeta(data, size);

          if (meta) {
            await file.truncate(off + META_SIZE);
            await file.close();

            meta.metaIndex = index;
            meta.metaPos = off;

            return meta;
          }
        }
      }

      await file.close();
      await fs.unlink(name);

      index -= 1;
    }

    return new Meta(1, 0, 1, 0);
  }

  async readMeta(index, pos) {
    const data = await this.read(index, pos, META_SIZE);
    return Meta.decode(data, this.hash);
  }

  async readRoot(rootHash) {
    if (this.index === 0)
      throw new Error('Store is closed.');

    assert(!rootHash || Buffer.isBuffer(rootHash));

    if (rootHash && rootHash.equals(this.hash.zero))
      return NIL;

    let {metaIndex, metaPos} = this.state;

    for (;;) {
      if (metaIndex === 1 && metaPos === 0) {
        if (rootHash)
          throw new Error('Root not found.');

        return NIL;
      }

      const meta = await this.readMeta(metaIndex, metaPos);
      const {rootIndex, rootPos} = meta;

      const node = await this.readNode(rootIndex, rootPos);

      if (!rootHash)
        return node;

      const hash = node.hash(this.hash);

      if (hash.equals(rootHash))
        return node;

      metaIndex = meta.metaIndex;
      metaPos = meta.metaPos;
    }
  }
}

/**
 * Meta
 */

class Meta {
  constructor(metaIndex, metaPos, rootIndex, rootPos) {
    this.metaIndex = metaIndex || 1;
    this.metaPos = metaPos || 0;
    this.rootIndex = rootIndex || 1;
    this.rootPos = rootPos || 0;
  }

  getSize() {
    return this.constructor.getSize();
  }

  encode(hash, padding = 0) {
    assert((padding >>> 0) === padding);

    const data = Buffer.allocUnsafe(padding + META_SIZE);

    data.fill(0x00, 0, padding);

    this.write(data, padding, hash);

    return data;
  }

  write(data, off, hash) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);
    assert(hash && typeof hash.digest === 'function');
    assert(off + META_SIZE <= data.length);

    data.writeUInt32LE(META_MAGIC, off + 0, true);
    data.writeUInt16LE(this.metaIndex, off + 4, true);
    data.writeUInt32LE(this.metaPos, off + 6, true);
    data.writeUInt16LE(this.rootIndex, off + 10, true);
    data.writeUInt32LE(this.rootPos, off + 12, true);

    const preimage = data.slice(off, off + 16);
    const digest = hash.digest(preimage);

    assert(digest.length >= 20);

    digest.copy(data, off + 16, 0, 20);

    return off + META_SIZE;
  }

  decode(data, hash) {
    assert(Buffer.isBuffer(data));
    assert(data.length === META_SIZE);
    return this.read(data, 0, hash);
  }

  read(data, off, hash) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);
    assert(hash && typeof hash.digest === 'function');
    assert(off + META_SIZE <= data.length);

    const magic = data.readUInt32LE(off + 0, true);

    if (magic !== META_MAGIC)
      throw new Error('Invalid magic number.');

    const preimage = data.slice(off + 0, off + 16);
    const checksum = data.slice(off + 16, off + 16 + 20);
    const digest = hash.digest(preimage);

    assert(digest.length >= 20);

    const expect = digest.slice(0, 20);

    if (!checksum.equals(expect))
      throw new Error('Invalid metadata checksum.');

    this.metaIndex = data.readUInt16LE(off + 4, true);
    this.metaPos = data.readUInt32LE(off + 6, true);
    this.rootIndex = data.readUInt16LE(off + 10, true);
    this.rootPos = data.readUInt32LE(off + 12, true);

    return this;
  }

  static read(data, off, hash) {
    return new this().read(data, off, hash);
  }

  static decode(data, hash) {
    return new this().decode(data, hash);
  }

  static getSize() {
    return META_SIZE;
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

  pad(size) {
    this.data.fill(0x00, this.written, this.written + size);
    this.written += size;
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

  writeNull() {
    this.expand(this.nodeSize);
    this.pad(this.nodeSize);
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

  async commit() {}
}

/*
 * Expose
 */

exports.FileStore = FileStore;
exports.WriteBuffer = WriteBuffer;
exports.MemoryStore = MemoryStore;
