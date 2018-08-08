/*!
 * store.js - tree storage
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('bsert');
const Path = require('path');
const fs = require('bfile');
const {Lock, MapLock} = require('bmutex');
const common = require('./common');
const errors = require('./errors');
const File = require('./file');
const LockFile = require('./lockfile');
const MFS = require('./mfs');
const nodes = require('./nodes');

const {
  parseU32,
  serializeU32,
  EMPTY,
  randomPath,
  readU32,
  writeU32,
  hashValue,
  randomBytes,
  checksum
} = common;

const {
  AssertionError,
  MissingNodeError
} = errors;

const {
  Pointer,
  NIL,
  decodeNode
} = nodes;

/*
 * Constants
 */

// Max read size on linux, and lower than off_t max
// (libuv will use a 32 bit off_t on 32 bit systems).
const MAX_FILE_SIZE = 0x7ffff000; // File max = 2 GB
const MAX_FILES = 0x7fff; // DB max = 64 TB.
// const MAX_FILES = 0x7fffff; // DB max = 16 PB.
// const MAX_FILES = 0x7fffffff; // DB max = 4 EB.
const MAX_OPEN_FILES = 32;
const META_SIZE = 4 + (Pointer.getSize() * 2) + 20;
const MAX_WRITE = 1024 + 0xffff + 1024 + META_SIZE;
const META_MAGIC = 0x6d726b6c;
const WRITE_BUFFER = 64 << 20;
const READ_BUFFER = 1 << 20;
const SLAB_SIZE = READ_BUFFER - (READ_BUFFER % META_SIZE);
const KEY_SIZE = 32;
const ZERO_KEY = Buffer.alloc(KEY_SIZE, 0x00);

/**
 * Store
 */

class Store {
  constructor(fs, prefix, hash, bits) {
    assert(fs && typeof fs.write === 'function');
    assert(typeof prefix === 'string');
    assert(hash && typeof hash.digest === 'function');
    assert((bits >>> 0) === bits);
    assert(bits > 0 && (bits & 7) === 0);

    this.fs = fs;
    this.prefix = prefix;
    this.hash = hash;
    this.bits = bits;

    this.lockFile = new LockFile(fs, prefix);
    this.openLock = new MapLock();
    this.readLock = new Lock();
    this.buffer = new WriteBuffer();
    this.files = new FileMap();
    this.current = null;
    this.state = new Meta();
    this.index = 0;
    this.rootCache = new Map();
    this.lastMeta = new Meta();
    this.key = ZERO_KEY;
  }

  clone(prefix) {
    const {hash, bits} = this;
    return new this.constructor(prefix, hash, bits);
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

      if (index > MAX_FILES)
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
        throw new Error('Missing tree files.');
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

  async ensureKey() {
    const path = Path.resolve(this.prefix, 'meta');
    const file = new File(this.fs, 0);

    await file.open(path, 'a+');

    try {
      if (file.size !== 0 && file.size < KEY_SIZE)
        throw new Error('Tree meta file corruption.');

      if (file.size === 0) {
        const key = randomBytes(KEY_SIZE);
        await file.write(key);
        await file.sync();
        this.key = key;
      } else {
        this.key = await file.read(0, KEY_SIZE);
      }
    } finally {
      await file.close();
    }
  }

  async ensure() {
    if (this.index !== 0)
      throw new Error('Store already opened.');

    await this.fs.mkdirp(this.prefix, 0o750);
    await this.ensureKey();

    const files = await this.readdir();

    if (files.length === 0)
      return 0;

    return files[files.length - 1].index;
  }

  async open() {
    if (this.index !== 0)
      throw new Error('Store already opened.');

    const index = await this.ensure();
    const [state, meta] = await this.recoverState(index);

    await this.lockFile.open();

    this.state = state;
    this.index = state.metaPtr.index || 1;
    this.lastMeta = meta;
    this.current = await this.openFile(this.index, 'a+');
    this.start();
    this.state.rootNode = await this.getRoot();

    return this.state.rootNode;
  }

  async close() {
    if (this.index === 0)
      throw new Error('Store already closed.');

    const files = this.files;

    this.openLock.destroy();
    this.readLock.destroy();

    this.openLock = new MapLock();
    this.readLock = new Lock();
    this.buffer = new WriteBuffer();
    this.files = new FileMap();
    this.current = null;
    this.state = new Meta();
    this.index = 0;
    this.rootCache.clear();
    this.lastMeta = new Meta();
    this.key = ZERO_KEY;

    for (const file of files.values())
      await file.close();

    return this.lockFile.close();
  }

  async destroy() {
    if (this.index !== 0)
      throw new Error('Store is opened.');

    try {
      await this.fs.unlink(this.lockFile.file);
    } catch (e) {
      if (e.code !== 'ENOENT')
        throw e;
    }

    const meta = Path.resolve(this.prefix, 'meta');
    await this.fs.unlink(meta);

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
    this.lockFile.rename(prefix);
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
    const unlock = await this.openLock.lock(index);
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

    if (this.files.size >= MAX_OPEN_FILES)
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

    if (this.files.size >= MAX_OPEN_FILES)
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

  decodeNode(data, ptr) {
    const node = decodeNode(data, this.hash, this.bits);
    node.ptr = ptr;
    return node;
  }

  async readNode(ptr) {
    const data = await this.read(ptr.index, ptr.pos, ptr.size);
    return this.decodeNode(data, ptr);
  }

  readNodeSync(ptr) {
    const data = this.readSync(ptr.index, ptr.pos, ptr.size);
    return this.decodeNode(data, ptr);
  }

  start() {
    assert(this.buffer.written === 0);
    assert(this.buffer.start === 0);
    this.buffer.offset = this.current.size;
    this.buffer.index = this.current.index;
    return this;
  }

  writeNode(node) {
    assert(node.isInternal() || node.isLeaf());
    assert(!node.ptr);

    const size = node.getSize(this.hash, this.bits);

    this.buffer.expand(size);

    const written = this.buffer.written;

    const off = node.write(
      this.buffer.data,
      this.buffer.written,
      this.hash,
      this.bits
    );

    assert(off - written === size);

    this.buffer.written += size;

    const pos = this.buffer.position(written);
    const index = this.buffer.index;

    node.mark(index, pos, size);

    return pos;
  }

  writeValue(node) {
    assert(node.isLeaf());
    assert(!node.ptr);
    assert(node.value);
    const size = node.value.length;
    const pos = this.buffer.write(node.value);
    const index = this.buffer.index;
    node.save(index, pos, size);
    return pos;
  }

  needsFlush() {
    return this.buffer.written >= WRITE_BUFFER - MAX_WRITE;
  }

  async flush() {
    for (const chunk of this.buffer.flush())
      await this.write(chunk);

    this.start();
  }

  async commit(root) {
    const state = this.writeMeta(root);
    const node = state.rootNode;

    await this.flush();
    await this.sync();

    this.state = state;

    if (!node.isNull()) {
      const key = node.data.toString('binary');
      this.rootCache.set(key, node);
    }
  }

  writeMeta(root) {
    assert(root);

    const state = this.state.clone();

    assert(root.ptr);
    state.rootPtr = root.ptr;
    state.rootNode = root.toHash(this.hash);

    const padding = META_SIZE - (this.buffer.pos % META_SIZE);

    this.buffer.expand(padding + META_SIZE);
    this.buffer.pad(padding);

    const off = state.write(
      this.buffer.data,
      this.buffer.written,
      this.hash,
      this.key
    );

    assert(off - this.buffer.written === META_SIZE);

    this.buffer.written += META_SIZE;

    state.metaPtr.index = this.buffer.index;
    state.metaPtr.pos = this.buffer.pos - META_SIZE;

    return state;
  }

  parseMeta(data, off) {
    try {
      return Meta.read(data, off, this.hash, this.key);
    } catch (e) {
      return null;
    }
  }

  async findMeta(file, slab) {
    assert(file instanceof File);
    assert(Buffer.isBuffer(slab));

    let off = file.size - (file.size % META_SIZE);

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

        if (readU32(data, size) !== META_MAGIC)
          continue;

        const meta = this.parseMeta(data, size);

        if (meta) {
          await file.truncate(off + META_SIZE);
          return [off, meta];
        }
      }
    }

    return [-1, null];
  }

  async recoverState(index) {
    assert((index >>> 0) === index);

    if (this.index !== 0)
      throw new Error('Store is open.');

    if (index === 0)
      return [new Meta(), new Meta()];

    const slab = Buffer.allocUnsafe(SLAB_SIZE);

    while (index >= 1) {
      const path = this.path(index);
      const file = new File(this.fs, index);

      let off = -1;
      let meta = null;

      await file.open(path, 'r+');

      try {
        [off, meta] = await this.findMeta(file, slab);
      } finally {
        await file.close();
      }

      if (meta) {
        const state = meta.clone();
        state.metaPtr.index = index;
        state.metaPtr.pos = off;

        return [state, meta];
      }

      await this.fs.unlink(path);

      index -= 1;
    }

    return [new Meta(), new Meta()];
  }

  async readMeta(ptr) {
    const data = await this.read(ptr.index, ptr.pos, META_SIZE);
    return Meta.decode(data, this.hash, this.key);
  }

  async getRoot() {
    if (this.index === 0)
      throw new Error('Store is closed.');

    if (this.state.rootNode)
      return this.state.rootNode;

    const {rootPtr} = this.state;

    return this.readRoot(rootPtr);
  }

  async readRoot(ptr) {
    if (ptr.index === 0)
      return NIL;

    const node = await this.readNode(ptr);

    // Edge case when our root is a leaf.
    // We need to recalculate the hash.
    if (node.isLeaf()) {
      const key = node.key;
      const value = await this.retrieve(node);

      node.data = hashValue(this.hash, key, value);
    }

    const root = node.toHash(this.hash);
    const key = root.data.toString('binary');

    if (!this.rootCache.has(key))
      this.rootCache.set(key, root);

    return root;
  }

  async getHistory(rootHash) {
    assert(Buffer.isBuffer(rootHash));

    if (this.index === 0)
      throw new Error('Store is closed.');

    if (rootHash.equals(this.hash.zero))
      return NIL;

    const root = this.state.rootNode;
    const hash = root.hash(this.hash);

    if (rootHash.equals(hash))
      return root;

    const key = rootHash.toString('binary');
    const cached = this.rootCache.get(key);

    if (cached)
      return cached;

    return this.readHistory(rootHash);
  }

  async readHistory(rootHash) {
    const unlock = await this.readLock.lock();
    try {
      return await this._readHistory(rootHash);
    } finally {
      unlock();
    }
  }

  async _readHistory(rootHash) {
    assert(Buffer.isBuffer(rootHash));

    if (this.index === 0)
      throw new Error('Store is closed.');

    const key = rootHash.toString('binary');
    const cached = this.rootCache.get(key);

    if (cached)
      return cached;

    let {metaPtr} = this.lastMeta;

    for (;;) {
      if (metaPtr.index === 0) {
        throw new MissingNodeError({
          rootHash: rootHash,
          nodeHash: rootHash
        });
      }

      const meta = await this.readMeta(metaPtr);
      const {rootPtr} = meta;
      const node = await this.readRoot(rootPtr);
      const hash = node.hash(this.hash);

      this.lastMeta = meta;

      if (hash.equals(rootHash))
        return node;

      metaPtr = meta.metaPtr;
    }
  }

  async resolve(node) {
    if (!node.isHash())
      return node;

    assert(node.ptr);
    const rn = await this.readNode(node.ptr);
    rn.data = node.data;
    return rn;
  }

  async retrieve(node) {
    assert(node.isLeaf());

    if (node.value)
      return node.value;

    assert(node.vptr);
    return this.read(node.vptr.index, node.vptr.pos, node.vptr.size);
  }
}

/**
 * Meta
 */

class Meta {
  constructor() {
    this.metaPtr = new Pointer();
    this.rootPtr = new Pointer();
    this.rootNode = null;
  }

  clone() {
    const meta = new this.constructor();
    meta.metaPtr = this.metaPtr.clone();
    meta.rootPtr = this.rootPtr.clone();
    meta.rootNode = this.rootNode;
    return meta;
  }

  getSize() {
    return this.constructor.getSize();
  }

  encode(padding = 0, hash, key) {
    assert((padding >>> 0) === padding);

    const data = Buffer.allocUnsafe(padding + META_SIZE);

    data.fill(0x00, 0, padding);

    this.write(data, padding, hash, key);

    return data;
  }

  write(data, off, hash, key) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);
    assert(off + META_SIZE <= data.length);
    assert(hash && typeof hash.multi === 'function');
    assert(Buffer.isBuffer(key));
    assert(key.length === KEY_SIZE);

    const start = off;

    off = writeU32(data, META_MAGIC, off);
    off = this.metaPtr.write(data, off);
    off = this.rootPtr.write(data, off);

    const preimage = data.slice(start, off);
    const chk = checksum(hash, preimage, key);

    off += chk.copy(data, off, 0, 20);

    assert((off - start) === META_SIZE);

    return off;
  }

  decode(data, hash, key) {
    assert(Buffer.isBuffer(data));
    assert(data.length === META_SIZE);
    return this.read(data, 0, hash, key);
  }

  read(data, off, hash, key) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);
    assert(off + META_SIZE <= data.length);
    assert(hash && typeof hash.multi === 'function');
    assert(Buffer.isBuffer(key));
    assert(key.length === KEY_SIZE);

    const start = off;
    const magic = readU32(data, off);
    off += 4;

    if (magic !== META_MAGIC)
      throw new Error('Invalid magic number.');

    this.metaPtr = Pointer.read(data, off);
    off += this.metaPtr.getSize();

    this.rootPtr = Pointer.read(data, off);
    off += this.rootPtr.getSize();

    const preimage = data.slice(start, off);
    const expect = checksum(hash, preimage, key);

    const chk = data.slice(off, off + 20);

    if (!chk.equals(expect))
      throw new Error('Invalid metadata checksum.');

    return this;
  }

  static read(data, off, hash, key) {
    return new this().read(data, off, hash, key);
  }

  static decode(data, hash, key) {
    return new this().decode(data, hash, key);
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
  constructor() {
    this.items = [];
    this.size = 0;
  }

  has(index) {
    return this.get(index) !== null;
  }

  get(index) {
    assert(index < MAX_FILES);

    if (index >= this.items.length)
      return null;

    const file = this.items[index];

    if (!file)
      return null;

    return file;
  }

  set(index, file) {
    assert(index < MAX_FILES);

    while (index >= this.items.length)
      this.items.push(null);

    if (!this.items[index])
      this.size += 1;

    this.items[index] = file;

    return this;
  }

  delete(index) {
    assert(index < MAX_FILES);

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

/**
 * File Store
 */

class FileStore extends Store {
  constructor(prefix, hash, bits) {
    super(fs, prefix, hash, bits);
  }
}

/**
 * Memory Store
 */

class MemoryStore extends Store {
  constructor(prefix, hash, bits) {
    super(new MFS(), prefix, hash, bits);
  }
}

/*
 * Expose
 */

exports.FileStore = FileStore;
exports.MemoryStore = MemoryStore;
