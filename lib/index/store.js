/*!
 * store.js - tree storage
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('assert');
const fs = require('bfile');
const common = require('../common');
const File = require('../file');
const MFS = require('../mfs');
const BaseStore = require('./base-store');
const nodes = require('./nodes');

const {
  readU32,
  writeU32,
  hashPerfect
} = common;

const {
  decodeNode,
  NIL,
  LEAF_SIZE,
  INTERNAL_SIZE
} = nodes;

/*
 * Constants
 */

const META_SIZE = (4 + 4 + 4) * 2;
const META_MAGIC = 0x5c615a5b;
const READ_BUFFER = 1 << 20;
const SLAB_SIZE = READ_BUFFER - (READ_BUFFER % META_SIZE);

/**
 * Store
 */

class Store extends BaseStore {
  constructor(fs, prefix) {
    super(fs, prefix);

    this.state = new Meta();
  }

  async open() {
    if (this.index !== 0)
      throw new Error('Store already opened.');

    const index = await this.ensure();
    const [i, meta] = await this.recoverState(index);

    this.state = meta;
    this.index = i || 1;
    this.current = await this.openFile(this.index, 'a+');
    this.start();
    this.state.rootNode = await this.getRoot();

    return this.state.rootNode;
  }

  async close() {
    if (this.index === 0)
      throw new Error('Store already closed.');

    this.state = new Meta();

    return super.close();
  }

  async readNode(index, pos, leaf) {
    const size = leaf ? LEAF_SIZE : INTERNAL_SIZE;
    const data = await this.read(index, pos, size);
    return decodeNode(data, index, pos);
  }

  writeNode(node) {
    assert(node.index === 0);

    this.wb.expand(node.getSize());

    const written = this.wb.written;

    node.write(
      this.wb.data,
      this.wb.written
    );

    this.wb.written += node.getSize();

    node.pos = this.wb.position(written);
    node.index = this.wb.index;

    return node.pos;
  }

  async commit(root) {
    const state = this.writeMeta(root);

    await this.flush();
    await this.sync();

    this.state = state;
  }

  writeMeta(root) {
    const state = this.state.clone();

    state.rootIndex = root.index;
    state.rootPos = root.pos;
    state.rootLeaf = root.hasLeaf();
    state.rootNode = root.toPosition();

    const padding = META_SIZE - (this.wb.pos % META_SIZE);

    this.wb.expand(padding + META_SIZE);
    this.wb.pad(padding);

    state.write(
      this.wb.data,
      this.wb.written
    );

    this.wb.written += META_SIZE;

    return state;
  }

  parseMeta(data, off) {
    try {
      return Meta.read(data, off);
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
          return meta;
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
      return [0, new Meta()];

    const slab = Buffer.allocUnsafe(SLAB_SIZE);

    while (index >= 1) {
      const path = this.path(index);
      const file = new File(this.fs, index);

      let meta = null;

      await file.open(path, 'r+');

      try {
        meta = await this.findMeta(file, slab);
      } finally {
        await file.close();
      }

      if (meta)
        return [index, meta];

      await this.fs.unlink(path);

      index -= 1;
    }

    return [0, new Meta()];
  }

  async readMeta(index, pos) {
    const data = await this.read(index, pos, META_SIZE);
    return Meta.decode(data);
  }

  async getRoot() {
    if (this.index === 0)
      throw new Error('Store is closed.');

    if (this.state.rootNode)
      return this.state.rootNode;

    if (this.state.rootIndex === 0)
      return NIL;

    const node = await this.readNode(
      this.state.rootIndex,
      this.state.rootPos,
      this.state.rootLeaf
    );

    return node.toPosition();
  }
}

/**
 * Meta
 */

class Meta {
  constructor() {
    this.rootIndex = 0;
    this.rootPos = 0;
    this.rootLeaf = 0;
    this.rootNode = null;
  }

  clone() {
    const meta = new this.constructor();
    meta.rootIndex = this.rootIndex;
    meta.rootPos = this.rootPos;
    meta.rootNode = this.rootNode;
    return meta;
  }

  encode(padding = 0) {
    assert((padding >>> 0) === padding);

    const data = Buffer.allocUnsafe(padding + META_SIZE);

    data.fill(0x00, 0, padding);

    this.write(data, padding);

    return data;
  }

  write(data, off) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);
    assert(off + META_SIZE <= data.length);

    const rootPos = this.rootPos * 2 + this.rootLeaf;

    writeU32(data, META_MAGIC, off + 0, true);
    writeU32(data, this.rootIndex, off + 4, true);
    writeU32(data, rootPos, off + 8, true);

    const preimage = data.slice(off, off + 12);
    const checksum = hashPerfect(preimage);

    checksum.copy(data, off + 12, 0, 12);

    return off + META_SIZE;
  }

  decode(data) {
    assert(Buffer.isBuffer(data));
    assert(data.length === META_SIZE);
    return this.read(data, 0);
  }

  read(data, off) {
    assert(Buffer.isBuffer(data));
    assert((off >>> 0) === off);
    assert(off + META_SIZE <= data.length);

    const magic = readU32(data, off + 0);

    if (magic !== META_MAGIC)
      throw new Error('Invalid magic number.');

    const preimage = data.slice(off + 0, off + 12);
    const checksum = data.slice(off + 12, off + 12 + 12);
    const expect = hashPerfect(preimage);

    if (!checksum.equals(expect))
      throw new Error('Invalid metadata checksum.');

    this.rootIndex = readU32(data, off + 4);
    this.rootPos = readU32(data, off + 8);
    this.rootLeaf = this.rootPos & 1;
    this.rootPos >>>= 1;

    return this;
  }

  static read(data, off) {
    return new this().read(data, off);
  }

  static decode(data) {
    return new this().decode(data);
  }
}

/**
 * File Store
 */

class FileStore extends Store {
  constructor(prefix) {
    super(fs, prefix);
  }
}

/**
 * Memory Store
 */

class MemoryStore extends Store {
  constructor(prefix) {
    super(new MFS(), prefix);
  }
}

/*
 * Expose
 */

exports.FileStore = FileStore;
exports.MemoryStore = MemoryStore;
