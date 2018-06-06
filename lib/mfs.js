/*!
 * mfs.js - in-memory file system
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const Path = require('path');
const os = require('os');

/*
 * Constants
 */

const EMPTY = Buffer.alloc(0);

const info = os.userInfo
  ? os.userInfo()
  : { uid: 0, gid: 0 };

/**
 * MFS
 */

class MFS {
  constructor() {
    this.files = new Map();
    this.fds = new Map();
    this.fd = 3;
    this.inode = 1;
    this.cwd = '/';
    this.root = new Directory(0o777, 0);
  }

  _find(path) {
    const names = parsePath(this.cwd, path);

    if (names.length === 0)
      return [this.root, null, ''];

    const name = names.pop();

    let node = this.root;

    for (const name of names) {
      node = node.map.get(name);

      if (!node)
        throw new Error('ENOENT');

      if (!node.isDirectory())
        throw new Error('ENOENT');
    }

    const file = node.map.get(name);

    return [file || null, node, name];
  }

  openSync(path, flags, mode) {
    if (mode == null)
      mode = 0o666;

    assert(typeof flags === 'string');
    assert((mode & 0xffff) === mode);

    let [file, parent, name] = this._find(path);

    switch (flags) {
      case 'ax':
      case 'wx':
      case 'ax+':
      case 'wx+':
        if (file)
          throw new Error('EEXIST');
        break;
      case 'r':
      case 'r+':
      case 'rs+':
        if (!file)
          throw new Error('ENOENT');
        break;
    }

    if (!file) {
      assert(parent);

      file = new RegularFile(mode, this.inode);
      this.inode += 1;

      parent.map.set(name, file);
    }

    if (file.isDirectory())
      throw new Error('EISDIR');

    switch (flags) {
      case 'w':
      case 'wx':
      case 'w+':
      case 'wx+':
        file.truncate(0);
        break;
    }

    const item = new FD(file, path, flags);

    this.fds.set(this.fd, item);
    this.fd += 1;

    return this.fd - 1;
  }

  async open(path, flags, mode) {
    return this.openSync(path, flags, mode);
  }

  closeSync(fd) {
    assert((fd >>> 0) === fd);

    if (!this.fds.has(fd))
      throw new Error('EBADF');

    this.fds.delete(fd);
  }

  async close(fd) {
    return this.closeSync(fd);
  }

  readdirSync(path) {
    const [file] = this._find(path);

    if (!file.isDirectory())
      throw new Error('ENOTDIR');

    return file.read();
  }

  async readdir(path) {
    return this.readdirSync(path);
  }

  mkdirSync(path, mode) {
    if (mode == null)
      mode = 0o777;

    const [file, parent, name] = this._find(path);

    if (file)
      throw new Error('EEXISTS');

    if (!parent)
      throw new Error('ENOENT');

    const dir = new Directory(mode, this.inode);
    this.inode += 1;

    parent.map.set(name, dir);
  }

  async mkdir(path, mode) {
    return this.mkdirpSync(path);
  }

  mkdirpSync(path, mode) {
    if (mode == null)
      mode = 0o777;

    const names = parsePath(this.cwd, path);

    if (names.length === 0)
      return;

    const name = names.pop();

    let parent = null;
    let node = this.root;

    for (const name of names) {
      parent = node;
      node = node.map.get(name);

      if (!node) {
        node = new Directory(mode, this.inode);
        this.inode += 1;
        parent.map.set(name, node);
      }

      if (!node.isDirectory())
        throw new Error('ENOENT');
    }

    const file = node.map.get(name);

    if (file) {
      if (!file.isDirectory())
        throw new Error('EISNOTDIR');
      return;
    }

    const dir = new Directory(mode, this.inode);
    this.inode += 1;

    node.map.set(name, dir);
  }

  async mkdirp(path, mode) {
    return this.mkdirpSync(path);
  }

  rmdirSync(path) {
    const [file, parent, name] = this._find(path);

    if (!file)
      throw new Error('ENOENT');

    if (!file.isDirectory())
      throw new Error('ENOTDIR');

    if (file.map.size > 0)
      throw new Error('ENOTEMPTY');

    if (!parent) {
      this.root = new Directory(0o777, 0);
      return;
    }

    parent.map.delete(name);
  }

  async rmdir(path) {
    return this.rmdirSync(path);
  }

  renameSync(path, to) {
    const [file1, parent1, name1] = this._find(path);

    if (!file1 || !parent1)
      throw new Error('ENOENT');

    const [file2, parent2, name2] = this._find(to);

    if (!parent2)
      throw new Error('ENOENT');

    if (file2)
      throw new Error('EEXISTS');

    parent1.map.delete(name1);
    parent2.map.set(name2, file1);
  }

  async rename(path, to) {
    return this.renameSync(path, to);
  }

  statSync(path) {
    const [file] = this._find(path);

    if (!file)
      throw new Error('ENOENT');

    return file.stat();
  }

  async stat(path) {
    return this.statSync(path);
  }

  lstatSync(path) {
    return this.statSync(path);
  }

  async lstat(path) {
    return this.statSync(path);
  }

  unlinkSync(path) {
    const [file, parent, name] = this._find(path);

    if (!file || !parent)
      throw new Error('ENOENT');

    if (file.isDirectory())
      throw new Error('EISDIR');

    parent.map.delete(name);
  }

  async unlink(path) {
    return this.unlinkSync(path);
  }

  fstatSync(fd) {
    assert((fd >>> 0) === fd);

    const item = this.fds.get(fd);

    if (!item)
      throw new Error('EBADF');

    const {file} = item;

    return file.stat();
  }

  async fstat(fd) {
    return this.fstatSync(fd);
  }

  ftruncateSync(fd, size) {
    assert((fd >>> 0) === fd);

    const item = this.fds.get(fd);

    if (!item)
      throw new Error('EBADF');

    if (!item.writable)
      throw new Error('EBADF');

    const {file} = item;

    file.truncate(size);
  }

  async ftruncate(fd, size) {
    return this.ftruncateSync(fd, size);
  }

  fsyncSync(fd) {
    assert((fd >>> 0) === fd);

    const item = this.fds.get(fd);

    if (!item)
      throw new Error('EBADF');

    if (!item.writable)
      throw new Error('EBADF');
  }

  async fsync(fd) {
    return this.fsyncSync(fd);
  }

  readSync(fd, data, offset, length, position) {
    assert((fd >>> 0) === fd);

    const item = this.fds.get(fd);

    if (!item)
      throw new Error('EBADF');

    if (!item.readable)
      throw new Error('EBADF');

    const {file} = item;

    return file.read(data, offset, length, position);
  }

  async read(fd, data, offset, length, position) {
    return this.readSync(fd, data, offset, length, position);
  }

  writeSync(fd, data, offset, length, position) {
    assert((fd >>> 0) === fd);

    const item = this.fds.get(fd);

    if (!item)
      throw new Error('EBADF');

    if (!item.writable)
      throw new Error('EBADF');

    const {file} = item;

    return file.write(data, offset, length, position);
  }

  async write(fd, data, offset, length, position) {
    return this.writeSync(fd, data, offset, length, position);
  }
}

/**
 * File
 */

class File {
  constructor(mode, inode) {
    assert((mode & 0xffff) === mode);
    assert((inode >>> 0) === inode);

    this.mode = mode;
    this.inode = inode;
    this.ctime = Date.now();
    this.mtime = this.ctime;
  }

  stat() {
    return new Stats(this);
  }

  isFile() {
    return false;
  }

  isDirectory() {
    return false;
  }
}

/**
 * RegularFile
 */

class RegularFile extends File {
  constructor(mode, inode) {
    super(mode, inode);
    this.data = EMPTY;
    this.size = 0;
  }

  isFile() {
    return true;
  }

  truncate(size) {
    assert(size <= this.size);
    this.size = size;
  }

  expand(size) {
    if (this.data.length === 0)
      this.data = Buffer.allocUnsafe(8192);

    while (this.size + size > this.data.length) {
      const buf = Buffer.allocUnsafe(this.data.length * 2);
      this.data.copy(buf, 0);
      this.data = buf;
    }
  }

  read(data, offset, length, position) {
    const sourceStart = Math.min(position, this.size);
    const sourceEnd = Math.min(position + length, this.size);
    return this.data.copy(data, offset, sourceStart, sourceEnd);
  }

  write(data, offset, length, position) {
    this.expand(length);

    const pos = typeof position === 'number'
      ? position
      : this.size;

    const w = data.copy(this.data, pos, offset, offset + length);

    this.size += w;
    this.mtime = Date.now();

    return w;
  }
}

/**
 * Directory
 */

class Directory extends File {
  constructor(mode, inode) {
    super(mode, inode);
    this.map = new Map();
  }

  isDirectory() {
    return true;
  }

  read() {
    const names = [];
    for (const name of this.map.keys())
      names.push(name);
    return names;
  }
}

/**
 * Stats
 */

class Stats {
  constructor(file) {
    assert(file instanceof File);

    const blocks = Math.floor((file.size + 4095) / 4096);

    this.dev = 0;
    this.ino = file.inode;
    this.mode = file.mode;
    this.nlink = 1;
    this.uid = info.uid;
    this.gid = info.gid;
    this.rdev = 0;
    this.size = file.size;
    this.blksize = blocks * 4096;
    this.blocks = blocks;
    this.atimeMs = file.mtime;
    this.mtimeMs = file.mtime;
    this.ctimeMs = file.ctime;
    this.birthtimeMs = file.ctime;
    this.atime = Math.floor(file.mtime / 1000);
    this.mtime = Math.floor(file.mtime / 1000);
    this.ctime = Math.floor(file.ctime / 1000);
    this.birthtime = Math.floor(file.ctime / 1000);

    this._isFile = file.isFile();
  }

  isBlockDevice() {
    return false;
  }

  isCharacterDevice() {
    return false;
  }

  isDirectory() {
    return !this._isFile;
  }

  isFIFO() {
    return false;
  }

  isFile() {
    return this._isFile;
  }

  isSocket() {
    return false;
  }

  isSymbolicLink() {
    return false;
  }
}

/**
 * FD
 */

class FD {
  constructor(file, path, flags) {
    assert(file instanceof File);
    assert(typeof path === 'string');
    assert(typeof flags === 'string');

    this.file = file;
    this.path = path;
    this.flags = flags;
    this.readable = false;
    this.writable = false;

    switch (flags) {
      case 'a':
      case 'ax': // fails on exist
      case 'as':
      case 'w': // truncated
      case 'wx': // truncated, fails on exist
        this.writable = true;
        break;
      case 'a+':
      case 'ax+': // fails on exist
      case 'as+':
      case 'w+': // truncated
      case 'wx+': // truncated, fails on exist
        this.readable = true;
        this.writable = true;
        break;
      case 'r': // fails on no exist
        this.readable = true;
        break;
      case 'r+': // fails on no exist
      case 'rs+': // fails on no exist
        this.readable = true;
        this.writable = true;
        break;
      default:
        throw new Error('EBADFLAGS');
    }
  }
}

/*
 * Helpers
 */

function parsePath(cwd, path) {
  assert(typeof path === 'string');

  path = Path.resolve(cwd, path);
  path = Path.normalize(path);
  path = path.toLowerCase();

  path = path.replace(/\\/g, '/');
  path = path.replace(/(^|\/)\.\//, '$1');
  path = path.replace(/\/+\.?$/, '');

  const parts = path.split(/\/+/);

  if (parts.length > 0) {
    if (parts[0].length === 0)
      parts.shift();
  }

  if (parts.length > 0) {
    if (parts[0].length === 0)
      parts.shift();
  }

  return parts;
}

/*
 * Expose
 */

module.exports = MFS;
