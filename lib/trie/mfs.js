/*!
 * mfs.js - in-memory file system
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('bsert');
const path = require('path');
const os = require('os');
const Path = path.posix || path;

/*
 * Constants
 */

const EMPTY = Buffer.alloc(0);

const info = os.userInfo
  ? os.userInfo()
  : { uid: 0, gid: 0 };

const constants = {
  UV_FS_SYMLINK_DIR: 1,
  UV_FS_SYMLINK_JUNCTION: 2,
  O_RDONLY: 0,
  O_WRONLY: 1,
  O_RDWR: 2,
  S_IFMT: 61440,
  S_IFREG: 32768,
  S_IFDIR: 16384,
  S_IFCHR: 8192,
  S_IFBLK: 24576,
  S_IFIFO: 4096,
  S_IFLNK: 40960,
  S_IFSOCK: 49152,
  O_CREAT: 64,
  O_EXCL: 128,
  O_NOCTTY: 256,
  O_TRUNC: 512,
  O_APPEND: 1024,
  O_DIRECTORY: 65536,
  O_NOATIME: 262144,
  O_NOFOLLOW: 131072,
  O_SYNC: 1052672,
  O_DSYNC: 4096,
  O_DIRECT: 16384,
  O_NONBLOCK: 2048,
  S_IRWXU: 448,
  S_IRUSR: 256,
  S_IWUSR: 128,
  S_IXUSR: 64,
  S_IRWXG: 56,
  S_IRGRP: 32,
  S_IWGRP: 16,
  S_IXGRP: 8,
  S_IRWXO: 7,
  S_IROTH: 4,
  S_IWOTH: 2,
  S_IXOTH: 1,
  F_OK: 0,
  R_OK: 4,
  W_OK: 2,
  X_OK: 1,
  UV_FS_COPYFILE_EXCL: 1,
  COPYFILE_EXCL: 1,
  UV_FS_COPYFILE_FICLONE: 2,
  COPYFILE_FICLONE: 2,
  UV_FS_COPYFILE_FICLONE_FORCE: 4,
  COPYFILE_FICLONE_FORCE: 4
};

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
    this.constants = constants;
  }

  chdir(path) {
    assert(typeof path === 'string');
    this.cwd = Path.resolve(this.cwd, path);
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
        throw makeError('stat', 'ENOENT', path);

      if (!node.isDirectory())
        throw makeError('stat', 'ENOENT', path);
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
          throw makeError('open', 'EEXIST', path);
        break;
      case 'r':
      case 'r+':
      case 'rs+':
        if (!file)
          throw makeError('open', 'ENOENT', path);
        break;
    }

    if (!file) {
      assert(parent);

      file = new RegularFile(mode, this.inode);
      this.inode += 1;

      parent.map.set(name, file);
    }

    if (file.isDirectory())
      throw makeError('open', 'EISDIR', path);

    switch (flags) {
      case 'w':
      case 'wx':
      case 'w+':
      case 'wx+':
        file.mtime = Date.now();
        file.ctime = file.mtime;
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
      throw makeError('close', 'EBADF');

    this.fds.delete(fd);
  }

  async close(fd) {
    return this.closeSync(fd);
  }

  readdirSync(path) {
    const [file] = this._find(path);

    if (!file)
      throw makeError('readdir', 'ENOENT', path);

    if (!file.isDirectory())
      throw makeError('readdir', 'ENOTDIR', path);

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
      throw makeError('mkdir', 'EEXIST', path);

    if (!parent)
      throw makeError('mkdir', 'ENOENT', path);

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
        throw makeError('mkdir', 'ENOENT', path);
    }

    const file = node.map.get(name);

    if (file) {
      if (!file.isDirectory())
        throw makeError('mkdir', 'ENOTDIR', path);
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
      throw makeError('rmdir', 'ENOENT', path);

    if (!file.isDirectory())
      throw makeError('rmdir', 'ENOTDIR', path);

    if (file.map.size > 0)
      throw makeError('rmdir', 'ENOTEMPTY', path);

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
      throw makeError('rename', 'ENOENT', path);

    const [file2, parent2, name2] = this._find(to);

    if (!parent2)
      throw makeError('rename', 'ENOENT', path);

    if (file2)
      throw makeError('rename', 'EEXIST', path);

    file1.ctime = Date.now();

    parent1.map.delete(name1);
    parent2.map.set(name2, file1);
  }

  async rename(path, to) {
    return this.renameSync(path, to);
  }

  statSync(path) {
    const [file] = this._find(path);

    if (!file)
      throw makeError('stat', 'ENOENT', path);

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

  chmodSync(path, mode) {
    assert((mode >>> 0) === mode);

    const [file] = this._find(path);

    if (!file)
      throw makeError('chmod', 'ENOENT', path);

    file.mode = mode & 0o777;
  }

  async chmod(path, mode) {
    return this.chmodSync(path, mode);
  }

  chownSync(path, uid, gid) {
    assert((uid >>> 0) === uid);
    assert((gid >>> 0) === gid);

    const [file] = this._find(path);

    if (!file)
      throw makeError('chown', 'ENOENT', path);

    file.uid = uid;
    file.gid = gid;
  }

  async chown(path, mode) {
    return this.chownSync(path, mode);
  }

  unlinkSync(path) {
    const [file, parent, name] = this._find(path);

    if (!file || !parent)
      throw makeError('unlink', 'ENOENT', path);

    if (file.isDirectory())
      throw makeError('unlink', 'EISDIR', path);

    parent.map.delete(name);
  }

  async unlink(path) {
    return this.unlinkSync(path);
  }

  utimesSync(path, atime, mtime) {
    assert((atime >>> 0) === atime);
    assert((mtime >>> 0) === mtime);

    const [file] = this._find(path);

    if (!file)
      throw makeError('utimes', 'ENOENT', path);

    file.atime = atime;
    file.mtime = mtime;
    file.ctime = Date.now();
  }

  async utimes(path, atime, mtime) {
    return this.utimesSync(path, atime, mtime);
  }

  truncateSync(path, size) {
    const [file] = this._find(path);

    if (!file)
      throw makeError('truncate', 'ENOENT', path);

    file.mtime = Date.now();
    file.ctime = file.mtime;

    file.truncate(size);
  }

  async truncate(path, size) {
    return this.truncateSync(path, size);
  }

  futimesSync(fd, atime, mtime) {
    assert((fd >>> 0) === fd);
    assert((atime >>> 0) === atime);
    assert((mtime >>> 0) === mtime);

    const item = this.fds.get(fd);

    if (!item)
      throw makeError('futimes', 'EBADF');

    const {file} = item;

    file.atime = atime;
    file.mtime = mtime;
    file.ctime = Date.now();
  }

  async futimes(fd, atime, mtime) {
    return this.futimesSync(fd, atime, mtime);
  }

  fstatSync(fd) {
    assert((fd >>> 0) === fd);

    const item = this.fds.get(fd);

    if (!item)
      throw makeError('fstat', 'EBADF');

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
      throw makeError('ftruncate', 'EBADF');

    if (!item.writable)
      throw makeError('ftruncate', 'EBADF');

    const {file} = item;

    file.mtime = Date.now();
    file.ctime = file.mtime;

    file.truncate(size);
  }

  async ftruncate(fd, size) {
    return this.ftruncateSync(fd, size);
  }

  fsyncSync(fd) {
    assert((fd >>> 0) === fd);

    const item = this.fds.get(fd);

    if (!item)
      throw makeError('fsync', 'EBADF');

    if (!item.writable)
      throw makeError('fsync', 'EBADF');
  }

  async fsync(fd) {
    return this.fsyncSync(fd);
  }

  fdatasyncSync(fd) {
    return this.fsyncSync(fd);
  }

  async fdatasync(fd) {
    return this.fdatasyncSync(fd);
  }

  readSync(fd, data, offset, length, position) {
    assert((fd >>> 0) === fd);

    const item = this.fds.get(fd);

    if (!item)
      throw makeError('read', 'EBADF');

    if (!item.readable)
      throw makeError('read', 'EBADF');

    const {file} = item;

    file.ctime = Date.now();
    file.atime = file.ctime;

    return file.read(data, offset, length, position);
  }

  async read(fd, data, offset, length, position) {
    return this.readSync(fd, data, offset, length, position);
  }

  writeSync(fd, data, offset, length, position) {
    assert((fd >>> 0) === fd);

    const item = this.fds.get(fd);

    if (!item)
      throw makeError('write', 'EBADF');

    if (!item.writable)
      throw makeError('write', 'EBADF');

    const {file} = item;

    file.ctime = Date.now();
    file.mtime = file.ctime;

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

    const now = Date.now();

    this.inode = inode;

    this.uid = info.uid;
    this.gid = info.gid;
    this.mode = mode;

    this.atime = now;
    this.mtime = now;
    this.ctime = now;
    this.birthtime = now;

    this.size = 0;
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

    return w;
  }
}

/**
 * Directory
 */

class Directory extends File {
  constructor(mode, inode) {
    super(mode, inode);
    this.size = 4096;
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
    this.mode = file.mode;
    this.nlink = 1;
    this.uid = file.uid;
    this.gid = file.gid;
    this.rdev = 0;
    this.blksize = blocks * 4096;
    this.ino = file.inode;
    this.size = file.size;
    this.blocks = blocks;
    this.atimeMs = file.atime;
    this.mtimeMs = file.mtime;
    this.ctimeMs = file.ctime;
    this.birthtimeMs = file.birthtime;
    this.atime = new Date(file.atime);
    this.mtime = new Date(file.mtime);
    this.ctime = new Date(file.ctime);
    this.birthtime = new Date(file.birthtime);

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
        throw makeError('open', 'EBADFLAGS', path);
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

  let root = '/';

  if (path.length === 0 || path[0] !== '/')
    root = '';

  path = path.substring(root.length);

  if (path.length > 0 && path[path.length - 1] === Path.sep)
    path = path.slice(0, -1);

  if (path === '.' || path.length === 0)
    return [];

  return path.split(Path.sep);
}

function makeError(syscall, code, path) {
  const err = new Error(code);

  err.errno = 0;
  err.code = code;
  err.syscall = syscall;
  err.path = '/';

  if (path)
    err.path = path;

  if (Error.captureStackTrace)
    Error.captureStackTrace(err, makeError);

  return err;
}

/*
 * Expose
 */

module.exports = MFS;
