/*!
 * file.js - tree file backend
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('bsert');
const {IOError} = require('./errors');

/**
 * File
 *
 * fs ???
 * size, should be an int, the current size of the file,  should be default 0 and will grow as the file increases
 * index, should be an index, the index of the file
 * fd ???
 * reads, an int, the number of current reads on the file
 */


class File {
  constructor(fs, index) {
    assert(fs);
    assert((index >>> 0) === index);

    this.fs = fs;
    this.index = index;
    this.fd = -1;
    this.size = 0;
    this.reads = 0;
  }



  async open(path, flags) {
    assert(typeof path === 'string');
    assert(typeof flags === 'string');

    this.checkFileClosed()
    this.fd = await this.fs.open(path, flags, 0o640);
    const stat = await this.fs.fstat(this.fd);
    this.size = stat.size;
  }



  openSync(path, flags) {
    assert(typeof path === 'string');
    assert(typeof flags === 'string');

    this.checkFileClosed()
    this.fd = this.fs.openSync(path, flags, 0o640);
    const stat = this.fs.fstatSync(this.fd);
    this.size = stat.size;
  }

  async close() {
    this.checkFileOpen();

    const fd = this.fd;

    this.fd = -1;
    this.size = 0;
    this.reads = 0;

    return this.fs.close(fd);
  }

  closeSync() {
    this.checkFileOpen()
    const fd = this.fd;

    this.fd = -1;
    this.size = 0;
    this.reads = 0;

    this.fs.closeSync(fd);
  }

  async sync() {
    this.checkFileOpen();
    return this.fs.fsync(this.fd);
  }

  syncSync() {
    this.checkFileOpen();
    this.fs.fsyncSync(this.fd);
  }

  async truncate(size) {
    this.checkFileOpen();
    if (size === this.size)
      return undefined;
    return this.fs.ftruncate(this.fd, size);
  }

  truncateSync(size) {
    this.checkFileOpen();
    if (size === this.size)
      return;
    this.fs.ftruncateSync(this.fd, size);
  }

  async read(pos, size) {
    this.checkFileOpen();
    const buf = Buffer.allocUnsafe(size);
    this.reads += 1;

    let r;

    try {
      r = await this.fs.read(this.fd, buf, 0, size, pos);
    } finally {
      this.reads -= 1;
    }

    this.checkReadSize(r, size, pos)
    return buf;
  }

  readSync(pos, size) {
    this.checkFileOpen();
    const buf = Buffer.allocUnsafe(size);
    const r = this.fs.readSync(this.fd, buf, 0, size, pos);
    this.checkReadSize(r, size, pos)

    return buf;
  }

  async write(data) {
    this.checkFileOpen();
    const pos = this.size;
    const w = await this.fs.write(this.fd, data, 0, data.length, null);
    this.checkWriteSize(w, data.length, pos)
    this.size += w;

    return pos;
  }

  writeSync(data) {
    this.checkFileOpen();
    const pos = this.size;
    const w = this.fs.writeSync(this.fd, data, 0, data.length, null);
    this.checkWriteSize(w, data.length, pos)
    this.size += w;

    return pos;
  }

  async rawRead(pos, size, out) {
    assert(size <= out.length);
    this.checkFileOpen();
    const r = await this.fs.read(this.fd, out, 0, size, pos);
    this.checkReadSize(r, size, pos)
    return out;
  }

  rawReadSync(pos, size, out) {
    assert(size <= out.length);
    this.checkFileOpen();
    const r = this.fs.readSync(this.fd, out, 0, size, pos);
    this.checkReadSize(r, size, pos)

    return out;

}

/*
Checks
*/

//TODO should this really be an error :/
checkFileClosed() {
  if (this.fd !== -1)
    throw new Error('File already open.');
}


checkFileOpen() {
  if (this.fd === -1)
    throw new Error('File already closed.');
}

checkReadSize(r, size, pos) {
  if (r !== size)
    throw new IOError('read', this.index, pos, size);
}


checkWriteSize(w, size, pos) {
  if (w !== size)
    throw new IOError('write', this.index, pos, size);
}

}
/*
 * Expose
 */

module.exports = File;
