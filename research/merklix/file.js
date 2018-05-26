/*!
 * file.js - merklix tree file backend
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const {IOError} = require('./errors');

/**
 * File
 */

class File {
  constructor(fs, index) {
    assert(fs);
    assert((index >>> 0) === index);

    this.fs = fs;
    this.index = index;
    this.fd = -1;
    this.pos = 0;
    this.reads = 0;
  }

  async open(path, flags) {
    assert(typeof path === 'string');
    assert(typeof flags === 'string');

    if (this.fd !== -1)
      throw new Error('File already open.');

    this.fd = await this.fs.open(path, flags, 0o660);

    const stat = await this.fs.fstat(this.fd);

    this.pos = stat.size;
  }

  async close() {
    if (this.fd === -1)
      throw new Error('File already closed.');

    const fd = this.fd;

    this.fd = -1;
    this.pos = 0;
    this.reads = 0;

    return this.fs.close(fd);
  }

  closeSync() {
    if (this.fd === -1)
      throw new Error('File already closed.');

    const fd = this.fd;

    this.fd = -1;
    this.pos = 0;
    this.reads = 0;

    this.fs.closeSync(fd);
  }

  async sync() {
    if (this.fd === -1)
      throw new Error('File already closed.');

    return this.fs.fsync(this.fd);
  }

  async truncate(size) {
    if (this.fd === -1)
      throw new Error('File already closed.');

    if (size === this.pos)
      return undefined;

    return this.fs.ftruncate(this.fd, size);
  }

  async read(pos, size) {
    if (this.fd === -1)
      throw new Error('File is closed.');

    const buf = Buffer.allocUnsafe(size);

    this.reads += 1;

    let r;

    try {
      r = await this.fs.read(this.fd, buf, 0, size, pos);
    } finally {
      this.reads -= 1;
    }

    if (r !== size)
      throw new IOError('read', this.index, pos, size);

    return buf;
  }

  async write(data) {
    if (this.fd === -1)
      throw new Error('File is closed.');

    const pos = this.pos;

    const w = await this.fs.write(this.fd, data, 0, data.length, null);

    if (w !== data.length)
      throw new IOError('write', this.index, pos, data.length);

    this.pos += w;

    return pos;
  }

  async rawRead(pos, size, out) {
    assert(size <= out.length);

    if (this.fd === -1)
      throw new Error('File is closed.');

    const r = await this.fs.read(this.fd, out, 0, size, pos);

    if (r !== size)
      throw new IOError('read', this.index, pos, size);

    return out;
  }
}

/*
 * Expose
 */

module.exports = File;
