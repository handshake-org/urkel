/*!
 * file.js - merklix tree file backend
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 */

'use strict';

const assert = require('assert');
const fs = require('bfile');

/**
 * File
 */

class File {
  constructor(index) {
    assert((index >>> 0) === index);
    this.index = index;
    this.fd = -1;
    this.pos = 0;
    this.reads = 0;
  }

  async open(name, flags) {
    assert(typeof name === 'string');
    assert(typeof flags === 'string');

    if (this.fd !== -1)
      throw new Error('File already open.');

    this.fd = await fs.open(name, flags, 0o660);

    const stat = await fs.fstat(this.fd);

    this.pos = stat.size;
  }

  async close() {
    if (this.fd === -1)
      throw new Error('File already closed.');

    await fs.close(this.fd);

    this.fd = -1;
    this.pos = 0;
    this.reads = 0;
  }

  closeSync() {
    if (this.fd === -1)
      throw new Error('File already closed.');

    fs.closeSync(this.fd);

    this.fd = -1;
    this.pos = 0;
    this.reads = 0;
  }

  async read(pos, size) {
    const buf = Buffer.allocUnsafe(size);

    this.reads += 1;

    const r = await fs.read(this.fd, buf, 0, size, pos);

    this.reads -= 1;

    assert.strictEqual(r, size);

    return buf;
  }

  async write(data) {
    const pos = this.pos;

    const w = await fs.write(this.fd, data, 0, data.length, null);

    assert.strictEqual(w, data.length);

    this.pos += w;

    return pos;
  }

  async sync() {
    return fs.fsync(this.fd);
  }
}

/*
 * Expose
 */

module.exports = File;
