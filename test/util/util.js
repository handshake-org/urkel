'use strict';

const assert = require('assert');
const crypto = require('crypto');

class SHA1 {
  constructor() {
    this.ctx = null;
  }

  init() {
    this.ctx = crypto.createHash('sha1');
    return this;
  }

  update(data) {
    assert(Buffer.isBuffer(data));
    assert(this.ctx, 'Context already finalized.');
    this.ctx.update(data);
    return this;
  }

  final() {
    assert(this.ctx, 'Context already finalized.');
    const hash = this.ctx.digest();
    this.ctx = null;
    return hash;
  }

  static hash() {
    return new SHA1();
  }

  static digest(data) {
    return new SHA1().init().update(data).final();
  }

  static root(left, right) {
    assert(Buffer.isBuffer(left) && left.length === 20);
    assert(Buffer.isBuffer(right) && right.length === 20);
    return new SHA1().init().update(left).update(right).final();
  }
}

SHA1.id = 'sha1';
SHA1.size = 20;
SHA1.bits = 160;
SHA1.zero = Buffer.alloc(20, 0x00);

class SHA256 {
  constructor() {
    this.ctx = null;
  }

  init() {
    this.ctx = crypto.createHash('sha256');
    return this;
  }

  update(data) {
    assert(Buffer.isBuffer(data));
    assert(this.ctx, 'Context already finalized.');
    this.ctx.update(data);
    return this;
  }

  final() {
    assert(this.ctx, 'Context already finalized.');
    const hash = this.ctx.digest();
    this.ctx = null;
    return hash;
  }

  static hash() {
    return new SHA256();
  }

  static digest(data) {
    return new SHA256().init().update(data).final();
  }

  static root(left, right) {
    assert(Buffer.isBuffer(left) && left.length === 32);
    assert(Buffer.isBuffer(right) && right.length === 32);
    return new SHA256().init().update(left).update(right).final();
  }
}

SHA256.id = 'sha256';
SHA256.size = 32;
SHA256.bits = 256;
SHA256.zero = Buffer.alloc(32, 0x00);

exports.sha1 = SHA1;
exports.sha256 = SHA256;
