/*!
 * lockfile.js - lock file
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('bsert');
const Path = require('path');

/**
 * LockFile
 */

class LockFile {
  constructor(fs, prefix) {
    assert(fs && typeof fs.write === 'function');

    this.fs = fs;
    this.file = '';
    this.attempts = 4;
    this.interval = 1500;
    this.retry = 1000;
    this.stale = 3500;
    this.timer = null;

    this.rename(prefix);
  }

  rename(prefix) {
    assert(typeof prefix === 'string');
    this.file = Path.resolve(prefix, 'lock');
  }

  async open() {
    for (let i = 0; i < this.attempts; i++) {
      const stat = await this.stat();

      if (stat) {
        const now = Date.now();
        const mtime = Math.round(stat.mtimeMs);

        // Somebody changed their system clock.
        if (mtime >= now + 10 * 1000) {
          await this.unlink();
          continue;
        }

        if (now < mtime + this.stale) {
          await this.wait();
          continue;
        }
      }

      try {
        if (!stat)
          await this.touch();
        else
          await this.update();
      } catch (e) {
        continue;
      }

      this.start();

      return;
    }

    throw new Error(`Could not acquire lock for: ${this.file}.`);
  }

  async close() {
    this.stop();
    return this.unlink();
  }

  start() {
    assert(this.timer == null);

    this.timer = setInterval(() => this.iterate(), this.interval);

    if (this.timer.unref)
      this.timer.unref();
  }

  stop() {
    assert(this.timer != null);
    clearInterval(this.timer);
    this.timer = null;
  }

  async iterate() {
    try {
      await this.update();
    } catch (e) {
      ;
    }
  }

  async stat() {
    let stat = null;

    try {
      stat = await this.fs.stat(this.file);
    } catch (e) {
      if (e.code !== 'ENOENT')
        throw e;
    }

    return stat;
  }

  async unlink() {
    try {
      await this.fs.unlink(this.file);
    } catch (e) {
      if (e.code !== 'ENOENT')
        throw e;
    }
  }

  async wait() {
    return new Promise(r => setTimeout(r, this.retry));
  }

  async touch() {
    const fd = await this.fs.open(this.file, 'wx', 0o640);
    return this.fs.close(fd);
  }

  async update() {
    return this.fs.truncate(this.file, 0);
  }
}

/*
 * Expose
 */

module.exports = LockFile;
