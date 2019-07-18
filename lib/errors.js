/*!
 * errors.js - tree errors
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/handshake-org/urkel
 */

'use strict';

const assert = require('bsert');

/**
 * Missing Node Error
 */

class MissingNodeError extends Error {
  /**
   * Create an error.
   * @constructor
   * @param {Object?} options
   */

  constructor(options = {}) {
    super();

    this.type = 'MissingNodeError';
    this.name = 'MissingNodeError';
    this.code = 'ERR_MISSING_NODE';
    this.rootHash = options.rootHash || null;
    this.nodeHash = options.nodeHash || null;
    this.key = options.key || null;
    this.depth = options.depth >>> 0;
    this.message = 'Missing node.';

    if (this.nodeHash)
      this.message = `Missing node: ${this.nodeHash.toString('hex')}.`;

    if (Error.captureStackTrace)
      Error.captureStackTrace(this, MissingNodeError);
  }
}

/**
 * IO Error
 */

class IOError extends Error {
  /**
   * Create an error.
   * @constructor
   */

  constructor(syscall, index, pos, size) {
    super();

    this.type = 'IOError';
    this.name = 'IOError';
    this.code = 'ERR_IO';
    this.syscall = syscall;
    this.index = index;
    this.pos = pos;
    this.size = size;
    this.message = `Invalid ${syscall} for file ${index} at ${pos}:${size}.`;

    if (Error.captureStackTrace)
      Error.captureStackTrace(this, IOError);
  }
}

class EncodingError extends Error {
  /**
   * Create an encoding error.
   * @constructor
   * @param {Number} offset
   * @param {String} reason
   */

  constructor(offset, reason, start) {
    super();

    this.type = 'EncodingError';
    this.name = 'EncodingError';
    this.code = 'ERR_ENCODING';
    this.message = `${reason} (offset=${offset}).`;

    if (Error.captureStackTrace)
      Error.captureStackTrace(this, start || EncodingError);
  }
}

/**
 * Assertion Error
 */

class AssertionError extends assert.AssertionError {
  constructor(message) {
    super({ message });
  }
}

/*
 * Expose
 */

exports.MissingNodeError = MissingNodeError;
exports.IOError = IOError;
exports.EncodingError = EncodingError;
exports.AssertionError = AssertionError;
