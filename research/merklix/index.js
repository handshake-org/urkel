/*!
 * merklix.js - merklix tree
 * Copyright (c) 2018, Christopher Jeffrey (MIT License).
 * https://github.com/bcoin-org/bcoin
 *
 * Merklix Trees:
 *   https://www.deadalnix.me/2016/09/24/introducing-merklix-tree-as-an-unordered-merkle-tree-on-steroid/
 *   https://www.deadalnix.me/2016/09/29/using-merklix-tree-to-checkpoint-an-utxo-set/
 */

'use strict';

/*
 * Expose
 */

exports.common = require('./common');
exports.errors = require('./errors');
exports.Merklix = require('./merklix');
exports.nodes = require('./nodes');
exports.proof = require('./proof');
exports.Proof = exports.proof.Proof;
exports.store = require('./store');
