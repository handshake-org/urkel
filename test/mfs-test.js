/* eslint-env mocha */
/* eslint prefer-arrow-callback: "off" */
/* eslint no-unused-vars: "off" */

/*
Tests for the MFS
*/

'use strict';

const assert = require('bsert');
const MFS = require('../lib/mfs.js');

/*
Test chdir for file path not string, should throw error
*/
async function filePathErrorTest() {
  let mfs = new MFS();
  assert.throws(() => { mfs.chdir(12345)}, Error, "Error thrown")

}


/*
Test mkdirpSync

*/
async function mkdirpSyncTest() {
  let mfs = new MFS();
  assert(await mfs.mkdirpSync('', null), null, "Should return null for empty path")
}




  describe("MFS", function() {
    this.timeout(5000);

    it('should not accept files without strings', async () => {
      await filePathErrorTest();
    });


/*
    it('should return null for zero length paths', async () => {
      await mkdirpSyncTest();
    });
*/

  });
