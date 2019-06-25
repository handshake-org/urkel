'use strict';

const assert = require('bsert');
const File = require('../lib/file.js');

/*
Test chdir for file path not string, should throw error

async function Test() {
  var file = new File();
  assert(true, true, "Always should be right")
}
*/


/*
Test
Closing and Opening sync
Cannot repeatedly open and close files
*/
async function OpenTest() {
  var file = new File("/store", 0);
  await file.openSync('/file.txt')
  // Must have string
  //assert.throws(await file.openSync('/file.txt'), Error, "Throws Error")

  //

}



  describe("Node", function() {
    this.timeout(5000);

/*
    it('should open files successfully', async () => {
      await OpenTest();
    });
*/


  });
