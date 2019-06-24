const randomBytes = require('randombytes');
const {Tree, Proof} = require('../optimized');
const bcrypto = require('bcrypto');
const {SHA256} = bcrypto;


function unpack(str, m = -1) {
    let n = m;
    let l = str.length;
    if (m == -1) {
      n = l;
    }
    str = str.padStart(n-l, ' ');

    var bytes = [];
    for(var i = 0; i < n; i++) {
        var char = str.charCodeAt(i);
        bytes.push(char >>> 8, char & 0xFF);
    }
    return new Buffer.from(bytes);
}


function pack(bytes) {
    var chars = [];
    for(var i = 0, n = bytes.length; i < n;) {
        chars.push(((bytes[i++] & 0xff) << 8) | (bytes[i++] & 0xff));
    }
    return String.fromCharCode.apply(null, chars);
}



async function create(index) {
  const tree = new Tree(SHA256, 256, './db/' + index);
  await tree.open();
  return tree
}




async function setup() {

  // Create a tree using blake2b-256 and a depth/key-size of 256 bits.
  let tree = await create("Test_Index");

  const txn = tree.transaction();

  var d = new Date();
  var n = d.getMilliseconds();
  var n1 = d.getSeconds();
  var n2 = d.getMinutes();
  //console.log(n2, n1, n);


  for (let j = 0; j < 200000; j++) {
      const k = unpack(j.toString(), 16);

      const v = unpack(JSON.stringify({value: j, id: Date.now()}));
      await txn.insert(k, v);
  }
  //const root = await txn.commit();
  //const snapshot = tree.snapshot(root);
  await tree.close();
  d = new Date();
  n = d.getMilliseconds();
  n1 = d.getSeconds();
  n2 = d.getMinutes();
  //console.log(n2, n1, n);
}


async function iteratorTest() {

  var d = new Date();
  var n = d.getMilliseconds();
  var n1 = d.getSeconds();
  var n2 = d.getMinutes();
  //console.log(n2, n1, n);

  let tree = await create("Test_Index");
  const iter = tree.iterator(true, true);

  while (await iter.next()) {
    const {key, v} = await iter;
    //console.log(pack(key))
    //console.log(v)
    //console.log('Iterated over item:');
    //console.log('%s: %s', pack(key));
  }
  var d = new Date();
  var n = d.getMilliseconds();
  var n1 = d.getSeconds();
  var n2 = d.getMinutes();
  //console.log(n2, n1, n);

}







async function main() {
    //console.log("Setup")
    await setup()
    //console.log("Iterator Test")
    //await iteratorTest()

}















async function main1() {

  // Create a tree using blake2b-256 and a depth/key-size of 256 bits.
  const tree = await tree.setup("new_index");
  const iter = tree.iterator(true, true);


  while (await iter.next()) {
    const {key, v} = await iter;
    console.log(pack(key))
    const {value, proof} = await urkel.get(tree, k)
    console.log('Iterated over item:');
    console.log('%s: %s', pack(key), value.toString('hex'));
  }
}




//main();

main();
