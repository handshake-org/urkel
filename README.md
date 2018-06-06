# urkel

An optimized and cryptographically provable database.

## Usage

``` js
const crypto = require('bcrypto');
const {Tree} = require('urkel');
const {SHA256, randomBytes} = crypto;

const tree = new Tree(SHA256, 160, '/path/to/my/db');

await tree.open();

let last;

for (let i = 0; i < 500; i++) {
  const key = randomBytes(20);
  const value = randomBytes(300);
  await tree.insert(key, value);
  last = key;
}

await tree.commit();

const root = tree.rootHash();
const proof = await tree.prove(root, last);
const [code, value] = tree.verify(root, last, proof);

if (code === 0 && value)
  console.log('Valid proof for: %s', value.toString('hex'));

await tree.values((key, value) => {
  console.log('Iterated over item:');
  console.log([key.toString('hex'), value.toString('hex')]);
});

await tree.close();
```

## Contribution and License Agreement

If you contribute code to this project, you are implicitly allowing your code
to be distributed under the MIT license. You are also implicitly verifying that
all code is your original work. `</legalese>`

## License

- Copyright (c) 2018, Christopher Jeffrey (MIT License).

See LICENSE for more info.
