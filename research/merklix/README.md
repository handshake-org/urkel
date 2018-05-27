# merklix

Optimized [Merklix Tree][1] for node.js.

## Usage

``` js
const crypto = require('bcrypto');
const {Merklix} = require('merklix');
const {SHA256, randomBytes} = crypto;

const tree = new Merklix(SHA256, 160, '/path/to/my/db');

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

## Description

Much like a patricia trie or a sparse merkle tree, the merklix tree follows a
path down each key in order to find the target leaf node.

### Insertion

We start with a simple insertion of value `a` with a key of `0000`.  It becomes
the root of the merkle tree.

```
Map:
  0000 = a

Tree:
       a
```

Say we insert value `b` with a key of `1100`. The tree grows down and we are
now 1 level deep. Note that we only went right once even though we have 3
extra bits in the key.

```
Map:
  0000 = a
  1100 = b

Tree:
       R
      / \
     /   \
    a     b
```

This next part is important to how the merklix tree handles key collisions. Say
we insert value `c` with a key of `1101`. You'll notice it has a three bit
collision with leaf `b` which has a key of `1100`. In order to maintain a
proper key path within the tree, we grow down and add "null" nodes (represented
by `x`) as children of internal nodes. This is basically a sign that there's a
dead end in one of the internal nodes. This is the trick to keeping the merklix
tree small and ever-growing, unlike a sparse merkle tree for example.

```
Map:
  0000 = a
  1100 = b
  1101 = c

Tree:
       R
      / \
     /   \
    a    /\
        /  \
       x   /\
          /  \
         /\   x
        /  \
       b    c
```

If we add value `d` with a key of `1000`, it is free to consume one of the
"null" nodes.

```
Map:
  0000 = a
  1100 = b
  1101 = c
  1000 = d

Tree:
       R
      / \
     /   \
    a    /\
        /  \
       d   /\
          /  \
         /\   x
        /  \
       b    c
```

### Removal

Removal is tricky when we have "dead-end" nodes in our subtree. We need to
revert all of the subtree growing we just did.

If we were to remove leaf `d` from the above tree, we _must_ replace it with a
"dead-end". The general rule is: if the target node's sibling is an internal
node, replace with a null node. If the sibling is another leaf, attempt to
ungrow the subtree by detecting key collisions.

Removing leaf `d` (we _must_ replace with a dead-end):

```
Map:
  0000 = a
  1100 = b
  1101 = c

Tree:
       R
      / \
     /   \
    a    /\
        /  \
       x   /\
          /  \
         /\   x
        /  \
       b    c
```

Removing leaf `c` (shrink the subtree):

```
Map:
  0000 = a
  1100 = b

Tree:
       R
      / \
     /   \
    a     b
```

Removing leaf `b` (`a` becomes the root):

```
Map:
  0000 = a

Tree:
       a
```

And we're back to where we started.

### Proofs

The proof is a standard merkle proof, with some extra gotchas.  The actual hash
at a leaf is the computed as `HASH(0x00 | key | HASH(value))`. It is important
to have the full key as part of the preimage. If a non-existence proof is
necessary, we need to send the full preimage to prove that we are a leaf, and
that we're also a different key that may have a colliding path with whatever
key a peer is trying to get a proof for. On the other hand, if the key path
stops at one of the "dead-end" nodes, we do not have to send any preimage! Even
better, if there are any "dead-end" nodes up the subtree when creating a proof,
we can compress them since they are redundant zero-hashes.

Say we were asked to prove the existence or non-existence of key `1110`, with
our original tree of:

```
Map:
  0000 = a
  1100 = b
  1101 = c
  1000 = d

Tree:
       R
      / \
     /   \
    a    /\
        /  \
       d   /\
          /  \
         /\   x
        /  \
       b    c
```

Key `1110` doesn't exist, so we must provide the hashes of nodes `a`, `d`, the
parent hash of `b` and `c`, and finally a dead-end node `x`. The fact that a
final leaf node was a dead-end node proves non-existence.

```
Map:
  0000 = a
  1100 = b
  1101 = c
  1000 = d

Tree:
       R
      / \
     /   \
   (a)   /\
        /  \
      (d)  /\
          /  \
        (/\) [x]
        /  \
       b    c
```

The dead-end node `x` can be compressed into a single bit, since it is a
zero-hash.

Proving non-existence for key `0100` is more difficult. Remember node `a` has a
key of `0000`. We have to provide the parent node's hash for `d` and it's right
sibling, as well as `a` and it's original key `0000`. This makes the
non-existence proof bigger because we have to provide the full preimage,
proving that node `a` is indeed a leaf, but that it has a different key than
the one we're looking for.

```
Map:
  0000 = a
  1100 = b
  1101 = c
  1000 = d

Tree:
       R
      / \
     /   \
   [a]  (/\)
        /  \
       d   /\
          /  \
         /\   x
        /  \
       b    c
```

We need only send the preimage for `a` (the value hash of `a` itself and its
key `0000`), sending it's hash would be a redundant 32 bytes.

Other than that, an existence proof is pretty straight forward. Proving leaf
`c` (`1101`), we would send the leaf hashes of `a`, and `d`, with one dead-end
node, and finally the sibling of `c`: `b`. The leaf hash of `c` is not
transmitted, only it's value (`c`). The full preimage is know on the other
side, allowing us to compute `HASH(0x00 | 1101 | HASH("c"))` to get the leaf
hash.

```
Map:
  0000 = a
  1100 = b
  1101 = c
  1000 = d

Tree:
       R
      / \
     /   \
   (a)   /\
        /  \
      (d)  /\
          /  \
         /\  (x) <-- compressed
        /  \
      (b)  [c]
```

---

With 50,000,000 leaves in the tree, the average depth of any given key path
down the tree should be around 26 or 27 (due the inevitable key prefix
collisions). This results in a proof size slightly over 800 bytes, pushing a
1-2ms proof creation time.

### Optimizing a merklix tree on disk

Due to the sheer number of nodes, a flat-file store is necessary. The amount of
database lookups would be overwhelming for something like leveldb. A merklix
tree is much simpler than a patricia trie in that we need only store 2 nodes:
internal nodes and leaves.

Internal nodes are stored as:

``` c
struct {
  uint8_t left_hash[32];
  uint16_t left_file;
  uint32_t left_position;
  uint8_t right_hash[32];
  uint16_t right_file;
  uint32_t right_position;
}
```

Leaf nodes are stored as:

``` c
struct {
  uint8_t leaf_hash[32];
  uint8_t key[20];
  uint16_t value_file;
  uint32_t value_position;
  uint32_t value_size;
}
```

The actual leaf data is stored at `value_position` in `value_file`.

This module will store the tree in a series of append-only files. A
particularly large write buffer is used to batch all insertions. Atomicity with
a parent database can be achieved by fsyncing every write and inserting the
best root hash and file position into something like leveldb (once the fsync
has completed).

That said, the module can also operate in "standalone" mode, where a metadata
root is written with a 20 byte checksum on every commit. This gives full crash
consistency as the database will always parse back to the last intact metadata
root on boot.

### Collision Attacks

It is possible for someone to grind a key to create bit collisions. Currently,
the entire bitcoin network produces 72-80 bit collisions on block hashes. So
worst case, that's 72-80 levels deep, but while storage increases, the rounds
of hashing are still half (or less than half) of that of a sparse merkle tree.

## Contribution and License Agreement

If you contribute code to this project, you are implicitly allowing your code
to be distributed under the MIT license. You are also implicitly verifying that
all code is your original work. `</legalese>`

## License

- Copyright (c) 2018, Christopher Jeffrey (MIT License).

See LICENSE for more info.

[1]: https://www.deadalnix.me/2016/09/24/introducing-merklix-tree-as-an-unordered-merkle-tree-on-steroid/
