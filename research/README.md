# Authenticated Data Structure Research

This directory contains some experiments with other provable data structures:

- `merklix/` - An implementation of a [Merklix Tree][merklix].
- `smt/` - An implementation of an optimized [Sparse Merkle Tree][smt],
  partially based on [gosmt].

This is ultimately here to determine if a Patricia Trie is worth using over
anything else.

[merklix]: https://www.deadalnix.me/2016/09/24/introducing-merklix-tree-as-an-unordered-merkle-tree-on-steroid/
[smt]: https://eprint.iacr.org/2016/683
[gosmt]: https://github.com/pylls/gosmt
