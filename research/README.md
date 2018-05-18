# Authenticated Data Structure Research

This directory contains some experiments with other provable data structures:

- `merklix/` - An implementation of a [Merklix Tree][merklix].
- `smt/` - An implementation of an optimized [Sparse Merkle Trees][smt], based
  on [gosmt].
- `ssmt/` - Simple [Sparse Merkle Tree][smt]. Unoptimized proof-of-concept,
  initially [implemented][ssmt] by Vitalik Buterin. It doesn't do bulk loaded
  insertions/splitting/caching/etc.

This is ultimately here to determine if a Patricia Trie is worth using over
anything else.

[merklix]: https://www.deadalnix.me/2016/09/24/introducing-merklix-tree-as-an-unordered-merkle-tree-on-steroid/
[smt]: https://eprint.iacr.org/2016/683
[gosmt]: https://github.com/pylls/gosmt
[ssmt]: https://github.com/ethereum/research/blob/master/trie_research/bintrie2/new_bintrie.py
