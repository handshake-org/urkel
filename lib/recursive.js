
  equals(node, hash) {
    return this.hash(hash).equals(node.hash(hash));
  }

function countBits(a, b, depth) {
  let bits = 0;

  while (hasBit(a, depth + bits) === hasBit(b, depth + bits))
    bits += 1;

  return bits;
}




  async _insert(node, leaf, depth) {
    if (node.isHash())
      node = await node.resolve(this.store);

    // Empty (sub)tree.
    if (node.isNull()) {
      // Replace the empty node.
      return leaf;
    }

    // Leaf node.
    if (node.isLeaf()) {
      // Current key.
      if (leaf.key.equals(node.key)) {
        // Exact leaf already exists.
        if (leaf.data.equals(node.data))
          return null;

        // The branch doesn't grow.
        // Replace the current node.
        return leaf;
      }

      assert(depth !== this.bits);

      // Count colliding bits.
      let bits = countBits(leaf.key, node.key, depth);

      // The other leaf is our sibling.
      let next = leaf;

      if (hasBit(leaf.key, depth + bits))
        next = new Internal(node, next);
      else
        next = new Internal(next, node);

      while (bits > 0) {
        bits -= 1;

        if (hasBit(leaf.key, depth + bits))
          next = new Internal(NIL, next);
        else
          next = new Internal(next, NIL);
      }

      return next;
    }

    if (depth === this.bits) {
      throw new MissingNodeError({
        nodeHash: node.hash(this.hash),
        key,
        depth
      });
    }

    assert(node.isInternal());

    // Internal node.
    if (hasBit(leaf.key, depth)) {
      const right = await this._insert(node.right, leaf, depth + 1);

      if (!right)
        return null;

      return new Internal(node.left, right);
    }

    const left = await this._insert(node.left, leaf, depth + 1);

    if (!left)
      return null;

    return new Internal(left, node.right);
  }

  async insert(key, value) {
    assert(this.isKey(key));
    assert(Buffer.isBuffer(value));

    const hash = this.hashValue(key, value);
    const leaf = new Leaf(hash, key, value);
    const root = await this._insert(this.root, leaf, 0);

    if (root)
      this.root = root;

    return this.root;
  }

  async _remove(node, sib, key, depth) {
    if (node.isHash())
      node = await node.resolve(this.store);

    // Empty (sub)tree.
    if (node.isNull())
      return null;

    // Leaf node.
    if (node.isLeaf()) {
      // Not our key.
      if (!key.equals(node.key))
        return null;

      // One extra disk read.
      if (sib.isHash())
        sib = await sib.resolve(this.store);

      // Shrink the subtree if we're a leaf.
      if (sib.isLeaf())
        return sib;

      return NIL;
    }

    if (depth === this.bits) {
      throw new MissingNodeError({
        nodeHash: node.hash(this.hash),
        key,
        depth
      });
    }

    assert(node.isInternal());

    // Internal node.
    if (hasBit(key, depth)) {
      let right = await this._remove(node.right, node.left, key, depth + 1);

      if (!right)
        return null;

      if (right.isLeaf()) {
        if (right.equals(node.left, this.hash))
          return right;

        if (node.left.isNull())
          return right;

        right = right.toHash(this.hash);
      }

      return new Internal(node.left, right);
    }

    let left = await this._remove(node.left, node.right, key, depth + 1);

    if (!left)
      return null;

    if (left.isLeaf()) {
      if (left.equals(node.right, this.hash))
        return left;

      if (node.right.isNull())
        return left;

      left = left.toHash(this.hash);
    }

    return new Internal(left, node.right);
  }

  async remove(key) {
    assert(this.isKey(key));

    const root = await this._remove(this.root, NIL, key, 0);

    if (root)
      this.root = root;

    return this.root;
  }

