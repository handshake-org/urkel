'use strict';

/*
 * Recursive insertion & removal
 */

class Tree {
  async _insert(node, leaf, depth) {
    switch (node.type()) {
      case NULL: {
        // Replace the empty node.
        return leaf;
      }

      case INTERNAL: {
        if (depth === this.bits) {
          throw new MissingNodeError({
            nodeHash: node.hash(this.hash),
            key,
            depth
          });
        }

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

      case LEAF: {
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

      case HASH: {
        const rn = await this.resolve(node);
        return this._insert(rn, leaf, depth);
      }

      default: {
        throw new AssertionError('Unknown node.');
      }
    }
  }

  async _remove(node, sibling, key, depth) {
    switch (node.type()) {
      case NULL: {
        // Empty (sub)tree.
        return null;
      }

      case INTERNAL: {
        if (depth === this.bits) {
          throw new MissingNodeError({
            nodeHash: node.hash(this.hash),
            key,
            depth
          });
        }

        if (hasBit(key, depth)) {
          let right = await this._remove(node.right, node.left, key, depth + 1);

          if (!right)
            return null;

          if (right.hasLeaf()) {
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

        if (left.hasLeaf()) {
          if (left.equals(node.right, this.hash))
            return left;

          if (node.right.isNull())
            return left;

          left = left.toHash(this.hash);
        }

        return new Internal(left, node.right);
      }

      case LEAF: {
        // Not our key.
        if (!key.equals(node.key))
          return null;

        // Shrink the subtree if we're a leaf.
        if (sibling.hasLeaf())
          return sibling;

        return NIL;
      }

      case HASH: {
        const rn = await this.resolve(node);
        return this._remove(rn, sibling, key, depth);
      }

      default: {
        throw new AssertionError('Unknown node.');
      }
    }
  }
}

class Node {
  equals(node, hash) {
    return this.hash(hash).equals(node.hash(hash));
  }
}

class Transaction {
  async insert(key, value) {
    assert(this.tree.isKey(key));
    assert(this.tree.isValue(value));

    const hash = this.tree.hashValue(key, value);
    const leaf = new Leaf(hash, key, value);
    const root = await this.tree._insert(this.root, leaf, 0);

    if (root)
      this.root = root;

    return this.root;
  }

  async remove(key) {
    assert(this.tree.isKey(key));

    const root = await this.tree._remove(this.root, NIL, key, 0);

    if (root)
      this.root = root;

    return this.root;
  }
}

function countBits(a, b, depth) {
  let bits = 0;

  while (hasBit(a, depth + bits) === hasBit(b, depth + bits))
    bits += 1;

  return bits;
}

/*
 * Old callback-style iteration
 */

class Tree {
  async keys(cb) {
    return this.iterate(false, cb);
  }

  async range(cb) {
    return this.iterate(true, cb);
  }

  async iterate(values, cb) {
    assert(typeof values === 'boolean');
    assert(typeof cb === 'function');

    return this._iterate(this.root, values, cb);
  }

  async _iterate(node, values, cb) {
    switch (node.type()) {
      case NULL: {
        return undefined;
      }

      case INTERNAL: {
        await this._iterate(node.left, values, cb);
        await this._iterate(node.right, values, cb);
        return undefined;
      }

      case LEAF: {
        let result = null;

        if (values) {
          const value = await this.retrieve(node);
          result = cb(node.key, value);
        } else {
          result = cb(node.key);
        }

        if (result instanceof Promise)
          await result;

        return undefined;
      }

      case HASH: {
        const rn = await this.resolve(node);
        return this._iterate(rn, values, cb);
      }
    }

    throw new AssertionError('Unknown node.');
  }
}

class Snapshot {
  async keys(cb) {
    return this.iterate(false, cb);
  }

  async range(cb) {
    return this.iterate(true, cb);
  }

  async iterate(values, cb) {
    assert(typeof values === 'boolean');
    assert(typeof cb === 'function');

    if (!this.root)
      this.root = await this.tree.getHistory(this.hash);

    return this.tree._iterate(this.root, values, cb);
  }
}
