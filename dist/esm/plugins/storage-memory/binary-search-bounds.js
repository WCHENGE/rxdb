/**
 * Optimized binary search functions for the memory storage.
 * Based on https://github.com/mikolalysenko/binary-search-bounds
 * but with performance improvements:
 * - Removed unnecessary undefined checks for comparator
 * - Inlined the norm() wrapper to avoid extra function calls
 * - Added string-specialized variants to avoid temporary array allocations
 */

export function boundGE(a, y, c, lo, hi) {
  var l = lo === undefined ? 0 : lo | 0;
  var h = hi === undefined ? a.length - 1 : hi | 0;
  var i = h + 1;
  while (l <= h) {
    var m = l + h >>> 1;
    if (c(a[m], y) >= 0) {
      i = m;
      h = m - 1;
    } else {
      l = m + 1;
    }
  }
  return i;
}
export function boundGT(a, y, c, lo, hi) {
  var l = lo === undefined ? 0 : lo | 0;
  var h = hi === undefined ? a.length - 1 : hi | 0;
  var i = h + 1;
  while (l <= h) {
    var m = l + h >>> 1;
    if (c(a[m], y) > 0) {
      i = m;
      h = m - 1;
    } else {
      l = m + 1;
    }
  }
  return i;
}
export function boundLT(a, y, c, lo, hi) {
  var l = lo === undefined ? 0 : lo | 0;
  var h = hi === undefined ? a.length - 1 : hi | 0;
  var i = l - 1;
  while (l <= h) {
    var m = l + h >>> 1;
    if (c(a[m], y) < 0) {
      i = m;
      l = m + 1;
    } else {
      h = m - 1;
    }
  }
  return i;
}
export function boundLE(a, y, c, lo, hi) {
  var l = lo === undefined ? 0 : lo | 0;
  var h = hi === undefined ? a.length - 1 : hi | 0;
  var i = l - 1;
  while (l <= h) {
    var m = l + h >>> 1;
    if (c(a[m], y) <= 0) {
      i = m;
      l = m + 1;
    } else {
      h = m - 1;
    }
  }
  return i;
}
export function boundEQ(a, y, c, lo, hi) {
  var l = lo === undefined ? 0 : lo | 0;
  var h = hi === undefined ? a.length - 1 : hi | 0;
  while (l <= h) {
    var m = l + h >>> 1;
    var p = c(a[m], y);
    if (p === 0) {
      return m;
    }
    if (p <= 0) {
      l = m + 1;
    } else {
      h = m - 1;
    }
  }
  return -1;
}

/**
 * Specialized binary search functions that compare DocWithIndexString
 * entries directly against an index string, avoiding temporary array allocations.
 * Used in query() and count() hot paths.
 */
export function boundGEByIndexString(a, indexString) {
  var l = 0;
  var h = a.length - 1;
  var i = h + 1;
  while (l <= h) {
    var m = l + h >>> 1;
    if (a[m][0] >= indexString) {
      i = m;
      h = m - 1;
    } else {
      l = m + 1;
    }
  }
  return i;
}
export function boundGTByIndexString(a, indexString) {
  var l = 0;
  var h = a.length - 1;
  var i = h + 1;
  while (l <= h) {
    var m = l + h >>> 1;
    if (a[m][0] > indexString) {
      i = m;
      h = m - 1;
    } else {
      l = m + 1;
    }
  }
  return i;
}
export function boundEQByIndexString(a, indexString) {
  var l = 0;
  var h = a.length - 1;
  while (l <= h) {
    var m = l + h >>> 1;
    var s = a[m][0];
    if (s === indexString) {
      return m;
    }
    if (s < indexString) {
      l = m + 1;
    } else {
      h = m - 1;
    }
  }
  return -1;
}
export function boundLTByIndexString(a, indexString) {
  var l = 0;
  var h = a.length - 1;
  var i = l - 1;
  while (l <= h) {
    var m = l + h >>> 1;
    if (a[m][0] < indexString) {
      i = m;
      l = m + 1;
    } else {
      h = m - 1;
    }
  }
  return i;
}
export function boundLEByIndexString(a, indexString) {
  var l = 0;
  var h = a.length - 1;
  var i = l - 1;
  while (l <= h) {
    var m = l + h >>> 1;
    if (a[m][0] <= indexString) {
      i = m;
      l = m + 1;
    } else {
      h = m - 1;
    }
  }
  return i;
}
//# sourceMappingURL=binary-search-bounds.js.map