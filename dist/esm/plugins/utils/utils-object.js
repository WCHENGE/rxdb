export function deepFreeze(o) {
  Object.freeze(o);
  Object.getOwnPropertyNames(o).forEach(function (prop) {
    if (Object.prototype.hasOwnProperty.call(o, prop) && o[prop] !== null && (typeof o[prop] === 'object' || typeof o[prop] === 'function') && !Object.isFrozen(o[prop])) {
      deepFreeze(o[prop]);
    }
  });
  return o;
}

/**
 * To get specific nested path values from objects,
 * RxDB normally uses the 'dot-prop' npm module.
 * But when performance is really relevant, this is not fast enough.
 * Instead we use a monad that can prepare some stuff up front
 * and we can reuse the generated function.
 */

/**
 * Cache for objectPathMonad to avoid re-creating closures
 * and re-splitting strings for the same paths.
 */
var objectPathMonadCache = new Map();
export function objectPathMonad(objectPath) {
  var fn = objectPathMonadCache.get(objectPath);
  if (fn) {
    return fn;
  }
  var split = objectPath.split('.');

  // reuse this variable for better performance.
  var splitLength = split.length;

  /**
   * Performance shortcut,
   * if no nested path is used,
   * directly return the field of the object.
   */
  if (splitLength === 1) {
    fn = obj => obj[objectPath];
  } else if (splitLength === 2) {
    /**
     * Fast path for 2-segment paths (e.g. 'nested.field').
     * Avoids the loop overhead for the most common nested case.
     */
    var key0 = split[0];
    var key1 = split[1];
    fn = obj => {
      var v = obj[key0];
      return v === undefined || v === null ? undefined : v[key1];
    };
  } else if (splitLength === 3) {
    /**
     * Fast path for 3-segment paths (e.g. 'deep.deeper.deepNr').
     * Common in index fields and nested document properties.
     */
    var _key = split[0];
    var _key2 = split[1];
    var key2 = split[2];
    fn = obj => {
      var v = obj[_key];
      if (v === undefined || v === null) return undefined;
      var v2 = v[_key2];
      return v2 === undefined || v2 === null ? undefined : v2[key2];
    };
  } else if (splitLength === 4) {
    /**
     * Fast path for 4-segment paths.
     * Avoids loop overhead for deeper nested properties.
     */
    var _key3 = split[0];
    var _key4 = split[1];
    var _key5 = split[2];
    var key3 = split[3];
    fn = obj => {
      var v = obj[_key3];
      if (v === undefined || v === null) return undefined;
      var v2 = v[_key4];
      if (v2 === undefined || v2 === null) return undefined;
      var v3 = v2[_key5];
      return v3 === undefined || v3 === null ? undefined : v3[key3];
    };
  } else if (splitLength === 5) {
    /**
     * Fast path for 5-segment paths.
     * Avoids loop overhead for deeply nested properties.
     */
    var _key6 = split[0];
    var _key7 = split[1];
    var _key8 = split[2];
    var _key9 = split[3];
    var key4 = split[4];
    fn = obj => {
      var v = obj[_key6];
      if (v === undefined || v === null) return undefined;
      var v2 = v[_key7];
      if (v2 === undefined || v2 === null) return undefined;
      var v3 = v2[_key8];
      if (v3 === undefined || v3 === null) return undefined;
      var v4 = v3[_key9];
      return v4 === undefined || v4 === null ? undefined : v4[key4];
    };
  } else {
    fn = obj => {
      var currentVal = obj;
      for (var i = 0; i < splitLength; ++i) {
        if (currentVal === undefined || currentVal === null) {
          return undefined;
        }
        currentVal = currentVal[split[i]];
      }
      return currentVal;
    };
  }
  objectPathMonadCache.set(objectPath, fn);
  return fn;
}
export function getFromObjectOrThrow(obj, key) {
  var val = obj[key];
  if (typeof val === 'undefined') {
    throw new Error('missing value from object ' + key);
  }
  return val;
}

/**
 * returns a flattened object
 * @link https://gist.github.com/penguinboy/762197
 */
export function flattenObject(ob) {
  var toReturn = {};
  for (var i in ob) {
    if (!Object.prototype.hasOwnProperty.call(ob, i)) continue;
    if (typeof ob[i] === 'object' && ob[i] !== null) {
      var flatObject = flattenObject(ob[i]);
      for (var x in flatObject) {
        if (!Object.prototype.hasOwnProperty.call(flatObject, x)) continue;
        toReturn[i + '.' + x] = flatObject[x];
      }
    } else {
      toReturn[i] = ob[i];
    }
  }
  return toReturn;
}

/**
 * does a flat copy on the objects,
 * is about 3 times faster than using deepClone.
 * Using the spread operator instead of Object.assign
 * because V8 optimizes spread for plain objects (~4x faster).
 */
export function flatClone(obj) {
  return {
    ...obj
  };
}

/**
 * @link https://stackoverflow.com/a/11509718/3443137
 */
export function firstPropertyNameOfObject(obj) {
  return Object.keys(obj)[0];
}
export function firstPropertyValueOfObject(obj) {
  var key = Object.keys(obj)[0];
  return obj[key];
}

/**
 * deep-sort an object so its attributes are in lexical order.
 * Also sorts the arrays inside of the object if no-array-sort not set
 */
export function sortObject(obj, noArraySort = false) {
  if (!obj) return obj; // do not sort null, false or undefined

  // array
  if (!noArraySort && Array.isArray(obj)) {
    return obj.sort((a, b) => {
      if (typeof a === 'string' && typeof b === 'string') return a.localeCompare(b);
      if (typeof a === 'object') return 1;else return -1;
    }).map(i => sortObject(i, noArraySort));
  }

  // object
  // array is also of type object
  if (typeof obj === 'object' && !Array.isArray(obj)) {
    var out = {};
    Object.keys(obj).sort((a, b) => a.localeCompare(b)).forEach(key => {
      out[key] = sortObject(obj[key], noArraySort);
    });
    return out;
  }

  // everything else
  return obj;
}

/**
 * Deep clone a plain json object.
 * Does not work with recursive stuff
 * or non-plain-json.
 * IMPORTANT: Performance of this is very important,
 * do not change it without running performance tests!
 *
 * @link https://github.com/zxdong262/deep-copy/blob/master/src/index.ts
 */
function deepClone(src) {
  if (!src || typeof src !== 'object') {
    return src;
  }
  if (Array.isArray(src)) {
    var ret = new Array(src.length);
    var i = ret.length;
    while (i--) {
      ret[i] = deepClone(src[i]);
    }
    return ret;
  }
  // Blobs are immutable — pass through without cloning, otherwise it gets converted into a normal object, which breaks things.
  if (typeof Blob !== 'undefined' && src instanceof Blob) {
    return src;
  }
  var dest = {};
  // eslint-disable-next-line guard-for-in
  for (var key in src) {
    dest[key] = deepClone(src[key]);
  }
  return dest;
}
export var clone = deepClone;

/**
 * overwrites the getter with the actual value
 * Mostly used for caching stuff on the first run.
 *
 * Using a value descriptor instead of a getter descriptor
 * so that subsequent property accesses are direct value lookups
 * instead of function calls, which is ~37% faster for reads.
 */
export function overwriteGetterForCaching(obj, getterName, value) {
  Object.defineProperty(obj, getterName, {
    value
  });
  return value;
}
export function hasDeepProperty(obj, property) {
  if (obj.hasOwnProperty(property)) {
    return true;
  }
  if (Array.isArray(obj)) {
    var has = !!obj.find(item => hasDeepProperty(item, property));
    return has;
  }

  // Recursively check for property in nested objects
  for (var key in obj) {
    if (typeof obj[key] === 'object' && obj[key] !== null) {
      if (hasDeepProperty(obj[key], property)) {
        return true;
      }
    }
  }

  // Return false if 'foobar' is not found at any level
  return false;
}

/**
 * Deeply checks if an object contains any property
 * with the value of undefined
 * If yes, returns the path to it.
 */
export function findUndefinedPath(obj, parentPath = '') {
  // If `obj` is not an object or is null, we can't go deeper, so return false
  if (typeof obj !== "object" || obj === null) {
    return false;
  }
  for (var key of Object.keys(obj)) {
    var value = obj[key];
    // Build the full path. For the root level, it's just the key;
    // for nested levels, prepend the parent path followed by a dot.
    var currentPath = parentPath ? parentPath + "." + key : key;

    // If the value is undefined, return the path
    if (typeof value === 'undefined') {
      return currentPath;
    }

    // If the value is an object, recurse to check deeper
    if (typeof value === "object" && value !== null) {
      var result = findUndefinedPath(value, currentPath);
      // If a path was found in the nested object, return it
      if (result) {
        return result;
      }
    }
  }

  // If no property with undefined was found
  return false;
}
//# sourceMappingURL=utils-object.js.map