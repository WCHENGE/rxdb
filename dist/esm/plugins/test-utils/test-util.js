import assert from 'assert';
import { promiseWait, requestIdlePromise } from "../utils/index.js";
export function testMultipleTimes(times, title, test) {
  new Array(times).fill(0).forEach(() => {
    it(title, test);
  });
}
export async function ensureCollectionsHaveEqualState(c1, c2, logContext) {
  await requestIdlePromise();
  var getJson = async collection => {
    var docs = await collection.find().exec();
    return docs.map(d => d.toJSON());
  };
  var json1 = await getJson(c1);
  var json2 = await getJson(c2);
  try {
    assert.deepStrictEqual(json1, json2);
  } catch (err) {
    console.error('ensureCollectionsHaveEqualState(' + logContext + ') states not equal (c1:' + c1.name + ', c2:' + c2.name + '):');
    console.dir({
      c1: json1,
      c2: json2
    });
    console.log('----------');
    throw err;
  }
}

/**
 * Waits until the collections have the equal state.
 */
export async function awaitCollectionsHaveEqualState(c1, c2, logContext, timeout = 8000) {
  var i = 0;
  var startTime = Date.now();
  while (true) {
    i++;
    try {
      await ensureCollectionsHaveEqualState(c1, c2, logContext);
      return;
    } catch (err) {
      if (Date.now() - startTime > timeout) {
        throw err;
      } else {
        await promiseWait(50 * i);
      }
    }
  }
}

/**
 * Deletes all locally stored IndexedDB databases.
 * Noop if IndexedDB is not available (e.g. in Node.js)
 * or if the .databases() method is not supported.
 */
export async function clearAllLocalIndexedDB() {
  if (typeof indexedDB === 'undefined' || typeof indexedDB.databases !== 'function') {
    return;
  }
  var databases = await indexedDB.databases();
  await Promise.all(databases.filter(db => !!db.name).map(db => {
    return new Promise((resolve, reject) => {
      var req = indexedDB.deleteDatabase(db.name);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }));
}

/**
 * Deletes all files and directories stored in the
 * Origin Private File System (OPFS).
 * Noop if OPFS is not available (e.g. in Node.js).
 */
export async function clearAllLocalOPFS(maxRetries = 20, delayMs = 200) {
  if (typeof navigator === 'undefined' || !navigator.storage || typeof navigator.storage.getDirectory !== 'function') {
    return;
  }
  var root = await navigator.storage.getDirectory();
  // @ts-ignore entries() is not in all TS lib definitions
  for await (var [name] of root.entries()) {
    for (var attempt = 0; attempt < maxRetries; attempt++) {
      try {
        await root.removeEntry(name, {
          recursive: true
        });
        break;
      } catch (err) {
        if (err?.name === 'NoModificationAllowedError' && attempt < maxRetries - 1) {
          await new Promise(resolve => setTimeout(resolve, delayMs));
        } else {
          throw err;
        }
      }
    }
  }
}

/**
 * Clears all localStorage data.
 * Noop if localStorage is not available (e.g. in Node.js).
 */
export async function clearAllLocalStorage() {
  if (typeof localStorage === 'undefined' || typeof localStorage.clear !== 'function') {
    return;
  }
  localStorage.clear();
}
export function ensureReplicationHasNoErrors(replicationState) {
  /**
   * We do not have to unsubscribe because the observable will cancel anyway.
   */
  replicationState.error$.subscribe(err => {
    console.error('ensureReplicationHasNoErrors() has error:');
    console.log(err);
    if (err?.parameters?.errors) {
      throw err.parameters.errors[0];
    }
    throw err;
  });
}
//# sourceMappingURL=test-util.js.map