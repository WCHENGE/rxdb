import { IdleQueue } from 'custom-idle-queue';
import { ObliviousSet } from 'oblivious-set';
import { pluginMissing, flatClone, PROMISE_RESOLVE_FALSE, randomToken, ensureNotFalsy, getDefaultRevision, getDefaultRxDocumentMeta, defaultHashSha256, RXDB_VERSION, deepEqual } from "./plugins/utils/index.js";
import { newRxError } from "./rx-error.js";
import { createRxSchema } from "./rx-schema.js";
import { runPluginHooks, runAsyncPluginHooks } from "./hooks.js";
import { Subject } from 'rxjs';
import { mergeMap } from 'rxjs/operators';
import { createRxCollection } from "./rx-collection.js";
import { flatCloneDocWithMeta, getSingleDocument, getWrappedStorageInstance, INTERNAL_STORAGE_NAME } from "./rx-storage-helper.js";
import { ensureStorageTokenDocumentExists, getAllCollectionDocuments, getPrimaryKeyOfInternalDocument, INTERNAL_CONTEXT_COLLECTION, INTERNAL_STORE_SCHEMA, _collectionNamePrimary } from "./rx-database-internal-store.js";
import { createRxCollectionStorageInstance, removeCollectionStorages } from "./rx-collection-helper.js";
import { overwritable } from "./overwritable.js";
import { rxChangeEventBulkToRxChangeEvents } from "./rx-change-event.js";

/**
 * stores the used database names+storage names
 * so we can throw when the same database is created more than once.
 */
var USED_DATABASE_NAMES = new Set();
var DATABASE_UNCLOSED_INSTANCE_PROMISE_MAP = new Map();
var DB_COUNT = 0;
export class RxDatabaseBase {
  idleQueue = new IdleQueue();
  rxdbVersion = RXDB_VERSION;

  /**
   * Contains all known non-closed storage instances
   * that belong to this database.
   * Used in plugins and unit tests.
   */
  storageInstances = new Set();
  constructor(name,
  /**
   * Uniquely identifies the instance
   * of this RxDatabase.
   */
  token, storage, instanceCreationOptions, password, multiInstance, eventReduce = false, options = {},
  /**
   * Stores information documents about the collections of the database
   */
  internalStore, hashFunction, cleanupPolicy, allowSlowCount, reactivity, onClosed, liveQueryUpdateThrottleTime = 0) {
    this.name = name;
    this.token = token;
    this.storage = storage;
    this.instanceCreationOptions = instanceCreationOptions;
    this.password = password;
    this.multiInstance = multiInstance;
    this.eventReduce = eventReduce;
    this.options = options;
    this.internalStore = internalStore;
    this.hashFunction = hashFunction;
    this.cleanupPolicy = cleanupPolicy;
    this.allowSlowCount = allowSlowCount;
    this.reactivity = reactivity;
    this.onClosed = onClosed;
    this.liveQueryUpdateThrottleTime = liveQueryUpdateThrottleTime;
    DB_COUNT++;

    /**
     * SECURITY: Make the password property non-enumerable
     * so it does not leak through Object.keys(), spreading,
     * Object.assign(), for..in loops, or JSON.stringify().
     */
    if (typeof password !== 'undefined') {
      Object.defineProperty(this, 'password', {
        value: password,
        enumerable: false,
        writable: false,
        configurable: false
      });
    }

    /**
     * In the dev-mode, we create a pseudoInstance
     * to get all properties of RxDatabase and ensure they do not
     * conflict with the collection names etc.
     * So only if it is not pseudoInstance,
     * we have all values to prepare a real RxDatabase.
     *
     * TODO this is ugly, we should use a different way in the dev-mode
     * so that all non-dev-mode code can be cleaner.
     */
    if (this.name !== 'pseudoInstance') {
      /**
       * Wrap the internal store
       * to ensure that calls to it also end up in
       * calculation of the idle state and the hooks.
       */
      this.internalStore = getWrappedStorageInstance(this.asRxDatabase, internalStore, INTERNAL_STORE_SCHEMA);

      /**
       * Start writing the storage token.
       * Do not await the creation because it would run
       * in a critical path that increases startup time.
       *
       * Writing the token takes about 20 milliseconds
       * even on a fast adapter, so this is worth it.
       */
      this.storageTokenDocument = ensureStorageTokenDocumentExists(this.asRxDatabase).catch(err => this.startupErrors.push(err));
      this.storageToken = this.storageTokenDocument.then(doc => doc.data.token).catch(err => this.startupErrors.push(err));
    }
  }
  get $() {
    return this.observable$;
  }

  /**
   * Emits events whenever a JavaScript instance
   * of RxCollection is added or removed on the RxDatabase instance.
   * Does not emit anything across browser-tabs!
   */
  get collections$() {
    return this.collectionsSubject$.asObservable();
  }
  getReactivityFactory() {
    if (!this.reactivity) {
      throw newRxError('DB14', {
        database: this.name
      });
    }
    return this.reactivity;
  }
  _subs = [];

  /**
   * Because having unhandled exceptions would fail,
   * we have to store the async errors of the constructor here
   * so we can throw them later.
   */
  startupErrors = [];

  /**
   * When the database is closed,
   * these functions will be called an awaited.
   * Used to automatically clean up stuff that
   * belongs to this collection.
   */
  onClose = [];
  closed = false;
  collections = {};
  states = {};

  /**
   * Support for `using` / `await using` (ECMAScript explicit resource management).
   * This allows: `await using db = await createRxDatabase(...)`
   */
  async [Symbol.asyncDispose]() {
    await this.close();
  }

  /**
   * Internally only use eventBulks$
   * Do not use .$ or .observable$ because that has to transform
   * the events which decreases performance.
   */
  eventBulks$ = new Subject();
  closePromise = null;
  collectionsSubject$ = new Subject();
  observable$ = this.eventBulks$.pipe(mergeMap(changeEventBulk => rxChangeEventBulkToRxChangeEvents(changeEventBulk)));

  /**
   * Unique token that is stored with the data.
   * Used to detect if the dataset has been deleted
   * and if two RxDatabase instances work on the same dataset or not.
   *
   * Because reading and writing the storageToken runs in the hot path
   * of database creation, we do not await the storageWrites but instead
   * work with the promise when we need the value.
   */
  storageToken = PROMISE_RESOLVE_FALSE;
  /**
   * Stores the whole state of the internal storage token document.
   * We need this in some plugins.
   */
  storageTokenDocument = PROMISE_RESOLVE_FALSE;

  /**
   * Contains the ids of all event bulks that have been emitted
   * by the database.
   * Used to detect duplicates that come in again via BroadcastChannel
   * or other streams.
   * In the past we tried to remove this and to ensure
   * all storages only emit the same event bulks only once
   * but it turns out this is just not possible for all storages.
   * JavaScript processes, workers and browser tabs can be closed and started at any time
   * which can cause cases where it is not possible to know if an event bulk has been emitted already.
   */
  emittedEventBulkIds = new ObliviousSet(60 * 1000);

  /**
   * This is the main handle-point for all change events
   * ChangeEvents created by this instance go:
   * RxDocument -> RxCollection -> RxDatabase.$emit -> MultiInstance
   * ChangeEvents created by other instances go:
   * MultiInstance -> RxDatabase.$emit -> RxCollection -> RxDatabase
   */
  $emit(changeEventBulk) {
    if (this.emittedEventBulkIds.has(changeEventBulk.id)) {
      return;
    }
    this.emittedEventBulkIds.add(changeEventBulk.id);
    this.eventBulks$.next(changeEventBulk);
  }

  /**
   * removes the collection-doc from the internalStore
   */
  async removeCollectionDoc(name, schema) {
    var doc = await getSingleDocument(this.internalStore, getPrimaryKeyOfInternalDocument(_collectionNamePrimary(name, schema), INTERNAL_CONTEXT_COLLECTION));
    if (!doc) {
      throw newRxError('SNH', {
        name,
        schema
      });
    }
    var writeDoc = flatCloneDocWithMeta(doc);
    writeDoc._deleted = true;
    await this.internalStore.bulkWrite([{
      document: writeDoc,
      previous: doc
    }], 'rx-database-remove-collection');
  }

  /**
   * creates multiple RxCollections at once
   * to be much faster by saving db txs and doing stuff in bulk-operations
   * This function is not called often, but mostly in the critical path at the initial page load
   * So it must be as fast as possible.
   */
  async addCollections(collectionCreators) {
    var jsonSchemas = {};
    var schemas = {};
    var bulkPutDocs = [];
    var useArgsByCollectionName = {};
    await Promise.all(Object.entries(collectionCreators).map(async ([name, args]) => {
      var collectionName = name;
      var rxJsonSchema = args.schema;
      jsonSchemas[collectionName] = rxJsonSchema;
      var schema = createRxSchema(rxJsonSchema, this.hashFunction);
      schemas[collectionName] = schema;

      // collection already exists
      if (this.collections[name]) {
        throw newRxError('DB3', {
          name
        });
      }
      var collectionNameWithVersion = _collectionNamePrimary(name, rxJsonSchema);
      var collectionDocData = {
        id: getPrimaryKeyOfInternalDocument(collectionNameWithVersion, INTERNAL_CONTEXT_COLLECTION),
        key: collectionNameWithVersion,
        context: INTERNAL_CONTEXT_COLLECTION,
        data: {
          name: collectionName,
          schemaHash: await schema.hash,
          schema: schema.jsonSchema,
          version: schema.version,
          connectedStorages: []
        },
        _deleted: false,
        _meta: getDefaultRxDocumentMeta(),
        _rev: getDefaultRevision(),
        _attachments: {}
      };
      bulkPutDocs.push({
        document: collectionDocData
      });
      var useArgs = Object.assign({}, args, {
        name: collectionName,
        schema,
        database: this
      });

      // run hooks
      var hookData = flatClone(args);
      hookData.database = this;
      hookData.name = name;
      runPluginHooks('preCreateRxCollection', hookData);
      useArgs.conflictHandler = hookData.conflictHandler;
      useArgsByCollectionName[collectionName] = useArgs;
    }));

    /**
     * Optimization: Start creating collection storage instances
     * in parallel with the internal store bulkWrite and startup error check.
     * Storage instance creation is independent of the internal store write,
     * so we can overlap these I/O operations to reduce time-to-first-insert.
     */
    var collectionStorageInstancePromises = {};
    Object.keys(collectionCreators).forEach(collectionName => {
      var useArgs = useArgsByCollectionName[collectionName];
      var storageInstanceCreationParams = {
        databaseInstanceToken: this.token,
        databaseName: this.name,
        collectionName: collectionName,
        schema: useArgs.schema.jsonSchema,
        options: useArgs.instanceCreationOptions || {},
        multiInstance: this.multiInstance,
        password: this.password,
        devMode: overwritable.isDevMode()
      };
      runPluginHooks('preCreateRxStorageInstance', storageInstanceCreationParams);
      var promise = createRxCollectionStorageInstance(this.asRxDatabase, storageInstanceCreationParams);
      /**
       * Prevent unhandled promise rejection warnings.
       * If ensureNoStartupErrors() or the bulkWrite error handling throws
       * (e.g. password mismatch, schema mismatch), these promises might
       * never be awaited. The actual errors are still propagated when
       * the promises are awaited in Phase 5 below.
       */
      promise.catch(() => {});
      collectionStorageInstancePromises[collectionName] = promise;
    });

    /**
     * If the ensureNoStartupErrors or the bulkWrite error handling throws,
     * we must close any pre-created storage instances to avoid resource leaks.
     */
    var putDocsResult;
    try {
      [putDocsResult] = await Promise.all([this.internalStore.bulkWrite(bulkPutDocs, 'rx-database-add-collection'), ensureNoStartupErrors(this)]);
      await Promise.all(putDocsResult.error.map(async error => {
        if (error.status !== 409) {
          throw newRxError('DB12', {
            database: this.name,
            writeError: error
          });
        }
        var docInDb = ensureNotFalsy(error.documentInDb);
        var collectionName = docInDb.data.name;
        var schema = schemas[collectionName];
        // collection already exists but has different schema
        if (docInDb.data.schemaHash !== (await schema.hash) &&
        /**
         * sortObject() uses localeCompare() which depends on the device locale,
         * so the same schema can produce a different key order and hash
         * on another device. Only when the hashes differ, we compare the content
         * so that this expensive check does not run on each startup.
         * @link https://github.com/pubkey/rxdb/issues/9128
         */
        !deepEqual(docInDb.data.schema, schema.jsonSchema)) {
          throw newRxError('DB6', {
            database: this.name,
            collection: collectionName,
            previousSchemaHash: docInDb.data.schemaHash,
            schemaHash: await schema.hash,
            previousSchema: docInDb.data.schema,
            schema: ensureNotFalsy(jsonSchemas[collectionName])
          });
        }
      }));
    } catch (err) {
      /**
       * Close any pre-created storage instances on error.
       * Some instances might have failed to create (rejected promise),
       * so we catch and ignore errors during cleanup.
       */
      await Promise.all(Object.values(collectionStorageInstancePromises).map(p => p.then(instance => instance.close()).catch(() => {})));
      throw err;
    }
    var ret = {};
    await Promise.all(Object.keys(collectionCreators).map(async collectionName => {
      var useArgs = useArgsByCollectionName[collectionName];
      var storageInstance = await collectionStorageInstancePromises[collectionName];
      var collection = await createRxCollection({
        ...useArgs,
        storageInstance
      });
      ret[collectionName] = collection;

      // set as getter to the database
      this.collections[collectionName] = collection;
      this.collectionsSubject$.next({
        collection,
        type: 'ADDED'
      });
      if (!this[collectionName]) {
        Object.defineProperty(this, collectionName, {
          get: () => this.collections[collectionName]
        });
      }
    }));
    return ret;
  }

  /**
   * runs the given function between idleQueue-locking
   */
  lockedRun(fn) {
    return this.idleQueue.wrapCall(fn);
  }
  requestIdlePromise() {
    return this.idleQueue.requestIdlePromise();
  }

  /**
   * Export database to a JSON friendly format.
   */

  exportJSON(_collectionsOrOptions, _options) {
    throw pluginMissing('json-dump');
  }
  addState(_name) {
    throw pluginMissing('state');
  }

  /**
   * Import the parsed JSON export into the collection.
   * @param _exportedJSON The previously exported data from the `<db>.exportJSON()` method.
   * @note When an interface is loaded in this collection all base properties of the type are typed as `any`
   * since data could be encrypted.
   */
  importJSON(_exportedJSON) {
    throw pluginMissing('json-dump');
  }
  backup(_options) {
    throw pluginMissing('backup');
  }
  leaderElector() {
    throw pluginMissing('leader-election');
  }
  isLeader() {
    throw pluginMissing('leader-election');
  }
  /**
   * returns a promise which resolves when the instance becomes leader
   */
  waitForLeadership() {
    throw pluginMissing('leader-election');
  }
  migrationStates() {
    throw pluginMissing('migration-schema');
  }

  /**
   * closes the database-instance and all collections
   */
  close() {
    if (this.closePromise) {
      return this.closePromise;
    }
    var {
      promise,
      resolve,
      reject
    } = createPromiseWithResolvers();
    var resolveClosePromise = result => {
      if (this.onClosed) {
        this.onClosed();
      }
      this.closed = true;
      resolve(result);
    };
    this.closePromise = promise;
    (async () => {
      await runAsyncPluginHooks('preCloseRxDatabase', this);
      /**
       * Complete the event stream
       * to stop all subscribers who forgot to unsubscribe.
       */
      this.eventBulks$.complete();
      this.collectionsSubject$.complete();
      DB_COUNT--;
      this._subs.map(sub => sub.unsubscribe());

      /**
       * closing the pseudo instance will throw
       * because stuff is missing
       * TODO we should not need the pseudo instance on runtime.
       * we should generate the property list on build time.
       */
      if (this.name === 'pseudoInstance') {
        resolveClosePromise(false);
        return;
      }

      /**
       * First wait until the database is idle
       */
      await this.requestIdlePromise();
      var closingError = null;
      try {
        await Promise.all(this.onClose.map(fn => fn()));
      } catch (err) {
        closingError = err;
      }

      // close all collections
      await Promise.all(Object.keys(this.collections).map(key => this.collections[key]).map(col => col.close()));

      // close internal storage instances
      await this.internalStore.close();
      if (closingError) {
        throw closingError;
      }
      resolveClosePromise(true);
    })().catch(err => {
      /**
       * If an error occurs during closing,
       * we still have to mark the database as closed
       * and clean up the instance tracking so that
       * a new database with the same name can be created.
       */
      if (this.onClosed) {
        this.onClosed();
      }
      this.closed = true;
      reject(err);
    });
    return promise;
  }

  /**
   * deletes the database and its stored data.
   * Returns the names of all removed collections.
   */
  async remove() {
    /**
     * Collect all collection onRemove handlers before closing,
     * because close() will clear the collections and unsubscribe
     * all listeners, making it impossible to trigger onRemove afterwards.
     */
    var collections = Object.values(this.collections);
    await this.close();
    await Promise.all(collections.map(col => Promise.all(col.onRemove.map(fn => fn()))));
    return removeRxDatabase(this.name, this.storage, this.multiInstance, this.password);
  }
  get asRxDatabase() {
    return this;
  }
  registerWebMCP(_options) {
    throw pluginMissing('webmcp');
  }
}

/**
 * checks if an instance with same name and storage already exists
 * @throws {RxError} if used
 */
function throwIfDatabaseNameUsed(name, storage) {
  if (USED_DATABASE_NAMES.has(getDatabaseNameKey(name, storage))) {
    throw newRxError('DB8', {
      name,
      storage: storage.name,
      link: 'https://rxdb.info/rx-database.html#ignoreduplicate'
    });
  }
}

/**
 * Polyfill for https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/withResolvers
 */
function createPromiseWithResolvers() {
  var resolve;
  var reject;
  var promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return {
    promise,
    resolve,
    reject
  };
}
function getDatabaseNameKey(name, storage) {
  return storage.name + '|' + name;
}

/**
 * Creates the storage instances that are used internally in the database
 * to store schemas and other configuration stuff.
 */
export async function createRxDatabaseStorageInstance(databaseInstanceToken, storage, databaseName, options, multiInstance, password) {
  var internalStore = await storage.createStorageInstance({
    databaseInstanceToken,
    databaseName,
    collectionName: INTERNAL_STORAGE_NAME,
    schema: INTERNAL_STORE_SCHEMA,
    options,
    multiInstance,
    password,
    devMode: overwritable.isDevMode()
  });
  return internalStore;
}

/**
 * Creates an RxDatabase instance.
 *
 * In development, add the dev-mode plugin via `addRxPlugin()` **before** calling
 * this function. The dev-mode plugin enables schema validation, detailed error
 * messages, and other helpful runtime checks that are stripped from production builds.
 *
 * Need help? The RxDB Discord is the fastest place to reach the maintainers.
 * @see https://rxdb.info/chat/?console=code RxDB Discord community and support
 *
 * @example
 * ```ts
 * import { createRxDatabase, addRxPlugin } from 'rxdb';
 * import { RxDBDevModePlugin } from 'rxdb/plugins/dev-mode';
 * import { wrappedValidateAjvStorage } from 'rxdb/plugins/validate-ajv';
 * import { getRxStorageDexie } from 'rxdb/plugins/storage-dexie';
 *
 * // Add dev-mode plugin first, before any other RxDB code runs.
 * if (process.env.NODE_ENV !== 'production') {
 *   addRxPlugin(RxDBDevModePlugin);
 * }
 *
 * const db = await createRxDatabase({
 *   name: 'mydb',
 *   // In dev-mode, wrap the storage with a validator so every write is
 *   // checked against the schema and errors surface immediately.
 *   storage: process.env.NODE_ENV !== 'production'
 *     ? wrappedValidateAjvStorage({ storage: getRxStorageDexie() })
 *     : getRxStorageDexie()
 * });
 * ```
 */
export function createRxDatabase({
  storage,
  instanceCreationOptions,
  name,
  password,
  multiInstance = true,
  eventReduce = true,
  ignoreDuplicate = false,
  options = {},
  cleanupPolicy,
  closeDuplicates = false,
  allowSlowCount = false,
  localDocuments = false,
  hashFunction = defaultHashSha256,
  reactivity,
  liveQueryUpdateThrottleTime = 0
}) {
  runPluginHooks('preCreateRxDatabase', {
    storage,
    instanceCreationOptions,
    name,
    password,
    multiInstance,
    eventReduce,
    ignoreDuplicate,
    options,
    localDocuments
  });
  var databaseNameKey = getDatabaseNameKey(name, storage);
  var databaseNameKeyUnclosedInstancesSet = DATABASE_UNCLOSED_INSTANCE_PROMISE_MAP.get(databaseNameKey) || new Set();
  var instancePromiseWithResolvers = createPromiseWithResolvers();
  var closeDuplicatesPromises = Array.from(databaseNameKeyUnclosedInstancesSet);
  var onInstanceClosed = () => {
    databaseNameKeyUnclosedInstancesSet.delete(instancePromiseWithResolvers.promise);
    USED_DATABASE_NAMES.delete(databaseNameKey);
  };
  databaseNameKeyUnclosedInstancesSet.add(instancePromiseWithResolvers.promise);
  DATABASE_UNCLOSED_INSTANCE_PROMISE_MAP.set(databaseNameKey, databaseNameKeyUnclosedInstancesSet);
  (async () => {
    if (closeDuplicates) {
      await Promise.all(closeDuplicatesPromises.map(unclosedInstancePromise => unclosedInstancePromise.catch(() => null).then(instance => instance && instance.close())));
    }
    if (ignoreDuplicate) {
      if (!overwritable.isDevMode()) {
        throw newRxError('DB9', {
          database: name
        });
      }
    } else {
      // check if combination already used
      throwIfDatabaseNameUsed(name, storage);
    }
    USED_DATABASE_NAMES.add(databaseNameKey);
    var databaseInstanceToken = randomToken(10);
    var storageInstance = await createRxDatabaseStorageInstance(databaseInstanceToken, storage, name, instanceCreationOptions, multiInstance, password);
    var rxDatabase = new RxDatabaseBase(name, databaseInstanceToken, storage, instanceCreationOptions, password, multiInstance, eventReduce, options, storageInstance, hashFunction, cleanupPolicy, allowSlowCount, reactivity, onInstanceClosed, liveQueryUpdateThrottleTime);
    await runAsyncPluginHooks('createRxDatabase', {
      database: rxDatabase,
      creator: {
        storage,
        instanceCreationOptions,
        name,
        password,
        multiInstance,
        eventReduce,
        ignoreDuplicate,
        options,
        localDocuments
      }
    });
    return rxDatabase;
  })().then(rxDatabase => {
    instancePromiseWithResolvers.resolve(rxDatabase);
  }).catch(err => {
    instancePromiseWithResolvers.reject(err);
    onInstanceClosed();
  });
  return instancePromiseWithResolvers.promise;
}

/**
 * Removes the database and all its known data
 * with all known collections and all internal meta data.
 *
 * Returns the names of the removed collections.
 */
export async function removeRxDatabase(databaseName, storage, multiInstance = true, password) {
  var databaseInstanceToken = randomToken(10);
  var dbInternalsStorageInstance = await createRxDatabaseStorageInstance(databaseInstanceToken, storage, databaseName, {}, multiInstance, password);
  var collectionDocs = await getAllCollectionDocuments(dbInternalsStorageInstance);
  var collectionNames = new Set();
  collectionDocs.forEach(doc => collectionNames.add(doc.data.name));
  var removedCollectionNames = Array.from(collectionNames);
  await Promise.all(removedCollectionNames.map(collectionName => removeCollectionStorages(storage, dbInternalsStorageInstance, databaseInstanceToken, databaseName, collectionName, multiInstance, password)));
  await runAsyncPluginHooks('postRemoveRxDatabase', {
    databaseName,
    storage
  });
  await dbInternalsStorageInstance.remove();
  return removedCollectionNames;
}
export function isRxDatabase(obj) {
  return obj instanceof RxDatabaseBase;
}
export function dbCount() {
  return DB_COUNT;
}

/**
 * Returns true if the given RxDatabase was the first
 * instance that was created on the storage with this name.
 *
 * Can be used for some optimizations because on the first instantiation,
 * we can assume that no data was written before.
 */
export async function isRxDatabaseFirstTimeInstantiated(database) {
  var tokenDoc = await database.storageTokenDocument;
  return tokenDoc.data.instanceToken === database.token;
}

/**
 * For better performance some tasks run async
 * and are awaited later.
 * But we still have to ensure that there have been no errors
 * on database creation.
 */
export async function ensureNoStartupErrors(rxDatabase) {
  await rxDatabase.storageToken;
  if (rxDatabase.startupErrors[0]) {
    throw rxDatabase.startupErrors[0];
  }
}
//# sourceMappingURL=rx-database.js.map