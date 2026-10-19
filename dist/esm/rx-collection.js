import { filter, map, mergeMap } from 'rxjs';
import { ucfirst, flatClone, promiseSeries, pluginMissing, ensureNotFalsy, getFromMapOrThrow, PROMISE_RESOLVE_FALSE, PROMISE_RESOLVE_VOID, NON_PREMIUM_COLLECTION_LIMIT, hasPremiumFlag } from "./plugins/utils/index.js";
import { fillObjectDataBeforeInsert, normalizeInlineAttachments, createRxCollectionStorageInstance, removeCollectionStorages, ensureRxCollectionIsNotClosed, isWriteAllowed } from "./rx-collection-helper.js";
import { createRxQuery, _getDefaultQuery } from "./rx-query.js";
import { newRxError, newRxTypeError } from "./rx-error.js";
import { DocumentCache, mapDocumentsDataToCacheDocs } from "./doc-cache.js";
import { createQueryCache, defaultCacheReplacementPolicy } from "./query-cache.js";
import { createChangeEventBuffer } from "./change-event-buffer.js";
import { runAsyncPluginHooks, runPluginHooks } from "./hooks.js";
import { fillPrimaryKey } from "./rx-schema-helper.js";
import { createNewRxDocument, getRxDocumentConstructor } from "./rx-document-prototype-merge.js";
import { getWrappedStorageInstance, getWrittenDocumentsFromBulkWriteResponse, throwIfIsStorageWriteError, RX_COLLECTION_BULK_INSERT_CONTEXT } from "./rx-storage-helper.js";
import { IncrementalWriteQueue } from "./incremental-write.js";
import { beforeDocumentUpdateWrite } from "./rx-document.js";
import { overwritable } from "./overwritable.js";
import { defaultConflictHandler } from "./replication-protocol/default-conflict-handler.js";
import { rxChangeEventBulkToRxChangeEvents } from "./rx-change-event.js";
var HOOKS_WHEN = ['pre', 'post'];
var HOOKS_KEYS = ['insert', 'save', 'remove', 'create'];
var hooksApplied = false;
export var OPEN_COLLECTIONS = new Set();
export class RxCollectionBase {
  /**
   * Stores all 'normal' documents
   */
  storageInstance = {};
  timeouts = new Set();
  incrementalWriteQueue = {};

  /**
   * Before reads, all these methods are awaited. Used to "block" reads
   * depending on other processes, like when the RxPipeline is running.
   */
  awaitBeforeReads = new Set();
  constructor(database, name, schema, internalStorageInstance, instanceCreationOptions = {}, migrationStrategies = {}, methods = {}, attachments = {}, options = {}, cacheReplacementPolicy = defaultCacheReplacementPolicy, statics = {}, conflictHandler = defaultConflictHandler, liveQueryUpdateThrottleTime = undefined) {
    this.database = database;
    this.name = name;
    this.schema = schema;
    this.internalStorageInstance = internalStorageInstance;
    this.instanceCreationOptions = instanceCreationOptions;
    this.migrationStrategies = migrationStrategies;
    this.methods = methods;
    this.attachments = attachments;
    this.options = options;
    this.cacheReplacementPolicy = cacheReplacementPolicy;
    this.statics = statics;
    this.conflictHandler = conflictHandler;
    this.liveQueryUpdateThrottleTime = liveQueryUpdateThrottleTime;
    _applyHookFunctions(this.asRxCollection);
    if (database) {
      // might be falsy on pseudoInstance
      this.eventBulks$ = database.eventBulks$.pipe(filter(changeEventBulk => changeEventBulk.collectionName === this.name));
    } else {}
  }
  get insert$() {
    return this.$.pipe(filter(cE => cE.operation === 'INSERT'));
  }
  get update$() {
    return this.$.pipe(filter(cE => cE.operation === 'UPDATE'));
  }
  get remove$() {
    return this.$.pipe(filter(cE => cE.operation === 'DELETE'));
  }
  _incrementalUpsertQueues = new Map();
  // defaults
  synced = false;
  hooks = {};
  _subs = [];
  _docCache = {};
  _queryCache = createQueryCache();
  $ = {};
  checkpoint$ = {};
  _changeEventBuffer = {};

  /**
   * Internally only use eventBulks$
   * Do not use .$ or .observable$ because that has to transform
   * the events which decreases performance.
   */
  eventBulks$ = {};

  /**
   * When the collection is closed,
   * these functions will be called an awaited.
   * Used to automatically clean up stuff that
   * belongs to this collection.
  */
  onClose = [];
  closed = false;

  /**
   * Set to true while a schema migration is running for this collection.
   * Writes are blocked while this is true to ensure the migration
   * replication can fill the new storage without external interference.
   */
  migrationInProgress = false;
  onRemove = [];
  async prepare() {
    if (!(await hasPremiumFlag())) {
      /**
       * When used in a test suite, we often open and close many databases with collections
       * while not awaiting the database.close() call to improve the test times.
       * So when reopening collections and the OPEN_COLLECTIONS size is full,
       * we retry after some times to account for this.
       * Use a higher retry count to account for slow storage backends like MongoDB
       * where tests hold collections open for several seconds.
       */
      var count = 0;
      var startTime = Date.now();
      while (count < 60 && OPEN_COLLECTIONS.size >= NON_PREMIUM_COLLECTION_LIMIT) {
        await this.promiseWait(30);
        count++;
      }
      if (OPEN_COLLECTIONS.size > NON_PREMIUM_COLLECTION_LIMIT) {
        var timeInRetry = Date.now() - startTime;
        throw newRxError('COL23', {
          database: this.database.name,
          collection: this.name,
          args: {
            timeInRetry,
            existing: Array.from(OPEN_COLLECTIONS.values()).map(c => ({
              db: c.database ? c.database.name : '',
              c: c.name
            }))
          }
        });
      }
    }

    /**
     * Must be after the check because the hooks might throw on dev-mode
     * checks and we do not want to have broken collections here.
     * RxCollection instances created for testings do not have a database
     * so we do not add these to the list.
     */
    if (this.database) {
      OPEN_COLLECTIONS.add(this);
    }
    this.storageInstance = getWrappedStorageInstance(this.database, this.internalStorageInstance, this.schema.jsonSchema);
    this.incrementalWriteQueue = new IncrementalWriteQueue(this.storageInstance, this.schema.primaryPath, (newData, oldData) => beforeDocumentUpdateWrite(this, newData, oldData), result => this._runHooks('post', 'save', result, this._docCache.getCachedRxDocument(result)));
    this.$ = this.eventBulks$.pipe(mergeMap(changeEventBulk => rxChangeEventBulkToRxChangeEvents(changeEventBulk)));
    this.checkpoint$ = this.eventBulks$.pipe(map(changeEventBulk => changeEventBulk.checkpoint));
    this._changeEventBuffer = createChangeEventBuffer(this.asRxCollection);
    var documentConstructor;
    this._docCache = new DocumentCache(this.schema.primaryPath, this.eventBulks$.pipe(filter(bulk => !bulk.isLocal), map(bulk => bulk.events)), docData => {
      if (!documentConstructor) {
        documentConstructor = getRxDocumentConstructor(this.asRxCollection);
      }
      return createNewRxDocument(this.asRxCollection, documentConstructor, docData);
    });
    var listenToRemoveSub = this.database.internalStore.changeStream().pipe(filter(bulk => {
      var key = this.name + '-' + this.schema.version;
      var found = bulk.events.find(event => {
        return event.documentData.context === 'collection' && event.documentData.key === key && event.operation === 'DELETE';
      });
      return !!found;
    })).subscribe(async () => {
      await this.close();
      await Promise.all(this.onRemove.map(fn => fn()));
    });
    this._subs.push(listenToRemoveSub);
    var databaseStorageToken = await this.database.storageToken;
    var subDocs = this.storageInstance.changeStream().subscribe(eventBulk => {
      var changeEventBulk = {
        id: eventBulk.id,
        isLocal: false,
        internal: false,
        collectionName: this.name,
        storageToken: databaseStorageToken,
        events: eventBulk.events,
        databaseToken: this.database.token,
        checkpoint: eventBulk.checkpoint,
        context: eventBulk.context
      };
      this.database.$emit(changeEventBulk);
    });
    this._subs.push(subDocs);
    return PROMISE_RESOLVE_VOID;
  }

  /**
   * Manually call the cleanup function of the storage.
   * @link https://rxdb.info/cleanup.html
   */
  cleanup(_minimumDeletedTime) {
    ensureRxCollectionIsNotClosed(this);
    throw pluginMissing('cleanup');
  }

  // overwritten by migration-plugin
  migrationNeeded() {
    throw pluginMissing('migration-schema');
  }
  getMigrationState() {
    throw pluginMissing('migration-schema');
  }
  startMigration(batchSize = 10) {
    ensureRxCollectionIsNotClosed(this);
    return this.getMigrationState().startMigration(batchSize);
  }
  migratePromise(batchSize = 10) {
    return this.getMigrationState().migratePromise(batchSize);
  }
  async insert(json) {
    isWriteAllowed(this);
    var writeResult = await this.bulkInsert([json]);
    var isError = writeResult.error[0];
    throwIfIsStorageWriteError(this, json[this.schema.primaryPath], json, isError);
    var insertResult = ensureNotFalsy(writeResult.success[0]);
    return insertResult;
  }
  async insertIfNotExists(json) {
    var writeResult = await this.bulkInsert([json]);
    if (writeResult.error.length > 0) {
      var error = writeResult.error[0];
      if (error.status === 409) {
        var conflictDocData = error.documentInDb;
        return mapDocumentsDataToCacheDocs(this._docCache, [conflictDocData])[0];
      } else {
        throw error;
      }
    }
    return writeResult.success[0];
  }
  async bulkInsert(docsData) {
    var _this = this;
    isWriteAllowed(this);
    /**
     * Optimization shortcut,
     * do nothing when called with an empty array
    */
    if (docsData.length === 0) {
      return {
        success: [],
        error: []
      };
    }
    var primaryPath = this.schema.primaryPath;
    var ids = new Set();

    /**
     * This code is a bit redundant for better performance.
     * Instead of iterating multiple times,
     * we directly transform the input to a write-row array.
     */
    var insertRows;
    if (this.hasHooks('pre', 'insert')) {
      insertRows = await Promise.all(docsData.map(docData => {
        var useDocData = fillObjectDataBeforeInsert(this.schema, docData, true);
        return this._runHooks('pre', 'insert', useDocData).then(() => {
          if (typeof this.schema.jsonSchema.primaryKey !== 'string') {
            fillPrimaryKey(this.schema.primaryPath, this.schema.jsonSchema, useDocData);
          }
          ids.add(useDocData[primaryPath]);
          return {
            document: useDocData
          };
        });
      }));
    } else {
      insertRows = new Array(docsData.length);
      var _schema = this.schema;
      for (var index = 0; index < docsData.length; index++) {
        var docData = docsData[index];
        var useDocData = fillObjectDataBeforeInsert(_schema, docData);
        ids.add(useDocData[primaryPath]);
        insertRows[index] = {
          document: useDocData
        };
      }
    }
    if (ids.size !== docsData.length) {
      var duplicateIdSet = new Set();
      var seenIds = new Set();
      for (var row of insertRows) {
        var id = row.document[primaryPath];
        if (seenIds.has(id)) {
          duplicateIdSet.add(id);
        } else {
          seenIds.add(id);
        }
      }
      throw newRxError('COL22', {
        collection: this.name,
        duplicateIds: Array.from(duplicateIdSet),
        args: {
          documents: docsData
        }
      });
    }

    // Normalize any inline attachment inputs (compute digest/length from Blob)
    // Also converts array format to internal map format.
    // Only create promises for rows that actually need normalization to avoid
    // expensive await overhead on the hot path.
    if (this.schema.jsonSchema.attachments) {
      var normalizePromises = [];
      var _loop = async function () {
        var doc = _row.document;
        var atts = doc._attachments;
        if (atts == null || Object.keys(atts).length === 0) {
          doc._attachments = {};
        } else {
          normalizePromises.push(normalizeInlineAttachments(_this.database.hashFunction, atts).then(normalized => {
            doc._attachments = normalized;
          }));
        }
      };
      for (var _row of insertRows) {
        await _loop();
      }
      if (normalizePromises.length > 0) {
        await Promise.all(normalizePromises);
      }
    }
    var results = await this.storageInstance.bulkWrite(insertRows, RX_COLLECTION_BULK_INSERT_CONTEXT);

    /**
     * Often the user does not need to access the RxDocuments of the bulkInsert() call.
     * So we transform the data to RxDocuments only if needed to use less CPU performance.
     */
    var rxDocuments;
    var collection = this;
    var ret = {
      get success() {
        if (!rxDocuments) {
          var success = getWrittenDocumentsFromBulkWriteResponse(collection.schema.primaryPath, insertRows, results);
          rxDocuments = mapDocumentsDataToCacheDocs(collection._docCache, success);
        }
        return rxDocuments;
      },
      error: results.error
    };
    if (this.hasHooks('post', 'insert')) {
      var docsMap = new Map();
      insertRows.forEach(row => {
        var doc = row.document;
        docsMap.set(doc[primaryPath], doc);
      });
      await Promise.all(ret.success.map(doc => {
        return this._runHooks('post', 'insert', docsMap.get(doc.primary), doc);
      }));
    }
    return ret;
  }
  async bulkRemove(
  /**
   * You can either remove the documents by their ids
   * or by directly providing the RxDocument instances
   * if you have them already. This improves performance a bit.
   */
  idsOrDocs) {
    isWriteAllowed(this);
    var primaryPath = this.schema.primaryPath;
    /**
     * Optimization shortcut,
     * do nothing when called with an empty array
     */
    if (idsOrDocs.length === 0) {
      return {
        success: [],
        error: []
      };
    }
    var rxDocumentMap;
    if (typeof idsOrDocs[0] === 'string') {
      rxDocumentMap = await this.findByIds(idsOrDocs).exec();
    } else {
      rxDocumentMap = new Map();
      idsOrDocs.forEach(d => rxDocumentMap.set(d.primary, d));
    }
    var docsData = [];
    var docsMap = new Map();
    Array.from(rxDocumentMap.values()).forEach(rxDocument => {
      var data = rxDocument.toMutableJSON(true);
      docsData.push(data);
      docsMap.set(rxDocument.primary, data);
    });
    await Promise.all(docsData.map(doc => {
      var primary = doc[this.schema.primaryPath];
      return this._runHooks('pre', 'remove', doc, rxDocumentMap.get(primary));
    }));
    var removeDocs = docsData.map(doc => {
      var writeDoc = flatClone(doc);
      writeDoc._deleted = true;
      return {
        previous: doc,
        document: writeDoc
      };
    });
    var results = await this.storageInstance.bulkWrite(removeDocs, 'rx-collection-bulk-remove');
    var success = getWrittenDocumentsFromBulkWriteResponse(this.schema.primaryPath, removeDocs, results);
    var deletedRxDocuments = [];
    var successIds = success.map(d => {
      var id = d[primaryPath];
      var doc = this._docCache.getCachedRxDocument(d);
      deletedRxDocuments.push(doc);
      return id;
    });

    // run hooks
    await Promise.all(successIds.map(id => {
      return this._runHooks('post', 'remove', docsMap.get(id), rxDocumentMap.get(id));
    }));
    return {
      success: deletedRxDocuments,
      error: results.error
    };
  }

  /**
   * same as bulkInsert but overwrites existing document with same primary
   */
  async bulkUpsert(docsData, options) {
    var _this2 = this;
    isWriteAllowed(this);
    var insertData = [];
    var useJsonByDocId = new Map();

    // First pass: synchronous work — schema filling and primary key validation
    var preparedDocs = [];
    for (var docData of docsData) {
      var useJson = fillObjectDataBeforeInsert(this.schema, docData);
      var primary = useJson[this.schema.primaryPath];
      if (!primary) {
        throw newRxError('COL3', {
          primaryPath: this.schema.primaryPath,
          data: useJson,
          schema: this.schema.jsonSchema
        });
      }
      preparedDocs.push(useJson);
    }

    // Second pass: normalize inline attachments concurrently across all documents.
    // Only create promises for docs that actually need normalization to avoid
    // expensive await overhead on the hot path.
    if (this.schema.jsonSchema.attachments) {
      var normalizePromises = [];
      var _loop2 = async function (_useJson) {
        var atts = _useJson._attachments;
        if (atts == null || Object.keys(atts).length === 0) {
          _useJson._attachments = {};
        } else {
          normalizePromises.push(normalizeInlineAttachments(_this2.database.hashFunction, atts).then(normalized => {
            _useJson._attachments = normalized;
          }));
        }
      };
      for (var _useJson of preparedDocs) {
        await _loop2(_useJson);
      }
      if (normalizePromises.length > 0) {
        await Promise.all(normalizePromises);
      }
    }
    for (var _useJson2 of preparedDocs) {
      var _primary = _useJson2[this.schema.primaryPath];
      useJsonByDocId.set(_primary, _useJson2);
      insertData.push(_useJson2);
    }
    var insertResult = await this.bulkInsert(insertData);
    var success = insertResult.success.slice(0);
    var error = [];

    // update the ones that existed already
    await Promise.all(insertResult.error.map(async err => {
      if (err.status !== 409) {
        error.push(err);
      } else {
        var id = err.documentId;
        var writeData = getFromMapOrThrow(useJsonByDocId, id);
        var docDataInDb = ensureNotFalsy(err.documentInDb);
        var newAttachments = writeData._attachments;
        var deleteExisting = options?.deleteExistingAttachments === true;
        var newDoc = await this.incrementalWriteQueue.addWrite(docDataInDb, docWriteData => {
          return Object.assign({}, writeData, {
            _attachments: deleteExisting ? newAttachments : Object.assign({}, docWriteData._attachments, newAttachments)
          });
        }).then(writeResult => this._docCache.getCachedRxDocument(writeResult));
        success.push(newDoc);
      }
    }));
    return {
      error,
      success
    };
  }

  /**
   * same as insert but overwrites existing document with same primary
   */
  async upsert(json, options) {
    isWriteAllowed(this);
    var bulkResult = await this.bulkUpsert([json], options);
    throwIfIsStorageWriteError(this.asRxCollection, json[this.schema.primaryPath], json, bulkResult.error[0]);
    return bulkResult.success[0];
  }

  /**
   * upserts to a RxDocument, uses incrementalModify if document already exists
   */
  incrementalUpsert(json, options) {
    isWriteAllowed(this);
    var useJson = fillObjectDataBeforeInsert(this.schema, json);
    var primary = useJson[this.schema.primaryPath];
    if (!primary) {
      throw newRxError('COL4', {
        data: json
      });
    }
    var hasAttachments = !!this.schema.jsonSchema.attachments;
    var deleteExisting = options?.deleteExistingAttachments === true;

    // ensure that it won't try 2 parallel runs
    var queue = this._incrementalUpsertQueues.get(primary);
    if (!queue) {
      queue = PROMISE_RESOLVE_VOID;
    }
    queue = queue
    // Normalize array-format attachments to map format
    .then(() => {
      if (hasAttachments) {
        var atts = useJson._attachments;
        if (atts == null || Object.keys(atts).length === 0) {
          return {};
        } else {
          return normalizeInlineAttachments(this.database.hashFunction, atts);
        }
      }
      return useJson._attachments;
    }).then(normalizedAttachments => {
      useJson._attachments = normalizedAttachments;
      return _incrementalUpsertEnsureRxDocumentExists(this, primary, useJson);
    }).then(wasInserted => {
      if (!wasInserted.inserted) {
        var doc = wasInserted.doc;
        var newAttachments = useJson._attachments;
        return this.incrementalWriteQueue.addWrite(doc._data, docWriteData => {
          return Object.assign({}, useJson, {
            _attachments: deleteExisting ? newAttachments : Object.assign({}, docWriteData._attachments, newAttachments)
          });
        }).then(writeResult => this._docCache.getCachedRxDocument(writeResult));
      } else {
        return wasInserted.doc;
      }
    });
    this._incrementalUpsertQueues.set(primary, queue);
    return queue;
  }
  find(queryObj) {
    ensureRxCollectionIsNotClosed(this);
    runPluginHooks('prePrepareRxQuery', {
      op: 'find',
      queryObj,
      collection: this
    });
    if (!queryObj) {
      queryObj = _getDefaultQuery();
    }
    var query = createRxQuery('find', queryObj, this);
    return query;
  }
  findOne(queryObj) {
    ensureRxCollectionIsNotClosed(this);
    runPluginHooks('prePrepareRxQuery', {
      op: 'findOne',
      queryObj,
      collection: this
    });
    var query;
    if (typeof queryObj === 'string') {
      query = createRxQuery('findOne', {
        selector: {
          [this.schema.primaryPath]: queryObj
        },
        limit: 1
      }, this);
    } else {
      if (!queryObj) {
        queryObj = _getDefaultQuery();
      }

      // cannot have limit on findOne queries because it will be overwritten
      if (queryObj.limit) {
        throw newRxError('QU6');
      }
      queryObj = flatClone(queryObj);
      queryObj.limit = 1;
      query = createRxQuery('findOne', queryObj, this);
    }
    return query;
  }
  count(queryObj) {
    ensureRxCollectionIsNotClosed(this);
    if (!queryObj) {
      queryObj = _getDefaultQuery();
    }
    var query = createRxQuery('count', queryObj, this);
    return query;
  }

  /**
   * find a list documents by their primary key
   * has way better performance then running multiple findOne() or a find() with a complex $or-selected
   */
  findByIds(ids) {
    ensureRxCollectionIsNotClosed(this);
    var mangoQuery = {
      selector: {
        [this.schema.primaryPath]: {
          $in: ids.slice(0)
        }
      }
    };
    var query = createRxQuery('findByIds', mangoQuery, this);
    return query;
  }

  /**
   * Export collection to a JSON friendly format.
   */

  exportJSON(_options) {
    throw pluginMissing('json-dump');
  }

  /**
   * Import the parsed JSON export into the collection.
   * @param _exportedJSON The previously exported data from the `<collection>.exportJSON()` method.
   */
  importJSON(_exportedJSON) {
    throw pluginMissing('json-dump');
  }
  registerWebMCP(_options) {
    throw pluginMissing('webmcp');
  }
  insertCRDT(_updateObj) {
    throw pluginMissing('crdt');
  }
  addPipeline(_options) {
    throw pluginMissing('pipeline');
  }

  /**
   * HOOKS
   */
  addHook(when, key, fun, parallel = false) {
    if (typeof fun !== 'function') {
      throw newRxTypeError('COL7', {
        key,
        when
      });
    }
    if (!HOOKS_WHEN.includes(when)) {
      throw newRxTypeError('COL8', {
        key,
        when
      });
    }
    if (!HOOKS_KEYS.includes(key)) {
      throw newRxError('COL9', {
        key
      });
    }
    if (when === 'post' && key === 'create' && parallel === true) {
      throw newRxError('COL10', {
        when,
        key,
        parallel
      });
    }

    // bind this-scope to hook-function
    var boundFun = fun.bind(this);
    var runName = parallel ? 'parallel' : 'series';
    if (!this.hooks[key]) {
      this.hooks[key] = {};
    }
    if (!this.hooks[key][when]) {
      this.hooks[key][when] = {
        series: [],
        parallel: []
      };
    }
    this.hooks[key][when][runName].push(boundFun);
  }
  getHooks(when, key) {
    if (!this.hooks[key] || !this.hooks[key][when]) {
      return {
        series: [],
        parallel: []
      };
    }
    return this.hooks[key][when];
  }
  hasHooks(when, key) {
    /**
     * Performance shortcut
     * so that we not have to build the empty object.
     */
    if (!this.hooks[key] || !this.hooks[key][when]) {
      return false;
    }
    var hooks = this.getHooks(when, key);
    if (!hooks) {
      return false;
    }
    return hooks.series.length > 0 || hooks.parallel.length > 0;
  }
  _runHooks(when, key, data, instance) {
    var hooks = this.getHooks(when, key);
    if (!hooks) {
      return PROMISE_RESOLVE_VOID;
    }

    // run parallel: false
    var tasks = hooks.series.map(hook => () => hook(data, instance));
    return promiseSeries(tasks)
    // run parallel: true
    .then(() => Promise.all(hooks.parallel.map(hook => hook(data, instance))));
  }

  /**
   * does the same as ._runHooks() but with non-async-functions
   */
  _runHooksSync(when, key, data, instance) {
    if (!this.hasHooks(when, key)) {
      return;
    }
    var hooks = this.getHooks(when, key);
    if (!hooks) return;
    hooks.series.forEach(hook => hook(data, instance));
  }

  /**
   * Returns a promise that resolves after the given time.
   * Ensures that is properly cleans up when the collection is closed
   * so that no running timeouts prevent the exit of the JavaScript process.
   */
  promiseWait(time) {
    var ret = new Promise(res => {
      var timeout = setTimeout(() => {
        this.timeouts.delete(timeout);
        res();
      }, time);
      this.timeouts.add(timeout);
    });
    return ret;
  }
  async close() {
    if (this.closed) {
      return PROMISE_RESOLVE_FALSE;
    }
    OPEN_COLLECTIONS.delete(this);
    await Promise.all(this.onClose.map(fn => fn()));

    /**
     * Settings closed = true
     * must be the first thing to do,
     * so for example the replication can directly stop
     * instead of sending requests to a closed storage.
     */
    this.closed = true;
    Array.from(this.timeouts).forEach(timeout => clearTimeout(timeout));
    if (this._changeEventBuffer) {
      this._changeEventBuffer.close();
    }
    /**
     * First wait until the whole database is idle.
     * This ensures that the storage does not get closed
     * while some operation is running.
     * It is important that we do not intercept a running call
     * because it might lead to undefined behavior like when a doc is written
     * but the change is not added to the changes collection.
     */
    return this.database.requestIdlePromise().then(() => this.storageInstance.close()).then(() => {
      /**
       * Unsubscribing must be done AFTER the storageInstance.close()
       * Because the conflict handling is part of the subscriptions and
       * otherwise there might be open conflicts to be resolved which
       * will then stuck and never resolve.
       */
      this._subs.forEach(sub => sub.unsubscribe());
      delete this.database.collections[this.name];
      this.database.collectionsSubject$.next({
        collection: this.asRxCollection,
        type: 'CLOSED'
      });
      return runAsyncPluginHooks('postCloseRxCollection', this).then(() => true);
    });
  }

  /**
   * remove all data of the collection
   */
  async remove() {
    await this.close();
    await Promise.all(this.onRemove.map(fn => fn()));
    /**
     * TODO here we should pass the already existing
     * storage instances instead of creating new ones.
     */
    await removeCollectionStorages(this.database.storage, this.database.internalStore, this.database.token, this.database.name, this.name, this.database.multiInstance, this.database.password, this.database.hashFunction);
  }
  get asRxCollection() {
    return this;
  }
}

/**
 * adds the hook-functions to the collections prototype
 * this runs only once
 */
function _applyHookFunctions(collection) {
  if (hooksApplied) return; // already run
  hooksApplied = true;
  var colProto = Object.getPrototypeOf(collection);
  HOOKS_KEYS.forEach(key => {
    HOOKS_WHEN.map(when => {
      var fnName = when + ucfirst(key);
      colProto[fnName] = function (fun, parallel) {
        return this.addHook(when, key, fun, parallel);
      };
    });
  });
}

/**
 * ensures that the given document exists
 * @return promise that resolves with new doc and flag if inserted
 */
function _incrementalUpsertEnsureRxDocumentExists(rxCollection, primary, json) {
  /**
   * Optimisation shortcut,
   * first try to find the document in the doc-cache
   */
  var docDataFromCache = rxCollection._docCache.getLatestDocumentDataIfExists(primary);
  if (docDataFromCache) {
    return Promise.resolve({
      doc: rxCollection._docCache.getCachedRxDocuments([docDataFromCache])[0],
      inserted: false
    });
  }
  return rxCollection.findOne(primary).exec().then(doc => {
    if (!doc) {
      return rxCollection.insert(json).then(newDoc => ({
        doc: newDoc,
        inserted: true
      })).catch(err => {
        /**
         * If the insert fails with a conflict error,
         * it means another concurrent operation already
         * inserted a document with the same primary key
         * between our findOne() and insert() calls.
         * Re-run the whole function which will now find the
         * existing document via cache or query.
         */
        if (err.code === 'CONFLICT') {
          return _incrementalUpsertEnsureRxDocumentExists(rxCollection, primary, json);
        }
        throw err;
      });
    } else {
      return {
        doc,
        inserted: false
      };
    }
  });
}

/**
 * creates and prepares a new collection
 */
export async function createRxCollection({
  database,
  name,
  schema,
  instanceCreationOptions = {},
  migrationStrategies = {},
  autoMigrate = true,
  statics = {},
  methods = {},
  attachments = {},
  options = {},
  localDocuments = false,
  cacheReplacementPolicy = defaultCacheReplacementPolicy,
  conflictHandler = defaultConflictHandler,
  storageInstance,
  liveQueryUpdateThrottleTime = undefined
}) {
  if (!storageInstance) {
    var storageInstanceCreationParams = {
      databaseInstanceToken: database.token,
      databaseName: database.name,
      collectionName: name,
      schema: schema.jsonSchema,
      options: instanceCreationOptions,
      multiInstance: database.multiInstance,
      password: database.password,
      devMode: overwritable.isDevMode()
    };
    runPluginHooks('preCreateRxStorageInstance', storageInstanceCreationParams);
    storageInstance = await createRxCollectionStorageInstance(database, storageInstanceCreationParams);
  }
  var collection = new RxCollectionBase(database, name, schema, storageInstance, instanceCreationOptions, migrationStrategies, methods, attachments, options, cacheReplacementPolicy, statics, conflictHandler, liveQueryUpdateThrottleTime);
  try {
    await collection.prepare();

    // ORM add statics
    Object.entries(statics).forEach(([funName, fun]) => {
      Object.defineProperty(collection, funName, {
        get: () => fun.bind(collection)
      });
    });
    runPluginHooks('createRxCollection', {
      collection,
      creator: {
        name,
        schema,
        storageInstance,
        instanceCreationOptions,
        migrationStrategies,
        methods,
        attachments,
        options,
        cacheReplacementPolicy,
        localDocuments,
        statics
      }
    });

    /**
     * Migration must run after the hooks so that the
     * dev-mode can check up front if the
     * migration strategies are correctly set.
     */
    if (autoMigrate && collection.schema.version !== 0) {
      await collection.migratePromise();
    }
  } catch (err) {
    /**
     * If the collection creation fails,
     * we yet have to close the storage instances.
     */
    OPEN_COLLECTIONS.delete(collection);
    await storageInstance.close();
    throw err;
  }
  return collection;
}
export function isRxCollection(obj) {
  return obj instanceof RxCollectionBase;
}
//# sourceMappingURL=rx-collection.js.map