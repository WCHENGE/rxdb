"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.TRIAL_SQLITE_OPERATION_LIMIT = exports.TRIAL_SQLITE_DOCUMENT_LIMIT = exports.RxStorageInstanceSQLite = void 0;
exports.createSQLiteTrialStorageInstance = createSQLiteTrialStorageInstance;
var _readOnlyError2 = _interopRequireDefault(require("@babel/runtime/helpers/readOnlyError"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _index = require("../../index.js");
var _rxjs = require("rxjs");
var _sqliteHelpers = require("./sqlite-helpers.js");
var _rxQueryHelper = require("../../rx-query-helper.js");
var _rxError = require("../../rx-error.js");
var shownNonPremiumLog = false;
var instanceId = 0;

/**
 * Limits of the free trial SQLite storage.
 * Reaching them throws an error, see the error codes SQL2 and SQL3.
 * Deleted documents are kept as tombstones for the replication but do
 * NOT count towards the document limit.
 * Use the premium SQLite storage to remove these limits:
 * @link https://rxdb.info/premium/
 */
var TRIAL_SQLITE_DOCUMENT_LIMIT = exports.TRIAL_SQLITE_DOCUMENT_LIMIT = 500;
var TRIAL_SQLITE_OPERATION_LIMIT = exports.TRIAL_SQLITE_OPERATION_LIMIT = 500;
/**
 * Ratio of the limits at which a loud warning is logged on each write.
 */
var TRIAL_SQLITE_WARN_RATIO = 0.8;
var RxStorageInstanceSQLite = exports.RxStorageInstanceSQLite = /*#__PURE__*/function () {
  function RxStorageInstanceSQLite(storage, databaseName, collectionName, schema, internals, options, settings, tableName, devMode) {
    (0, _defineProperty2.default)(this, "primaryPath", void 0);
    (0, _defineProperty2.default)(this, "changes$", new _rxjs.Subject());
    (0, _defineProperty2.default)(this, "instanceId", instanceId++);
    (0, _defineProperty2.default)(this, "closed", void 0);
    (0, _defineProperty2.default)(this, "sqliteBasics", void 0);
    (0, _defineProperty2.default)(this, "openWriteCount$", new _rxjs.BehaviorSubject(0));
    (0, _defineProperty2.default)(this, "opCount", 0);
    this.storage = storage;
    this.databaseName = databaseName;
    this.collectionName = collectionName;
    this.schema = schema;
    this.internals = internals;
    this.options = options;
    this.settings = settings;
    this.tableName = tableName;
    this.devMode = devMode;
    this.sqliteBasics = storage.settings.sqliteBasics;
    this.primaryPath = (0, _index.getPrimaryFieldOfPrimaryKey)(this.schema.primaryKey);
  }
  var _proto = RxStorageInstanceSQLite.prototype;
  _proto.run = function run(db, queryWithParams) {
    if (this.devMode) {
      (0, _sqliteHelpers.ensureParamsCountIsCorrect)(queryWithParams);
    }
    return this.sqliteBasics.run(db, queryWithParams);
  };
  _proto.all = function all(db, queryWithParams) {
    if (this.devMode) {
      (0, _sqliteHelpers.ensureParamsCountIsCorrect)(queryWithParams);
    }
    this.opCount = this.opCount + 1;
    if (this.opCount > TRIAL_SQLITE_OPERATION_LIMIT) {
      throw (0, _rxError.newRxError)('SQL3');
    }
    return this.sqliteBasics.all(db, queryWithParams);
  }

  /**
   * @link https://medium.com/@JasonWyatt/squeezing-performance-from-sqlite-insertions-971aff98eef2
   */;
  _proto.bulkWrite = async function bulkWrite(documentWrites, context) {
    this.openWriteCount$.next(this.openWriteCount$.getValue() + 1);
    var database = await this.internals.databasePromise;
    var ret = {
      error: []
    };
    var writePromises = [];
    var categorized = {};
    var isPremium = await (0, _index.hasPremiumFlag)();
    if (!shownNonPremiumLog && !isPremium) {
      console.warn(['-------------- RxDB SQLite Trial Storage ---------------------------------', 'You are using the free *trial* SQLite-based RxStorage implementation: https://rxdb.info/rx-storage-sqlite.html?console=sqlite-trial', 'This storage is intended only for evaluation purposes and comes with strict limitations (no indexes, no attachments, and a ~500-document cap).', 'For production use and optimal performance, we strongly recommend upgrading to the premium SQLite storage.', 'Premium version: https://rxdb.info/premium/?console=sqlite', 'If you already have premium access, you can disable this message by calling setPremiumFlag() from rxdb-premium/plugins/shared.', '----------------------------------------------------------------------------'].join('\n'));
      shownNonPremiumLog = true;
    } else {
      shownNonPremiumLog = true;
    }
    await (0, _sqliteHelpers.sqliteTransaction)(database, this.sqliteBasics, async () => {
      if (this.closed) {
        this.openWriteCount$.next(this.openWriteCount$.getValue() - 1);
        throw new Error('SQLite.bulkWrite(' + context + ') already closed ' + this.tableName + ' context: ' + context);
      }
      var result = await this.all(database, {
        query: "SELECT data FROM \"" + this.tableName + "\"",
        params: [],
        context: {
          method: 'bulkWrite',
          data: documentWrites
        }
      });
      var docsInDb = new Map();
      result.forEach(docSQLResult => {
        var doc = JSON.parse((0, _sqliteHelpers.getDataFromResultRow)(docSQLResult));
        var id = doc[this.primaryPath];
        docsInDb.set(id, doc);
      });
      categorized = (0, _index.categorizeBulkWriteRows)(this, this.primaryPath, docsInDb, documentWrites, context);
      ret.error = categorized.errors;

      /**
       * Deleted documents are kept as tombstones for the replication
       * but must NOT count towards the trial document limit.
       * Therefore we count the resulting amount of non-deleted documents.
       */
      var liveDocCount = 0;
      docsInDb.forEach(doc => {
        if (!doc._deleted) {
          liveDocCount = liveDocCount + 1;
        }
      });
      categorized.bulkInsertDocs.forEach(row => {
        if (!row.document._deleted) {
          liveDocCount = liveDocCount + 1;
        }
      });
      categorized.bulkUpdateDocs.forEach(row => {
        var previous = docsInDb.get(row.document[this.primaryPath]);
        var wasLive = !!previous && !previous._deleted;
        var isLive = !row.document._deleted;
        if (wasLive && !isLive) {
          liveDocCount = liveDocCount - 1;
        } else if (!wasLive && isLive) {
          liveDocCount = liveDocCount + 1;
        }
      });
      if (!isPremium && (liveDocCount >= Math.floor(TRIAL_SQLITE_DOCUMENT_LIMIT * TRIAL_SQLITE_WARN_RATIO) || this.opCount >= Math.floor(TRIAL_SQLITE_OPERATION_LIMIT * TRIAL_SQLITE_WARN_RATIO))) {
        console.warn(['-------------- RxDB SQLite Trial Storage - Limit Almost Reached ----------------', 'You are close to the limits of the free *trial* SQLite RxStorage.', 'Current usage: ' + liveDocCount + ' / ' + TRIAL_SQLITE_DOCUMENT_LIMIT + ' documents and ' + this.opCount + ' / ' + TRIAL_SQLITE_OPERATION_LIMIT + ' operations.', 'When a limit is reached, your read and write operations will start to throw errors.', 'Upgrade to the premium SQLite storage to remove these limits and get full performance:', 'https://rxdb.info/premium/?console=sqlite-trial-limit', '--------------------------------------------------------------------------------'].join('\n'));
      }
      if (liveDocCount > TRIAL_SQLITE_DOCUMENT_LIMIT) {
        throw (0, _rxError.newRxError)('SQL2');
      }
      categorized.bulkInsertDocs.forEach(row => {
        var insertQuery = (0, _sqliteHelpers.getSQLiteInsertSQL)(this.tableName, this.primaryPath, row.document);
        writePromises.push(this.all(database, {
          query: insertQuery.query,
          params: insertQuery.params,
          context: {
            method: 'bulkWrite',
            data: categorized
          }
        }));
      });
      categorized.bulkUpdateDocs.forEach(row => {
        var updateQuery = (0, _sqliteHelpers.getSQLiteUpdateSQL)(this.tableName, this.primaryPath, row);
        writePromises.push(this.run(database, updateQuery));
      });
      await Promise.all(writePromises);

      // close transaction
      if (this.closed) {
        this.openWriteCount$.next(this.openWriteCount$.getValue() - 1);
        return 'ROLLBACK';
      } else {
        this.openWriteCount$.next(this.openWriteCount$.getValue() - 1);
        return 'COMMIT';
      }
    }, {
      databaseName: this.databaseName,
      collectionName: this.collectionName
    });
    if (categorized && categorized.eventBulk.events.length > 0) {
      var lastState = (0, _index.ensureNotFalsy)(categorized.newestRow).document;
      categorized.eventBulk.checkpoint = {
        id: lastState[this.primaryPath],
        lwt: lastState._meta.lwt
      };
      this.changes$.next(categorized.eventBulk);
    }
    return ret;
  };
  _proto.query = async function query(originalPreparedQuery) {
    var database = await this.internals.databasePromise;
    var result = [];
    var query = originalPreparedQuery.query;
    var skip = query.skip ? query.skip : 0;
    /**
     * Use typeof so an explicit `limit: 0` from the mango query is
     * honored. The previous truthy check treated `0` as "no limit"
     * and returned all matching documents.
     */
    var limit = typeof query.limit === 'number' ? query.limit : Infinity;
    var skipPlusLimit = skip + limit;
    var queryMatcher = (0, _index.getQueryMatcher)(this.schema, query);
    var subResult = await this.all(database, {
      query: 'SELECT data FROM "' + this.tableName + '"',
      params: [],
      context: {
        method: 'query',
        data: originalPreparedQuery
      }
    });
    subResult.forEach(row => {
      var docData = JSON.parse((0, _sqliteHelpers.getDataFromResultRow)(row));
      if (queryMatcher(docData)) {
        result.push(docData);
      }
    });
    var sortComparator = (0, _rxQueryHelper.getSortComparator)(this.schema, query);
    result = result.sort(sortComparator);
    result = result.slice(skip, skipPlusLimit);
    return {
      documents: result
    };
  };
  _proto.count = async function count(originalPreparedQuery) {
    var results = await this.query(originalPreparedQuery);
    return {
      count: results.documents.length,
      mode: 'fast'
    };
  };
  _proto.findDocumentsById = async function findDocumentsById(ids, withDeleted) {
    var database = await this.internals.databasePromise;
    if (this.closed) {
      throw new Error('SQLite.findDocumentsById() already closed ' + this.tableName);
    }
    var result = await this.all(database, {
      query: "SELECT data FROM \"" + this.tableName + "\"",
      params: [],
      context: {
        method: 'findDocumentsById',
        data: ids
      }
    });
    var ret = [];
    for (var i = 0; i < result.length; ++i) {
      var resultRow = result[i];
      var doc = JSON.parse((0, _sqliteHelpers.getDataFromResultRow)(resultRow));
      if (ids.includes(doc[this.primaryPath]) && (withDeleted || !doc._deleted)) {
        ret.push(doc);
      }
    }
    return ret;
  };
  _proto.changeStream = function changeStream() {
    return this.changes$.asObservable();
  };
  _proto.cleanup = async function cleanup(minimumDeletedTime) {
    await (0, _index.promiseWait)(0);
    await (0, _index.promiseWait)(0);
    var database = await this.internals.databasePromise;

    /**
     * Purge deleted documents
     */
    var minTimestamp = new Date().getTime() - minimumDeletedTime;
    await this.all(database, {
      query: "\n                    DELETE FROM\n                        \"" + this.tableName + "\"\n                    WHERE\n                        deleted = 1\n                        AND\n                        lastWriteTime < ?\n                ",
      params: [minTimestamp],
      context: {
        method: 'cleanup',
        data: minimumDeletedTime
      }
    });
    return true;
  };
  _proto.getAttachmentData = async function getAttachmentData(_documentId, _attachmentId) {
    throw (0, _rxError.newRxError)('SQL1');
  };
  _proto.remove = async function remove() {
    if (this.closed) {
      throw new Error('closed already');
    }
    var database = await this.internals.databasePromise;
    /**
     * The DROP TABLE must run inside of a transaction so that it
     * goes through TX_QUEUE_BY_DATABASE. All storage instances of one
     * database share a single connection and adapters like expo-sqlite
     * throw SQLITE_LOCKED when two statements run on it at the same time.
     */
    await (0, _sqliteHelpers.sqliteTransaction)(database, this.sqliteBasics, async () => {
      await this.run(database, {
        query: "DROP TABLE IF EXISTS \"" + this.tableName + "\"",
        params: [],
        context: {
          method: 'remove',
          data: this.tableName
        }
      });
      return 'COMMIT';
    }, {
      databaseName: this.databaseName,
      collectionName: this.collectionName
    });
    return this.close();
  };
  _proto.close = async function close() {
    var queue = _sqliteHelpers.TX_QUEUE_BY_DATABASE.get(await this.internals.databasePromise);
    if (queue) {
      await queue;
    }
    if (this.closed) {
      return this.closed;
    }
    this.closed = (async () => {
      await (0, _rxjs.firstValueFrom)(this.openWriteCount$.pipe((0, _rxjs.filter)(v => v === 0)));
      var database = await this.internals.databasePromise;

      /**
       * First get a transaction
       * to ensure currently running operations
       * are finished
       */
      await (0, _sqliteHelpers.sqliteTransaction)(database, this.sqliteBasics, () => {
        return Promise.resolve('COMMIT');
      }).catch(() => {});
      this.changes$.complete();
      await (0, _sqliteHelpers.closeDatabaseConnection)(this.databaseName, this.storage.settings.sqliteBasics);
    })();
    return this.closed;
  };
  return RxStorageInstanceSQLite;
}();
async function createSQLiteTrialStorageInstance(storage, params, settings) {
  var sqliteBasics = settings.sqliteBasics;
  var tableName = params.collectionName + '-' + params.schema.version;
  if (params.schema.attachments) {
    throw (0, _rxError.newRxError)('SQL1');
  }
  var internals = {};
  var useDatabaseName = (settings.databaseNamePrefix ? settings.databaseNamePrefix : '') + '_trial_' + params.databaseName;
  internals.databasePromise = (0, _sqliteHelpers.getDatabaseConnection)(storage.settings.sqliteBasics, useDatabaseName).then(async database => {
    await (0, _sqliteHelpers.sqliteTransaction)(database, sqliteBasics, async () => {
      var tableQuery = "\n                CREATE TABLE IF NOT EXISTS \"" + tableName + "\"(\n                    id TEXT NOT NULL PRIMARY KEY UNIQUE,\n                    revision TEXT,\n                    deleted BOOLEAN NOT NULL CHECK (deleted IN (0, 1)),\n                    lastWriteTime INTEGER NOT NULL,\n                    data json\n                );\n                ";
      await sqliteBasics.run(database, {
        query: tableQuery,
        params: [],
        context: {
          method: 'createSQLiteStorageInstance create tables',
          data: params.databaseName
        }
      });
      return 'COMMIT';
    }, {
      indexCreation: false,
      databaseName: params.databaseName,
      collectionName: params.collectionName
    });
    return database;
  });
  var instance = new RxStorageInstanceSQLite(storage, params.databaseName, params.collectionName, params.schema, internals, params.options, settings, tableName, params.devMode);
  await (0, _index.addRxStorageMultiInstanceSupport)(_sqliteHelpers.RX_STORAGE_NAME_SQLITE, params, instance);
  return instance;
}
//# sourceMappingURL=sqlite-storage-instance.js.map