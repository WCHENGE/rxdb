"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxMigrationState = void 0;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _rxjs = require("rxjs");
var _rxError = require("../../rx-error.js");
var _migrationHelpers = require("./migration-helpers.js");
var _index = require("../utils/index.js");
var _rxStorageHelper = require("../../rx-storage-helper.js");
var _broadcastChannel = require("broadcast-channel");
var _index2 = require("../../replication-protocol/index.js");
var _overwritable = require("../../overwritable.js");
var _rxDatabaseInternalStore = require("../../rx-database-internal-store.js");
var _rxQueryHelper = require("../../rx-query-helper.js");
var RxMigrationState = exports.RxMigrationState = /*#__PURE__*/function () {
  function RxMigrationState(collection, migrationStrategies, statusDocKey = [collection.name, 'v', collection.schema.version].join('-')) {
    (0, _defineProperty2.default)(this, "database", void 0);
    (0, _defineProperty2.default)(this, "started", false);
    (0, _defineProperty2.default)(this, "oldCollectionMeta", void 0);
    (0, _defineProperty2.default)(this, "mustMigrate", void 0);
    (0, _defineProperty2.default)(this, "statusDocId", void 0);
    (0, _defineProperty2.default)(this, "$", void 0);
    /**
     * Contains ALL replication states
     * that are ever used in this migration state.
     */
    (0, _defineProperty2.default)(this, "replicationStates", new Set());
    /**
     * All storage instances that are opened by the migration itself.
     * They have to be closed when the migration is canceled,
     * otherwise they stay open forever when the migration is interrupted,
     * for example when the database is closed while the migration is running.
     */
    (0, _defineProperty2.default)(this, "openStorageInstances", new Set());
    (0, _defineProperty2.default)(this, "canceled", false);
    (0, _defineProperty2.default)(this, "broadcastChannel", void 0);
    (0, _defineProperty2.default)(this, "heartbeatInterval", void 0);
    (0, _defineProperty2.default)(this, "updateStatusHandlers", []);
    (0, _defineProperty2.default)(this, "updateStatusQueue", _index.PROMISE_RESOLVE_TRUE);
    this.collection = collection;
    this.migrationStrategies = migrationStrategies;
    this.statusDocKey = statusDocKey;
    this.database = collection.database;
    this.oldCollectionMeta = (0, _migrationHelpers.getOldCollectionMeta)(this);
    this.mustMigrate = (0, _migrationHelpers.mustMigrate)(this);
    this.statusDocId = (0, _rxDatabaseInternalStore.getPrimaryKeyOfInternalDocument)(this.statusDocKey, _rxDatabaseInternalStore.INTERNAL_CONTEXT_MIGRATION_STATUS);
    (0, _migrationHelpers.addMigrationStateToDatabase)(this);
    this.$ = (0, _rxStorageHelper.observeSingle)(this.database.internalStore, this.statusDocId).pipe((0, _rxjs.filter)(d => !!d), (0, _rxjs.map)(d => (0, _index.ensureNotFalsy)(d).data),
    /**
     * The heartbeat of the leader rewrites the status document
     * without changing its data, which must not emit here.
     */
    (0, _rxjs.distinctUntilChanged)((a, b) => (0, _index.deepEqual)(a, b)), (0, _rxjs.shareReplay)(_index.RXJS_SHARE_REPLAY_DEFAULTS));
  }
  var _proto = RxMigrationState.prototype;
  _proto.getStatus = function getStatus() {
    return (0, _rxjs.firstValueFrom)(this.$);
  }

  /**
   * Starts the migration.
   * Returns void so that people to not get the idea to await
   * this function.
   * Instead use migratePromise() if you want to await
   * the migration. This ensures it works even if the migration
   * is run on a different browser tab.
   */;
  _proto.startMigration = async function startMigration(batchSize = _migrationHelpers.MIGRATION_DEFAULT_BATCH_SIZE) {
    if (this.started) {
      throw (0, _rxError.newRxError)('DM1');
    }
    /**
     * Block outside writes to the collection while the migration is running.
     * The migration replication fills the new storage and concurrent writes
     * could conflict with that process.
     * We set the flag synchronously (before the `mustMigrate` await) so that
     * any code calling `migratePromise()` and then immediately performing a
     * write will reliably observe the block.
     * If no migration is actually needed, the flag is cleared again below.
     */
    this.collection.migrationInProgress = true;
    var must = await this.mustMigrate;
    if (!must) {
      this.collection.migrationInProgress = false;
      return;
    }
    this.started = true;

    /**
     * Ensure the migration is cleaned up when the collection or the
     * database is closed. Registering this here (instead of inside
     * migrateStorage()) guarantees the broadcastChannel / leader
     * election is released even if the migration throws before the
     * replication is set up.
     */
    this.collection.onClose.push(() => this.cancel());
    this.database.onClose.push(() => this.cancel());
    try {
      await this.runMigration(batchSize);
    } finally {
      this.stopHeartbeat();
      /**
       * Always close the broadcastChannel so that the tab does not
       * stay leader forever if the migration throws on any code path.
       * @link https://github.com/pubkey/rxdb/pull/7827
       */
      if (this.broadcastChannel) {
        await this.broadcastChannel.close();
        this.broadcastChannel = undefined;
      }
    }
  };
  _proto.runMigration = async function runMigration(batchSize) {
    /**
     * To ensure that multiple tabs do not migrate the same collection,
     * we use a new broadcastChannel/leaderElector for each collection.
     * This is required because collections can be added dynamically and
     * not all tabs might know about this collection.
     */
    if (this.database.multiInstance) {
      this.broadcastChannel = new _broadcastChannel.BroadcastChannel(['rx-migration-state', this.database.name, this.collection.name, this.collection.schema.version].join('|'));
      var leaderElector = (0, _broadcastChannel.createLeaderElection)(this.broadcastChannel);
      var waitResult = await this.awaitLeadershipOrStalledLeader(() => leaderElector.awaitLeadership());
      if (waitResult === 'DONE' || this.canceled) {
        this.collection.migrationInProgress = false;
        return;
      }
      this.startHeartbeat();
    }

    /**
     * Instead of writing a custom migration protocol,
     * we do a push-only replication from the old collection data to the new one.
     * This also ensure that restarting the replication works without problems.
     */
    var oldCollectionMeta = await this.oldCollectionMeta;
    var oldStorageInstance = await this.database.storage.createStorageInstance({
      databaseName: this.database.name,
      collectionName: this.collection.name,
      databaseInstanceToken: this.database.token,
      multiInstance: this.database.multiInstance,
      options: {},
      schema: (0, _index.ensureNotFalsy)(oldCollectionMeta).data.schema,
      password: this.database.password,
      devMode: _overwritable.overwritable.isDevMode()
    });
    this.openStorageInstances.add(oldStorageInstance);
    var connectedInstances = await this.getConnectedStorageInstances();

    /**
     * Initially write the migration status into a meta document.
     */
    var totalCount = await this.countAllDocuments([oldStorageInstance].concat(connectedInstances.map(r => r.oldStorage)));
    await this.updateStatus(s => {
      s.count.total = totalCount;
      return s;
    });
    try {
      /**
       * First migrate the connected storages,
       * afterwards migrate the normal collection.
      */
      await Promise.all(connectedInstances.map(async connectedInstance => {
        await (0, _rxDatabaseInternalStore.addConnectedStorageToCollection)(this.collection, connectedInstance.newStorage.collectionName, connectedInstance.newStorage.schema);
        await this.migrateStorage(connectedInstance.oldStorage, connectedInstance.newStorage, batchSize);
        this.openStorageInstances.delete(connectedInstance.newStorage);
        await connectedInstance.newStorage.close();
      }));
      await this.migrateStorage(oldStorageInstance,
      /**
       * Use the originalStorageInstance here
       * so that the _meta.lwt time keeps the same
       * and our replication checkpoints still point to the
       * correct checkpoint.
      */
      this.collection.storageInstance.originalStorageInstance, batchSize);
    } catch (err) {
      this.openStorageInstances.delete(oldStorageInstance);
      await oldStorageInstance.close();
      this.collection.migrationInProgress = false;
      await this.updateStatus(s => {
        /**
         * When a frozen leader was taken over by another tab
         * and continues later, it can fail because the other tab
         * has already finished the migration and removed the old storages.
         * A finished migration must not be marked as failed.
         */
        if (s.status === 'DONE') {
          return s;
        }
        s.status = 'ERROR';
        s.error = (0, _index.errorToPlainJson)(err);
        return s;
      });
      return;
    }

    /**
     * Remove old collection meta doc with retry on conflict.
     * The _rev of the meta doc may have changed since we fetched it
     * at the start of migration (due to updateStatus() calls),
     * so we re-fetch before each deletion attempt.
     * @link https://github.com/pubkey/rxdb/issues/7791
     */
    while (true) {
      var currentMeta = await (0, _migrationHelpers.getOldCollectionMeta)(this);
      if (!currentMeta) {
        break;
      }
      try {
        await (0, _rxStorageHelper.writeSingle)(this.database.internalStore, {
          previous: currentMeta,
          document: Object.assign({}, currentMeta, {
            _deleted: true
          })
        }, 'rx-migration-remove-collection-meta');
        break;
      } catch (error) {
        var isConflict = (0, _rxError.isBulkWriteConflictError)(error);
        if (isConflict && !!isConflict.documentInDb._deleted) {
          break;
        } else if (isConflict) {
          continue;
        } else {
          throw error;
        }
      }
    }
    this.collection.migrationInProgress = false;
    await this.updateStatus(s => {
      s.status = 'DONE';
      return s;
    });
  }

  /**
   * Waits until this instance is the leader of the migration.
   * Returns 'STALLED' when the current leader did not write
   * the status document for longer than MIGRATION_LEADER_TIMING.timeout,
   * because then the leader is likely frozen, throttled or discarded
   * by the browser and this instance takes over the migration.
   * Returns 'DONE' when another instance has finished the migration.
   * @link https://github.com/pubkey/rxdb/issues/9191
   */;
  _proto.awaitLeadershipOrStalledLeader = async function awaitLeadershipOrStalledLeader(awaitLeadership) {
    var waiting = true;
    var startTime = (0, _index.now)();
    var leadershipPromise = awaitLeadership().then(() => 'LEADER');
    var stalledPromise = (async () => {
      while (waiting && !this.canceled) {
        await (0, _index.promiseWait)(_migrationHelpers.MIGRATION_LEADER_TIMING.heartbeatInterval);
        if (!waiting || this.canceled) {
          break;
        }
        var statusDoc = await (0, _rxStorageHelper.getSingleDocument)(this.database.internalStore, this.statusDocId);
        if (statusDoc && statusDoc.data.status === 'DONE') {
          return 'DONE';
        }
        var lastActivity = Math.max(startTime, statusDoc ? statusDoc._meta.lwt : 0);
        if ((0, _index.now)() - lastActivity > _migrationHelpers.MIGRATION_LEADER_TIMING.timeout) {
          return 'STALLED';
        }
      }
      /**
       * Waiting has ended because of leadership or cancellation,
       * the other promise of the race decides the result.
       */
      return new Promise(() => {});
    })();
    try {
      return await Promise.race([leadershipPromise, stalledPromise]);
    } finally {
      waiting = false;
    }
  }

  /**
   * Regularly rewrites the status document while this instance
   * runs the migration, so that other instances can see that
   * the leader is still alive.
   * Frozen or discarded browser tabs do not run timers,
   * so the heartbeat stops when the leader stops working.
   */;
  _proto.startHeartbeat = function startHeartbeat() {
    this.stopHeartbeat();
    this.heartbeatInterval = setInterval(() => {
      this.updateStatusQueue = this.updateStatusQueue.then(async () => {
        if (this.canceled) {
          return;
        }
        try {
          var previous = await (0, _rxStorageHelper.getSingleDocument)(this.database.internalStore, this.statusDocId);
          if (!previous || previous.data.status !== 'RUNNING') {
            return;
          }
          var newDoc = (0, _index.clone)(previous);
          newDoc._meta.lwt = (0, _index.now)();
          await (0, _rxStorageHelper.writeSingle)(this.database.internalStore, {
            previous,
            document: newDoc
          }, _rxDatabaseInternalStore.INTERNAL_CONTEXT_MIGRATION_STATUS);
        } catch (_err) {
          /**
           * A failed heartbeat is not a problem,
           * conflicts mean another write has refreshed the document anyway.
           */
        }
      });
    }, _migrationHelpers.MIGRATION_LEADER_TIMING.heartbeatInterval);
  };
  _proto.stopHeartbeat = function stopHeartbeat() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = undefined;
    }
  };
  _proto.updateStatus = function updateStatus(handler) {
    this.updateStatusHandlers.push(handler);
    this.updateStatusQueue = this.updateStatusQueue.then(async () => {
      if (this.updateStatusHandlers.length === 0) {
        return;
      }
      // re-run until no conflict
      var useHandlers = this.updateStatusHandlers;
      this.updateStatusHandlers = [];
      while (true) {
        var previous = await (0, _rxStorageHelper.getSingleDocument)(this.database.internalStore, this.statusDocId);
        var newDoc = (0, _index.clone)(previous);
        if (!previous) {
          newDoc = {
            id: this.statusDocId,
            key: this.statusDocKey,
            context: _rxDatabaseInternalStore.INTERNAL_CONTEXT_MIGRATION_STATUS,
            data: {
              collectionName: this.collection.name,
              status: 'RUNNING',
              count: {
                total: 0,
                handled: 0,
                percent: 0
              }
            },
            _deleted: false,
            _meta: (0, _index.getDefaultRxDocumentMeta)(),
            _rev: (0, _index.getDefaultRevision)(),
            _attachments: {}
          };
        }
        var status = (0, _index.ensureNotFalsy)(newDoc).data;
        for (var oneHandler of useHandlers) {
          status = oneHandler(status);
        }
        status.count.percent = status.count.total === 0 ? 100 : Math.round(status.count.handled / status.count.total * 100);
        if (newDoc && previous && (0, _index.deepEqual)(newDoc.data, previous.data)) {
          break;
        }
        try {
          await (0, _rxStorageHelper.writeSingle)(this.database.internalStore, {
            previous,
            document: (0, _index.ensureNotFalsy)(newDoc)
          }, _rxDatabaseInternalStore.INTERNAL_CONTEXT_MIGRATION_STATUS);

          // write successful
          break;
        } catch (err) {
          // ignore conflicts
          if (!(0, _rxError.isBulkWriteConflictError)(err)) {
            throw err;
          }
        }
      }
    });
    return this.updateStatusQueue;
  };
  _proto.migrateStorage = async function migrateStorage(oldStorage, newStorage, batchSize) {
    var replicationMetaStorageInstance = await this.database.storage.createStorageInstance({
      databaseName: this.database.name,
      collectionName: 'rx-migration-state-meta-' + oldStorage.collectionName + '-' + oldStorage.schema.version,
      databaseInstanceToken: this.database.token,
      multiInstance: this.database.multiInstance,
      options: {},
      schema: (0, _index2.getRxReplicationMetaInstanceSchema)(oldStorage.schema, (0, _rxStorageHelper.hasEncryption)(oldStorage.schema)),
      password: this.database.password,
      devMode: _overwritable.overwritable.isDevMode()
    });
    this.openStorageInstances.add(replicationMetaStorageInstance);
    var replicationHandlerBase = (0, _index2.rxStorageInstanceToReplicationHandler)(newStorage,
    /**
     * Ignore push-conflicts.
     * If this happens we drop the 'old' document state.
     */
    _index2.defaultConflictHandler, this.database.token, true);
    var replicationState = (0, _index2.replicateRxStorageInstance)({
      keepMeta: true,
      skipStoringPullMeta: false,
      identifier: ['rx-migration-state', oldStorage.collectionName, oldStorage.schema.version, this.collection.schema.version].join('-'),
      replicationHandler: {
        masterChangesSince() {
          return Promise.resolve({
            checkpoint: null,
            documents: []
          });
        },
        masterWrite: async rows => {
          var migratedRows = await Promise.all(rows.map(async row => {
            var newDocData = row.newDocumentState;
            if (newStorage.schema.title === _index2.META_INSTANCE_SCHEMA_TITLE) {
              newDocData = row.newDocumentState.docData;
              if (row.newDocumentState.isCheckpoint === '1') {
                return {
                  assumedMasterState: undefined,
                  newDocumentState: row.newDocumentState
                };
              }
            }
            var migratedDocData = await (0, _migrationHelpers.migrateDocumentData)(this.collection, oldStorage.schema.version, newDocData);

            /**
             * The migration strategy can return null
             * which means the document must be deleted during migration.
             */
            if (migratedDocData === null) {
              return null;
            }
            var newRow = {
              // drop the assumed master state, we do not have to care about conflicts here.
              assumedMasterState: undefined,
              newDocumentState: newStorage.schema.title === _index2.META_INSTANCE_SCHEMA_TITLE ? Object.assign({}, row.newDocumentState, {
                docData: migratedDocData
              }) : migratedDocData
            };
            return newRow;
          }));

          // filter out the documents where the migration strategy returned null
          migratedRows = migratedRows.filter(row => !!row && !!row.newDocumentState);
          await replicationHandlerBase.masterWrite(migratedRows);

          /**
           * Push-conflicts are ignored on purpose,
           * we keep what is already stored in the new storage
           * and drop the 'old' document state.
           *
           * Reporting the conflicts to the upstream would make the migration
           * run forever: The rows are pushed with `assumedMasterState: undefined`
           * which the replication protocol handles as an insert, so every document
           * that already exists in the new storage is a conflict. The upstream then
           * resolves these conflicts by writing the master state back into the
           * old storage, that write emits on the fork change stream, the same
           * documents are read again and the cycle starts over.
           * This happens whenever documents are already in the new storage before
           * the migration runs, for example when a previous migration was
           * interrupted after it has written some documents.
           */
          return [];
        },
        masterChangeStream$: new _rxjs.Subject().asObservable()
      },
      forkInstance: oldStorage,
      metaInstance: replicationMetaStorageInstance,
      pushBatchSize: batchSize,
      pullBatchSize: 0,
      conflictHandler: _index2.defaultConflictHandler,
      hashFunction: this.database.hashFunction
    });
    this.replicationStates.add(replicationState);
    var hasError = false;
    replicationState.events.error.subscribe(err => hasError = err);

    // update replication status on each change
    replicationState.events.processed.up.subscribe(() => {
      this.updateStatus(status => {
        status.count.handled = status.count.handled + 1;
        return status;
      });
    });
    await (0, _index2.awaitRxStorageReplicationFirstInSync)(replicationState);
    await (0, _index2.awaitRxStorageReplicationInSync)(replicationState);
    await this.updateStatusQueue;
    if (hasError) {
      await (0, _index2.cancelRxStorageReplication)(replicationState);
      this.openStorageInstances.delete(replicationMetaStorageInstance);
      await replicationMetaStorageInstance.close();
      throw hasError;
    }

    // cleanup old storages
    this.openStorageInstances.delete(oldStorage);
    this.openStorageInstances.delete(replicationMetaStorageInstance);
    /**
     * Remove the storages one after another, not in parallel.
     * Storages like SQLite share a single connection between all
     * storage instances of one database and can only run
     * one operation on it at the same time.
     */
    await oldStorage.remove();
    await replicationMetaStorageInstance.remove();
    await (0, _index2.cancelRxStorageReplication)(replicationState);
  }

  /**
   * Stops the migration.
   * Mostly used in tests to simulate what happens
   * when the user reloads the page during a migration.
   */;
  _proto.cancel = async function cancel() {
    this.canceled = true;
    this.stopHeartbeat();
    this.collection.migrationInProgress = false;
    await Promise.all(Array.from(this.replicationStates.values()).map(state => (0, _index2.cancelRxStorageReplication)(state)));

    /**
     * Close the storage instances that were opened by the migration.
     * After the replications are canceled, migrateStorage() does not
     * continue and therefore never closes them on its own.
     */
    var openInstances = Array.from(this.openStorageInstances);
    this.openStorageInstances.clear();
    await Promise.all(openInstances.map(instance => instance.close().catch(() => {})));
    if (this.broadcastChannel) {
      await this.broadcastChannel.close();
    }
  };
  _proto.countAllDocuments = async function countAllDocuments(storageInstances) {
    var ret = 0;
    await Promise.all(storageInstances.map(async instance => {
      var preparedQuery = (0, _rxQueryHelper.prepareQuery)(instance.schema, (0, _rxQueryHelper.normalizeMangoQuery)(instance.schema, {
        selector: {}
      }));
      var countResult = await instance.count(preparedQuery);
      ret += countResult.count;
    }));
    return ret;
  };
  _proto.getConnectedStorageInstances = async function getConnectedStorageInstances() {
    var oldCollectionMeta = (0, _index.ensureNotFalsy)(await this.oldCollectionMeta);
    var ret = [];
    await Promise.all(await Promise.all(oldCollectionMeta.data.connectedStorages.map(async connectedStorage => {
      // atm we can only migrate replication states.
      if (connectedStorage.schema.title !== _index2.META_INSTANCE_SCHEMA_TITLE) {
        throw new Error('unknown migration handling for schema');
      }
      var newSchema = (0, _index2.getRxReplicationMetaInstanceSchema)((0, _index.clone)(this.collection.schema.jsonSchema), (0, _rxStorageHelper.hasEncryption)(connectedStorage.schema));
      newSchema.version = this.collection.schema.version;
      var [oldStorage, newStorage] = await Promise.all([this.database.storage.createStorageInstance({
        databaseInstanceToken: this.database.token,
        databaseName: this.database.name,
        devMode: _overwritable.overwritable.isDevMode(),
        multiInstance: this.database.multiInstance,
        options: {},
        schema: connectedStorage.schema,
        password: this.database.password,
        collectionName: connectedStorage.collectionName
      }), this.database.storage.createStorageInstance({
        databaseInstanceToken: this.database.token,
        databaseName: this.database.name,
        devMode: _overwritable.overwritable.isDevMode(),
        multiInstance: this.database.multiInstance,
        options: {},
        schema: newSchema,
        password: this.database.password,
        collectionName: connectedStorage.collectionName
      })]);
      this.openStorageInstances.add(oldStorage);
      this.openStorageInstances.add(newStorage);
      ret.push({
        oldStorage,
        newStorage
      });
    })));
    return ret;
  };
  _proto.migratePromise = async function migratePromise(batchSize) {
    this.startMigration(batchSize).catch(() => {});
    var must = await this.mustMigrate;
    if (!must) {
      return {
        status: 'DONE',
        collectionName: this.collection.name,
        count: {
          handled: 0,
          percent: 100,
          total: 0
        }
      };
    }
    var result = await Promise.race([(0, _rxjs.firstValueFrom)(this.$.pipe((0, _rxjs.filter)(d => d.status === 'DONE'))), (0, _rxjs.firstValueFrom)(this.$.pipe((0, _rxjs.filter)(d => d.status === 'ERROR')))]);
    if (result.status === 'ERROR') {
      throw (0, _rxError.newRxError)('DM4', {
        collection: this.collection.name,
        error: result.error
      });
    } else {
      return result;
    }
  };
  return RxMigrationState;
}();
//# sourceMappingURL=rx-migration-state.js.map