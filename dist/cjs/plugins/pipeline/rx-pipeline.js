"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxPipeline = void 0;
exports.addPipeline = addPipeline;
exports.getCheckpointDoc = getCheckpointDoc;
exports.setCheckpointDoc = setCheckpointDoc;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _rxjs = require("rxjs");
var _index = require("../utils/index.js");
var _rxStorageHelper = require("../../rx-storage-helper.js");
var _docCache = require("../../doc-cache.js");
var _rxDatabaseInternalStore = require("../../rx-database-internal-store.js");
var _flaggedFunctions = require("./flagged-functions.js");
var RxPipeline = exports.RxPipeline = /*#__PURE__*/function () {
  function RxPipeline(identifier, source, destination, handler, batchSize = 100) {
    (0, _defineProperty2.default)(this, "processQueue", _index.PROMISE_RESOLVE_VOID);
    (0, _defineProperty2.default)(this, "subs", []);
    (0, _defineProperty2.default)(this, "stopped", false);
    (0, _defineProperty2.default)(this, "toRun", 1);
    (0, _defineProperty2.default)(this, "checkpointId", void 0);
    (0, _defineProperty2.default)(this, "lastSourceDocTime", new _rxjs.BehaviorSubject(-1));
    (0, _defineProperty2.default)(this, "lastProcessedDocTime", new _rxjs.BehaviorSubject(0));
    (0, _defineProperty2.default)(this, "somethingChanged", new _rxjs.Subject());
    (0, _defineProperty2.default)(this, "waitBeforeWriteFn", async () => {
      /**
       * If the pipeline is in an errored state, do not block reads on the
       * destination collection. The error is surfaced via pipeline.awaitIdle()
       * and should not poison unrelated reads on the destination collection.
       */
      if (this.error) {
        return;
      }
      var stack = new Error().stack;
      if (stack && stack.includes(_flaggedFunctions.PIPELINE_FN_PREFIX)) {} else {
        await this.awaitIdle();
      }
    });
    /**
     * The handler of the pipeline must never throw.
     * If it did anyway, the pipeline will be stuck and always
     * throw the previous error on all operations.
     */
    (0, _defineProperty2.default)(this, "error", void 0);
    this.identifier = identifier;
    this.source = source;
    this.destination = destination;
    this.handler = handler;
    this.batchSize = batchSize;
    this.checkpointId = 'rx-pipeline-' + identifier;
    this.source.onClose.push(() => this.close());
    this.destination.awaitBeforeReads.add(this.waitBeforeWriteFn);
    this.subs.push(this.source.eventBulks$.pipe((0, _rxjs.filter)(bulk => !this.stopped && !bulk.isLocal)).subscribe(bulk => {
      this.lastSourceDocTime.next(bulk.events[0].documentData._meta.lwt);
      this.somethingChanged.next({});
    }));
    this.subs.push(this.destination.database.internalStore.changeStream().subscribe(eventBulk => {
      var events = eventBulk.events;
      for (var index = 0; index < events.length; index++) {
        var event = events[index];
        if (event.documentData.context === _rxDatabaseInternalStore.INTERNAL_CONTEXT_PIPELINE_CHECKPOINT && event.documentData.key === this.checkpointId) {
          this.lastProcessedDocTime.next(event.documentData.data.lastDocTime);
          this.somethingChanged.next({});
        }
      }
    }));
  }
  var _proto = RxPipeline.prototype;
  _proto.trigger = function trigger() {
    var _this2 = this;
    /**
     * Do not stack up too many
     * so that fast writes to the source collection
     * do not block anything too long.
     */
    if (this.toRun > 2) {
      return;
    }
    this.toRun = this.toRun + 1;
    this.processQueue = this.processQueue.then(async () => {
      this.toRun = this.toRun - 1;
      var done = false;
      var _loop = async function () {
          var checkpointDoc = await getCheckpointDoc(_this2);
          var checkpoint = checkpointDoc ? checkpointDoc.data.checkpoint : undefined;
          var docsSinceResult = await (0, _rxStorageHelper.getChangedDocumentsSince)(_this2.source.storageInstance, _this2.batchSize, checkpoint);
          var lastTime = checkpointDoc ? checkpointDoc.data.lastDocTime : 0;
          if (docsSinceResult.documents.length > 0) {
            var rxDocuments = (0, _docCache.mapDocumentsDataToCacheDocs)(_this2.source._docCache, docsSinceResult.documents);
            var _this = _this2;

            // const o: any = {};
            // eval(`
            //     async function ${this.secretFunctionName}(docs){ const x = await _this.handler(docs); return x; }
            //     o.${this.secretFunctionName} = ${this.secretFunctionName};
            // `);
            // await o[this.secretFunctionName](rxDocuments);

            var fnKey = (0, _flaggedFunctions.blockFlaggedFunctionKey)();
            try {
              await _flaggedFunctions.FLAGGED_FUNCTIONS[fnKey](() => _this.handler(rxDocuments));
            } catch (err) {
              _this2.error = err;
            } finally {
              (0, _flaggedFunctions.releaseFlaggedFunctionKey)(fnKey);
            }
            if (_this2.error) {
              return {
                v: void 0
              };
            }
            lastTime = (0, _index.ensureNotFalsy)((0, _index.lastOfArray)(docsSinceResult.documents))._meta.lwt;
          }
          if (!_this2.destination.closed) {
            await setCheckpointDoc(_this2, {
              checkpoint: docsSinceResult.checkpoint,
              lastDocTime: lastTime
            }, checkpointDoc);
          }
          if (docsSinceResult.documents.length < _this2.batchSize) {
            done = true;
          }
        },
        _ret;
      while (!done && !this.stopped && !this.destination.closed && !this.source.closed && !this.error) {
        _ret = await _loop();
        if (_ret) return _ret.v;
      }
    }).catch(err => {
      this.error = err;
    });
  };
  _proto.awaitIdle = async function awaitIdle() {
    if (this.error) {
      throw this.error;
    }
    var done = false;
    while (!done) {
      await this.processQueue;
      if (this.error) {
        throw this.error;
      }
      if (this.lastProcessedDocTime.getValue() >= this.lastSourceDocTime.getValue()) {
        done = true;
      } else {
        await (0, _rxjs.firstValueFrom)(this.somethingChanged);
      }
    }
  };
  _proto.close = async function close() {
    await this.processQueue;
    this.stopped = true;
    this.destination.awaitBeforeReads.delete(this.waitBeforeWriteFn);
    this.subs.forEach(s => s.unsubscribe());
    await this.processQueue;
  }

  /**
   * Remove the pipeline and all metadata which it has stored
   */;
  _proto.remove = async function remove() {
    await this.close();
    var insternalStore = this.destination.database.internalStore;
    var checkpointDoc = await getCheckpointDoc(this);
    if (checkpointDoc) {
      var newDoc = (0, _index.clone)(checkpointDoc);
      newDoc._deleted = true;
      newDoc._meta.lwt = (0, _index.now)();
      newDoc._rev = (0, _index.createRevision)(this.destination.database.token, checkpointDoc);
      var writeResult = await insternalStore.bulkWrite([{
        previous: checkpointDoc,
        document: newDoc
      }], 'rx-pipeline');
      if (writeResult.error.length > 0) {
        throw writeResult.error;
      }
    }
  };
  return RxPipeline;
}();
async function getCheckpointDoc(pipeline) {
  var insternalStore = pipeline.destination.database.internalStore;
  var checkpointId = (0, _rxDatabaseInternalStore.getPrimaryKeyOfInternalDocument)(pipeline.checkpointId, _rxDatabaseInternalStore.INTERNAL_CONTEXT_PIPELINE_CHECKPOINT);
  var results = await insternalStore.findDocumentsById([checkpointId], false);
  var result = results[0];
  if (result) {
    return result;
  } else {
    return undefined;
  }
}
async function setCheckpointDoc(pipeline, newCheckpoint, previous) {
  var insternalStore = pipeline.destination.database.internalStore;
  var newDoc = {
    _attachments: {},
    _deleted: false,
    _meta: {
      lwt: (0, _index.now)()
    },
    _rev: (0, _index.createRevision)(pipeline.destination.database.token, previous),
    context: _rxDatabaseInternalStore.INTERNAL_CONTEXT_PIPELINE_CHECKPOINT,
    data: newCheckpoint,
    id: (0, _rxDatabaseInternalStore.getPrimaryKeyOfInternalDocument)(pipeline.checkpointId, _rxDatabaseInternalStore.INTERNAL_CONTEXT_PIPELINE_CHECKPOINT),
    key: pipeline.checkpointId
  };
  var writeResult = await insternalStore.bulkWrite([{
    previous,
    document: newDoc
  }], 'rx-pipeline');
  if (writeResult.error.length > 0) {
    throw writeResult.error;
  }
}
async function addPipeline(options) {
  var pipeline = new RxPipeline(options.identifier, this, options.destination, options.handler, options.batchSize);
  var waitForLeadership = typeof options.waitForLeadership === 'undefined' ? true : options.waitForLeadership;
  var startPromise = waitForLeadership ? this.database.waitForLeadership() : _index.PROMISE_RESOLVE_VOID;
  startPromise.then(() => {
    pipeline.trigger();
    pipeline.subs.push(this.eventBulks$.pipe((0, _rxjs.filter)(bulk => {
      if (pipeline.stopped) {
        return false;
      }
      return !bulk.isLocal;
    })).subscribe(() => pipeline.trigger()));
  });
  return pipeline;
}
//# sourceMappingURL=rx-pipeline.js.map