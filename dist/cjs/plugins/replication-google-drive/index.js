"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
var _exportNames = {
  DEFAULT_TRANSACTION_TIMEOUT: true,
  RxGoogleDriveReplicationState: true,
  replicateGoogleDrive: true
};
exports.RxGoogleDriveReplicationState = exports.DEFAULT_TRANSACTION_TIMEOUT = void 0;
exports.replicateGoogleDrive = replicateGoogleDrive;
var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _index = require("../leader-election/index.js");
var _index2 = require("../replication/index.js");
var _index3 = require("../../index.js");
var _rxjs = require("rxjs");
var _init = require("./init.js");
Object.keys(_init).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _init[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _init[key];
    }
  });
});
var _upstream = require("./upstream.js");
Object.keys(_upstream).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _upstream[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _upstream[key];
    }
  });
});
var _downstream = require("./downstream.js");
Object.keys(_downstream).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _downstream[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _downstream[key];
    }
  });
});
var _transaction = require("./transaction.js");
Object.keys(_transaction).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _transaction[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _transaction[key];
    }
  });
});
var _connectionHandlerSimplePeer = require("../replication-webrtc/connection-handler-simple-peer.js");
var _signaling = require("./signaling.js");
Object.keys(_signaling).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _signaling[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _signaling[key];
    }
  });
});
var _documentHandling = require("./document-handling.js");
Object.keys(_documentHandling).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _documentHandling[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _documentHandling[key];
    }
  });
});
var _googleDriveTypes = require("./google-drive-types.js");
Object.keys(_googleDriveTypes).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _googleDriveTypes[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _googleDriveTypes[key];
    }
  });
});
var _googleDriveHelper = require("./google-drive-helper.js");
Object.keys(_googleDriveHelper).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _googleDriveHelper[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _googleDriveHelper[key];
    }
  });
});
var _multipart = require("./multipart.js");
Object.keys(_multipart).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _multipart[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _multipart[key];
    }
  });
});
var DEFAULT_TRANSACTION_TIMEOUT = exports.DEFAULT_TRANSACTION_TIMEOUT = 60 * 1000;
var RxGoogleDriveReplicationState = exports.RxGoogleDriveReplicationState = /*#__PURE__*/function (_RxReplicationState) {
  function RxGoogleDriveReplicationState(googleDrive, driveStructure, replicationIdentifierHash, collection, pull, push, signalingOptions, live = true, retryTime = 1000 * 5, autoStart = true) {
    var _this;
    _this = _RxReplicationState.call(this, replicationIdentifierHash, collection, '_deleted', pull, push, live, retryTime, autoStart) || this;
    /**
     * Only exists on live replication
     */
    (0, _defineProperty2.default)(_this, "signalingState", void 0);
    _this.googleDrive = googleDrive;
    _this.driveStructure = driveStructure;
    _this.replicationIdentifierHash = replicationIdentifierHash;
    _this.collection = collection;
    _this.pull = pull;
    _this.push = push;
    _this.signalingOptions = signalingOptions;
    _this.live = live;
    _this.retryTime = retryTime;
    _this.autoStart = autoStart;
    return _this;
  }

  /**
   * Notify other peers that something
   * has or might have changed so that
   * they can pull from their checkpoints.
   */
  (0, _inheritsLoose2.default)(RxGoogleDriveReplicationState, _RxReplicationState);
  var _proto = RxGoogleDriveReplicationState.prototype;
  _proto.notifyPeers = async function notifyPeers() {
    if (this.signalingState) {
      await this.signalingState.pingPeers('RESYNC');
    }
  };
  return RxGoogleDriveReplicationState;
}(_index2.RxReplicationState);
async function replicateGoogleDrive(options) {
  var collection = options.collection;
  (0, _index3.addRxPlugin)(_index.RxDBLeaderElectionPlugin);
  var googleDriveOptionsWithDefaults = Object.assign({
    apiEndpoint: 'https://www.googleapis.com',
    transactionTimeout: DEFAULT_TRANSACTION_TIMEOUT,
    space: 'drive',
    folderPath: ''
  }, options.googleDrive);
  if (typeof googleDriveOptionsWithDefaults.folderPath !== 'string') {
    googleDriveOptionsWithDefaults.folderPath = '';
  }
  var driveStructure = await (0, _init.initDriveStructure)(googleDriveOptionsWithDefaults);

  /**
   * When true, attachment binary data is serialised as base64 inside the
   * document JSON file stored on Google Drive.  Defaults to true only when
   * the collection schema has `attachments: {}` defined.  Can be disabled
   * explicitly by passing `attachments: false` in the options.
   */
  var replicateAttachments = options.attachments !== false && !!collection.schema.jsonSchema.attachments;
  var replicationState;
  var pullStream$ = new _rxjs.Subject();
  var replicationPrimitivesPull;
  options.live = typeof options.live === 'undefined' ? true : options.live;
  options.waitForLeadership = typeof options.waitForLeadership === 'undefined' ? true : options.waitForLeadership;
  if (options.pull) {
    replicationPrimitivesPull = {
      async handler(lastPulledCheckpoint, batchSize) {
        return (0, _transaction.runInTransaction)(googleDriveOptionsWithDefaults, driveStructure, collection.schema.primaryPath, async () => {
          var changes = await (0, _downstream.fetchChanges)(googleDriveOptionsWithDefaults, driveStructure, lastPulledCheckpoint, batchSize);
          /**
           * Convert base64 attachment data that was stored in the
           * Google Drive JSON file back to Blobs so that the
           * downstream replication protocol can write them to the
           * fork storage instance correctly.
           */
          if (replicateAttachments) {
            await Promise.all(changes.documents.map(doc => (0, _documentHandling.deserializeDocAttachments)(doc)));
          }
          return changes;
        });
      },
      batchSize: options.pull.batchSize,
      modifier: options.pull.modifier,
      stream$: pullStream$.asObservable(),
      initialCheckpoint: options.pull.initialCheckpoint
    };
  }
  var replicationPrimitivesPush;
  if (options.push) {
    replicationPrimitivesPush = {
      async handler(rows) {
        /**
         * Convert Blob attachment data to base64 strings before the
         * rows are written to the WAL file or stored as document JSON
         * in Google Drive.  We create new row objects so the originals
         * (which the replication protocol still uses for meta updates)
         * are not mutated.
         * When attachments are disabled we still run the conversion so
         * that Blob values are stripped rather than serialised as `{}`
         * by JSON.stringify.
         */
        var rowsForDrive = await Promise.all(rows.map(async row => {
          var newState = await (0, _documentHandling.serializeDocAttachments)(row.newDocumentState, replicateAttachments);
          return {
            ...row,
            newDocumentState: newState
          };
        }));
        return (0, _transaction.runInTransaction)(googleDriveOptionsWithDefaults, driveStructure, collection.schema.primaryPath, async () => {
          var conflicts = await (0, _upstream.handleUpstreamBatch)(googleDriveOptionsWithDefaults, driveStructure, options.collection.schema.primaryPath, rowsForDrive);
          /**
           * When attachment replication is enabled, deserialise the
           * base64 attachment data that Google Drive stored in the
           * `_attachments_data` field back into Blobs on each conflict
           * document before returning them to the replication protocol.
           *
           * The protocol passes these as `realMasterState` to
           * `resolveConflictError`, which (with the fix in
           * `conflicts.ts`) then writes the resolved state — including
           * the correct attachment Blob — to the fork instance.
           * Without this deserialisation the fork would receive stubs
           * with no binary data and the attachment would be lost.
           */
          if (replicateAttachments && conflicts.length > 0) {
            await Promise.all(conflicts.map(c => (0, _documentHandling.deserializeDocAttachments)(c)));
          }
          return conflicts;
        }, () => replicationState.notifyPeers().catch(() => {}));
      },
      batchSize: options.push.batchSize,
      modifier: options.push.modifier
    };
  }
  replicationState = new RxGoogleDriveReplicationState(googleDriveOptionsWithDefaults, driveStructure, options.replicationIdentifier, collection, replicationPrimitivesPull, replicationPrimitivesPush, options.signalingOptions, options.live, options.retryTime, options.autoStart);

  /**
   * Google drive has no websocket or server-send-events
   * to observe file changes. Therefore we use WebRTC to
   * connect clients which then can ping each other on changes.
   * Instead of a signaling server, we use the google-drive itself
   * to exchange signaling data.
   */
  if (options.live && options.pull) {
    (0, _connectionHandlerSimplePeer.ensureProcessNextTickIsSet)();
    var startBefore = replicationState.start.bind(replicationState);
    var cancelBefore = replicationState.cancel.bind(replicationState);
    replicationState.start = () => {
      replicationState.signalingState = new _signaling.SignalingState(replicationState.googleDrive, replicationState.driveStructure, options.signalingOptions ? options.signalingOptions : {});
      var sub = replicationState.signalingState.resync$.subscribe(() => {
        replicationState.reSync();
      });
      replicationState.cancel = () => {
        sub.unsubscribe();
        replicationState.signalingState?.close();
        return cancelBefore();
      };
      return startBefore();
    };
  }
  (0, _index2.startReplicationOnLeaderShip)(options.waitForLeadership, replicationState);
  return replicationState;
}
//# sourceMappingURL=index.js.map