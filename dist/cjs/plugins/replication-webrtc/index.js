"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
var _exportNames = {
  replicateWebRTC: true,
  RxWebRTCReplicationPool: true
};
exports.RxWebRTCReplicationPool = void 0;
exports.replicateWebRTC = replicateWebRTC;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _rxjs = require("rxjs");
var _plugin = require("../../plugin.js");
var _index = require("../../replication-protocol/index.js");
var _index2 = require("../../plugins/utils/index.js");
var _index3 = require("../leader-election/index.js");
var _index4 = require("../replication/index.js");
var _webrtcHelper = require("./webrtc-helper.js");
Object.keys(_webrtcHelper).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _webrtcHelper[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _webrtcHelper[key];
    }
  });
});
var _rxError = require("../../rx-error.js");
var _signalingServer = require("./signaling-server.js");
Object.keys(_signalingServer).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _signalingServer[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _signalingServer[key];
    }
  });
});
var _webrtcTypes = require("./webrtc-types.js");
Object.keys(_webrtcTypes).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _webrtcTypes[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _webrtcTypes[key];
    }
  });
});
var _connectionHandlerSimplePeer = require("./connection-handler-simple-peer.js");
Object.keys(_connectionHandlerSimplePeer).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _connectionHandlerSimplePeer[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _connectionHandlerSimplePeer[key];
    }
  });
});
var _connectionHandlerNostr = require("./connection-handler-nostr.js");
Object.keys(_connectionHandlerNostr).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _connectionHandlerNostr[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _connectionHandlerNostr[key];
    }
  });
});
/**
 * The methods of the master replication handler
 * that remote peers are allowed to call.
 */
var WEBRTC_MASTER_METHODS = ['masterChangesSince', 'masterWrite'];
async function replicateWebRTC(options) {
  var collection = options.collection;
  (0, _plugin.addRxPlugin)(_index3.RxDBLeaderElectionPlugin);

  // fill defaults
  if (options.pull) {
    if (!options.pull.batchSize) {
      options.pull.batchSize = 20;
    }
  }
  if (options.push) {
    if (!options.push.batchSize) {
      options.push.batchSize = 20;
    }
  }
  if (collection.database.multiInstance) {
    await collection.database.waitForLeadership();
  }

  // used to easier debug stuff
  var requestCounter = 0;
  var requestFlag = (0, _index2.randomToken)(10);
  function getRequestId() {
    var count = requestCounter++;
    return collection.database.token + '|' + requestFlag + '|' + count;
  }
  var storageToken = await collection.database.storageToken;
  var pool = new RxWebRTCReplicationPool(collection, options, await options.connectionHandlerCreator(options));
  var requestTimeout = options.requestTimeout ? options.requestTimeout : _webrtcHelper.WEBRTC_DEFAULT_REQUEST_TIMEOUT;
  var masterHandler = pool.masterReplicationHandler;
  function sendToPeer(peer, messageOrResponse) {
    return Promise.resolve().then(() => pool.connectionHandler.send(peer, messageOrResponse)).catch(() => {
      /**
       * Sending fails when the peer disconnected in the meantime.
       * This is handled by the disconnect$ stream so it can be ignored here.
       */
    });
  }
  pool.subs.push(pool.connectionHandler.error$.subscribe(err => pool.error$.next(err)), pool.connectionHandler.disconnect$.subscribe(peer => {
    pool.connectedPeers.delete(peer);
    pool.peerValidity.delete(peer);
    pool.removePeer(peer);
  }));

  /**
   * Answer the requests of other peers.
   * This is subscribed once for all peers
   * and independent of which side is master,
   * so that no request gets lost when the remote peer
   * finishes its handshake before the own side has finished it.
   */
  pool.subs.push(pool.connectionHandler.message$.subscribe(async data => {
    var {
      peer,
      message
    } = data;
    if (message.method === 'token') {
      sendToPeer(peer, {
        id: message.id,
        result: storageToken
      });
      return;
    }
    if (!WEBRTC_MASTER_METHODS.includes(message.method)) {
      return;
    }
    var isValid = await pool.isPeerValid(peer);
    if (!isValid || pool.canceled) {
      return;
    }
    var response;
    try {
      var result = await masterHandler[message.method](...message.params);
      response = {
        id: message.id,
        result
      };
    } catch (err) {
      response = {
        id: message.id,
        result: null,
        error: (0, _index2.errorToPlainJson)(err)
      };
    }
    sendToPeer(peer, response);
  }));
  var connectSub = pool.connectionHandler.connect$.pipe((0, _rxjs.filter)(() => !pool.canceled)).subscribe(async peer => {
    pool.connectedPeers.add(peer);
    var isValid = await pool.isPeerValid(peer);
    if (!isValid || !pool.isPeerConnected(peer)) {
      return;
    }
    var peerToken;
    try {
      var tokenResponse = await (0, _webrtcHelper.sendMessageAndAwaitAnswer)(pool.connectionHandler, peer, {
        id: getRequestId(),
        method: 'token',
        params: []
      }, requestTimeout);
      peerToken = tokenResponse.result;
    } catch (error) {
      /**
       * If could not get the tokenResponse,
       * just ignore that peer.
       */
      if (pool.isPeerConnected(peer)) {
        pool.error$.next((0, _rxError.newRxError)('RC_WEBRTC_PEER', {
          error: (0, _index2.errorToPlainJson)(error)
        }));
      }
      return;
    }
    var isMaster = await (0, _webrtcHelper.isMasterInWebRTCReplication)(collection.database.hashFunction, storageToken, peerToken);
    if (!pool.isPeerConnected(peer)) {
      return;
    }
    var replicationState;
    var subs = [];
    if (isMaster) {
      subs.push(masterHandler.masterChangeStream$.subscribe(ev => {
        sendToPeer(peer, {
          id: 'masterChangeStream$',
          result: ev
        });
      }));
      /**
       * Changes that happened between the initial pull of the
       * remote fork and the start of the change stream would be missed.
       * So we tell the fork to run a checkpoint iteration.
       */
      sendToPeer(peer, {
        id: 'masterChangeStream$',
        result: 'RESYNC'
      });
    } else {
      replicationState = (0, _index4.replicateRxCollection)({
        replicationIdentifier: [collection.name, options.topic, peerToken].join('||'),
        collection: collection,
        autoStart: true,
        deletedField: '_deleted',
        live: true,
        retryTime: options.retryTime,
        waitForLeadership: false,
        pull: options.pull ? Object.assign({}, options.pull, {
          async handler(lastPulledCheckpoint) {
            var answer = await (0, _webrtcHelper.sendMessageAndAwaitAnswer)(pool.connectionHandler, peer, {
              method: 'masterChangesSince',
              params: [lastPulledCheckpoint, (0, _index2.ensureNotFalsy)(options.pull).batchSize],
              id: getRequestId()
            }, requestTimeout);
            return answer.result;
          },
          stream$: pool.connectionHandler.response$.pipe((0, _rxjs.filter)(m => m.peer === peer), (0, _rxjs.filter)(m => m.response.id === 'masterChangeStream$'), (0, _rxjs.map)(m => m.response.result))
        }) : undefined,
        push: options.push ? Object.assign({}, options.push, {
          async handler(docs) {
            var answer = await (0, _webrtcHelper.sendMessageAndAwaitAnswer)(pool.connectionHandler, peer, {
              method: 'masterWrite',
              params: [docs],
              id: getRequestId()
            }, requestTimeout);
            return answer.result;
          }
        }) : undefined
      });
    }
    pool.addPeer(peer, peerToken, replicationState, subs);
  });
  pool.subs.push(connectSub);
  return pool;
}

/**
 * Because the WebRTC replication runs between many instances,
 * we use a Pool instead of returning a single replication state.
 */
var RxWebRTCReplicationPool = exports.RxWebRTCReplicationPool = /*#__PURE__*/function () {
  function RxWebRTCReplicationPool(collection, options, connectionHandler) {
    (0, _defineProperty2.default)(this, "peerStates$", new _rxjs.BehaviorSubject(new Map()));
    (0, _defineProperty2.default)(this, "canceled", false);
    (0, _defineProperty2.default)(this, "masterReplicationHandler", void 0);
    (0, _defineProperty2.default)(this, "subs", []);
    (0, _defineProperty2.default)(this, "peerValidity", new Map());
    (0, _defineProperty2.default)(this, "connectedPeers", new Set());
    (0, _defineProperty2.default)(this, "error$", new _rxjs.Subject());
    this.collection = collection;
    this.options = options;
    this.connectionHandler = connectionHandler;
    this.collection.onClose.push(() => this.cancel());
    this.masterReplicationHandler = (0, _index.rxStorageInstanceToReplicationHandler)(collection.storageInstance, collection.conflictHandler, collection.database.token);
  }

  /**
   * Returns true if the peer is valid.
   * The result is cached per peer so that
   * options.isPeerValid() is only called once.
   */
  var _proto = RxWebRTCReplicationPool.prototype;
  _proto.isPeerValid = function isPeerValid(peer) {
    var ret = this.peerValidity.get(peer);
    if (!ret) {
      var isPeerValidFn = this.options.isPeerValid;
      ret = isPeerValidFn ? Promise.resolve().then(() => isPeerValidFn(peer)).catch(() => false) : _index2.PROMISE_RESOLVE_TRUE;
      this.peerValidity.set(peer, ret);
    }
    return ret;
  };
  _proto.isPeerConnected = function isPeerConnected(peer) {
    return !this.canceled && this.connectedPeers.has(peer);
  };
  _proto.addPeer = function addPeer(peer, peerToken,
  // only if isMaster=false it has a replicationState
  replicationState, subs = []) {
    var peerState = {
      peer,
      peerToken,
      replicationState,
      subs
    };
    if (!this.isPeerConnected(peer)) {
      this.cleanupPeerState(peerState);
      return;
    }
    if (replicationState) {
      /**
       * When the connection to the signaling server is re-established,
       * it can happen that a second connection to the same remote instance is created.
       * Running two replications with the same replicationIdentifier would conflict,
       * so the replication of the outdated connection is canceled.
       */
      Array.from(this.peerStates$.getValue().values()).filter(otherState => otherState.peerToken === peerToken && otherState.replicationState).forEach(otherState => this.removePeer(otherState.peer));
      peerState.subs.push(replicationState.error$.subscribe(ev => this.error$.next(ev)));
    }
    this.peerStates$.next(this.peerStates$.getValue().set(peer, peerState));
  };
  _proto.removePeer = function removePeer(peer) {
    var peerStates = this.peerStates$.getValue();
    var peerState = peerStates.get(peer);
    if (!peerState) {
      return _index2.PROMISE_RESOLVE_VOID;
    }
    peerStates.delete(peer);
    this.peerStates$.next(peerStates);
    return this.cleanupPeerState(peerState);
  };
  _proto.cleanupPeerState = function cleanupPeerState(peerState) {
    peerState.subs.forEach(sub => sub.unsubscribe());
    if (peerState.replicationState) {
      return peerState.replicationState.cancel();
    }
    return _index2.PROMISE_RESOLVE_VOID;
  }

  // often used in unit tests
;
  _proto.awaitFirstPeer = function awaitFirstPeer() {
    return (0, _rxjs.firstValueFrom)(this.peerStates$.pipe((0, _rxjs.filter)(peerStates => peerStates.size > 0)));
  };
  _proto.cancel = async function cancel() {
    if (this.canceled) {
      return;
    }
    this.canceled = true;
    this.subs.forEach(sub => sub.unsubscribe());
    this.connectedPeers.clear();
    this.peerValidity.clear();
    /**
     * The replications must be fully canceled before the cancel() promise resolves,
     * otherwise they could still write to the storage after the database was closed.
     */
    await Promise.all(Array.from(this.peerStates$.getValue().keys()).map(peer => this.removePeer(peer)));
    await this.connectionHandler.close();
  };
  return RxWebRTCReplicationPool;
}(); // export * from './connection-handler-webtorrent';
// export * from './connection-handler-p2pcf';
//# sourceMappingURL=index.js.map