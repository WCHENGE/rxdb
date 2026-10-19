import { BehaviorSubject, filter, firstValueFrom, map, Subject } from 'rxjs';
import { addRxPlugin } from "../../plugin.js";
import { rxStorageInstanceToReplicationHandler } from "../../replication-protocol/index.js";
import { ensureNotFalsy, errorToPlainJson, PROMISE_RESOLVE_TRUE, PROMISE_RESOLVE_VOID, randomToken } from "../../plugins/utils/index.js";
import { RxDBLeaderElectionPlugin } from "../leader-election/index.js";
import { replicateRxCollection } from "../replication/index.js";
import { isMasterInWebRTCReplication, sendMessageAndAwaitAnswer, WEBRTC_DEFAULT_REQUEST_TIMEOUT } from "./webrtc-helper.js";
import { newRxError } from "../../rx-error.js";

/**
 * The methods of the master replication handler
 * that remote peers are allowed to call.
 */
var WEBRTC_MASTER_METHODS = ['masterChangesSince', 'masterWrite'];
export async function replicateWebRTC(options) {
  var collection = options.collection;
  addRxPlugin(RxDBLeaderElectionPlugin);

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
  var requestFlag = randomToken(10);
  function getRequestId() {
    var count = requestCounter++;
    return collection.database.token + '|' + requestFlag + '|' + count;
  }
  var storageToken = await collection.database.storageToken;
  var pool = new RxWebRTCReplicationPool(collection, options, await options.connectionHandlerCreator(options));
  var requestTimeout = options.requestTimeout ? options.requestTimeout : WEBRTC_DEFAULT_REQUEST_TIMEOUT;
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
        error: errorToPlainJson(err)
      };
    }
    sendToPeer(peer, response);
  }));
  var connectSub = pool.connectionHandler.connect$.pipe(filter(() => !pool.canceled)).subscribe(async peer => {
    pool.connectedPeers.add(peer);
    var isValid = await pool.isPeerValid(peer);
    if (!isValid || !pool.isPeerConnected(peer)) {
      return;
    }
    var peerToken;
    try {
      var tokenResponse = await sendMessageAndAwaitAnswer(pool.connectionHandler, peer, {
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
        pool.error$.next(newRxError('RC_WEBRTC_PEER', {
          error: errorToPlainJson(error)
        }));
      }
      return;
    }
    var isMaster = await isMasterInWebRTCReplication(collection.database.hashFunction, storageToken, peerToken);
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
      replicationState = replicateRxCollection({
        replicationIdentifier: [collection.name, options.topic, peerToken].join('||'),
        collection: collection,
        autoStart: true,
        deletedField: '_deleted',
        live: true,
        retryTime: options.retryTime,
        waitForLeadership: false,
        pull: options.pull ? Object.assign({}, options.pull, {
          async handler(lastPulledCheckpoint) {
            var answer = await sendMessageAndAwaitAnswer(pool.connectionHandler, peer, {
              method: 'masterChangesSince',
              params: [lastPulledCheckpoint, ensureNotFalsy(options.pull).batchSize],
              id: getRequestId()
            }, requestTimeout);
            return answer.result;
          },
          stream$: pool.connectionHandler.response$.pipe(filter(m => m.peer === peer), filter(m => m.response.id === 'masterChangeStream$'), map(m => m.response.result))
        }) : undefined,
        push: options.push ? Object.assign({}, options.push, {
          async handler(docs) {
            var answer = await sendMessageAndAwaitAnswer(pool.connectionHandler, peer, {
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
export class RxWebRTCReplicationPool {
  peerStates$ = new BehaviorSubject(new Map());
  canceled = false;
  masterReplicationHandler;
  subs = [];
  peerValidity = new Map();
  connectedPeers = new Set();
  error$ = new Subject();
  constructor(collection, options, connectionHandler) {
    this.collection = collection;
    this.options = options;
    this.connectionHandler = connectionHandler;
    this.collection.onClose.push(() => this.cancel());
    this.masterReplicationHandler = rxStorageInstanceToReplicationHandler(collection.storageInstance, collection.conflictHandler, collection.database.token);
  }

  /**
   * Returns true if the peer is valid.
   * The result is cached per peer so that
   * options.isPeerValid() is only called once.
   */
  isPeerValid(peer) {
    var ret = this.peerValidity.get(peer);
    if (!ret) {
      var isPeerValidFn = this.options.isPeerValid;
      ret = isPeerValidFn ? Promise.resolve().then(() => isPeerValidFn(peer)).catch(() => false) : PROMISE_RESOLVE_TRUE;
      this.peerValidity.set(peer, ret);
    }
    return ret;
  }
  isPeerConnected(peer) {
    return !this.canceled && this.connectedPeers.has(peer);
  }
  addPeer(peer, peerToken,
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
  }
  removePeer(peer) {
    var peerStates = this.peerStates$.getValue();
    var peerState = peerStates.get(peer);
    if (!peerState) {
      return PROMISE_RESOLVE_VOID;
    }
    peerStates.delete(peer);
    this.peerStates$.next(peerStates);
    return this.cleanupPeerState(peerState);
  }
  cleanupPeerState(peerState) {
    peerState.subs.forEach(sub => sub.unsubscribe());
    if (peerState.replicationState) {
      return peerState.replicationState.cancel();
    }
    return PROMISE_RESOLVE_VOID;
  }

  // often used in unit tests
  awaitFirstPeer() {
    return firstValueFrom(this.peerStates$.pipe(filter(peerStates => peerStates.size > 0)));
  }
  async cancel() {
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
  }
}
export * from "./webrtc-helper.js";
export * from "./signaling-server.js";
export * from "./webrtc-types.js";
// export * from './connection-handler-webtorrent';
// export * from './connection-handler-p2pcf';
export * from "./connection-handler-simple-peer.js";
export * from "./connection-handler-nostr.js";
//# sourceMappingURL=index.js.map