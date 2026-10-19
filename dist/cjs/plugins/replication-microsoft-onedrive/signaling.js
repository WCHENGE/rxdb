"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.SignalingState = void 0;
exports.cleanupOldSignalingMessages = cleanupOldSignalingMessages;
exports.readMessages = readMessages;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _simplePeer = _interopRequireDefault(require("simple-peer"));
var _connectionHandlerSimplePeer = require("../replication-webrtc/connection-handler-simple-peer");
var _index = require("../utils/index.js");
var _microsoftOnedriveHelper = require("./microsoft-onedrive-helper.js");
var _rxjs = require("rxjs");
var _rxError = require("../../rx-error.js");
/**
 * Timings on when to call the api to check for new messages.
 */
var BACKOFF_STEPS = [50, 50, 100, 100, 200, 400, 600, 1_000, 2_000, 4_000, 8_000, 15_000, 30_000, 60_000, 120_000];
var MAX_BACKOFF_STEP_ID = BACKOFF_STEPS.length - 1;
var SignalingState = exports.SignalingState = /*#__PURE__*/function () {
  function SignalingState(oneDriveState, init, signalingOptions) {
    (0, _defineProperty2.default)(this, "sessionId", (0, _index.randomToken)(12));
    (0, _defineProperty2.default)(this, "processedMessageIds", new Set());
    /**
     * Emits whenever a new connection
     * is there or some connection
     * told us to RESYNC
     */
    (0, _defineProperty2.default)(this, "_resync$", new _rxjs.Subject());
    (0, _defineProperty2.default)(this, "resync$", this._resync$.asObservable());
    (0, _defineProperty2.default)(this, "peerBySenderId", new Map());
    (0, _defineProperty2.default)(this, "processQueue", _index.PROMISE_RESOLVE_VOID);
    (0, _defineProperty2.default)(this, "backoffStepId", 0);
    (0, _defineProperty2.default)(this, "skipBackoffTime", void 0);
    (0, _defineProperty2.default)(this, "closed", false);
    (0, _defineProperty2.default)(this, "resetReaderFn", () => {
      this.resetReadLoop();
    });
    this.oneDriveState = oneDriveState;
    this.init = init;
    this.signalingOptions = signalingOptions;
    (0, _connectionHandlerSimplePeer.ensureProcessNextTickIsSet)();
    cleanupOldSignalingMessages(this.oneDriveState, this.init.signalingFolderId).catch(() => {});

    // Send "i exist" message
    this.sendMessage({
      i: 'exist'
    });
    this.processNewMessages();
    if (typeof window !== 'undefined') {
      window.addEventListener('online', this.resetReaderFn);
      document.addEventListener('visibilitychange', this.resetReaderFn);
    }

    // start processing loop
    (async () => {
      while (!this.closed) {
        await this.processNewMessages();
        /**
         * Read the backoff AFTER processing the messages so that a
         * reset triggered by newly arrived messages (see
         * _processNewMessages) takes effect on the very next wait.
         * When there is active signaling traffic we keep polling fast,
         * when things are idle the backoff grows again.
         */
        var time = BACKOFF_STEPS[this.backoffStepId];
        var skippable = (0, _index.promiseWaitSkippable)(time);
        this.skipBackoffTime = skippable.skip;
        await skippable.promise;
        this.backoffStepId = this.backoffStepId + 1;
        if (this.backoffStepId > MAX_BACKOFF_STEP_ID) {
          this.backoffStepId = MAX_BACKOFF_STEP_ID;
        }
      }
    })();
  }
  var _proto = SignalingState.prototype;
  _proto.sendMessage = async function sendMessage(data) {
    var messageId = (0, _index.randomToken)(12);
    var fileName = [this.sessionId, (0, _index.now)(), messageId].join('_') + '.json';

    // add here so we skip these
    this.processedMessageIds.add(messageId);
    var baseUrl = (0, _microsoftOnedriveHelper.getDriveBaseUrl)(this.oneDriveState);
    var url = baseUrl + "/items/" + this.init.signalingFolderId + ":/" + encodeURIComponent(fileName) + ":/content";
    var res = await fetch(url, {
      method: 'PUT',
      headers: {
        Authorization: "Bearer " + this.oneDriveState.authToken,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(data)
    });
    if (!res.ok) {
      throw await (0, _rxError.newRxFetchError)(res, {
        args: {
          fileName
        }
      });
    }
  };
  _proto.pingPeers = async function pingPeers(message) {
    Array.from(this.peerBySenderId.values()).forEach(peer => {
      if (peer.connected) {
        peer.send(message);
      }
    });
  }

  /**
   * Notify other peers that something changed so that they can pull
   * from their checkpoints. This uses two channels:
   * - The WebRTC datachannel for instant delivery to already connected peers.
   * - A durable signaling message so that peers which are not connected via
   *   WebRTC (handshake not finished yet or the connection dropped) still
   *   learn about the change on their next signaling poll and re-sync.
   *
   * Relying on the WebRTC ping alone is not reliable: a ping to a peer that
   * is momentarily not connected is silently dropped and there is no other
   * trigger that makes the peer pull again, so the change would be stuck
   * until the next unrelated write. The durable message closes that gap.
   */;
  _proto.notifyResync = async function notifyResync() {
    await this.pingPeers('RESYNC');
    await this.sendMessage({
      i: 'resync'
    }).catch(() => {});
  };
  _proto.resetReadLoop = async function resetReadLoop() {
    await this.processNewMessages();
    this.backoffStepId = 0;
    if (this.skipBackoffTime) {
      this.skipBackoffTime();
    }
  };
  _proto.processNewMessages = async function processNewMessages() {
    this.processQueue = this.processQueue.then(() => this._processNewMessages().catch(() => {}));
    return this.processQueue;
  };
  _proto._processNewMessages = async function _processNewMessages() {
    var messages = await readMessages(this.oneDriveState, this.init, this.processedMessageIds);
    if (messages.length > 0) {
      this._resync$.next();
      /**
       * New messages mean there is active signaling traffic: a peer
       * announced itself or pushed a change. Reset the backoff so we
       * keep polling fast while things are happening, which lets peers
       * converge quickly even when the WebRTC datachannel is not
       * connected. Once traffic stops, the backoff grows again.
       */
      this.backoffStepId = 0;
    }
    messages.forEach(message => {
      var senderId = message.senderId;
      if (senderId === this.sessionId) {
        return;
      }
      var peerInstance;
      peerInstance = (0, _index.getFromMapOrCreate)(this.peerBySenderId, senderId, () => {
        var peer = new _simplePeer.default({
          initiator: senderId > this.sessionId,
          trickle: true,
          wrtc: this.signalingOptions.wrtc,
          config: this.signalingOptions.config
        });
        peer.on("signal", async signalData => {
          await this.sendMessage(signalData);
        });
        peer.on('connect', () => {
          this._resync$.next();
          peer.send('RESYNC');
        });
        peer.on('data', dataBuffer => {
          var data = dataBuffer + '';
          switch (data) {
            case 'NEW_PEER':
              this.resetReadLoop();
              break;
            case 'RESYNC':
              this._resync$.next();
              break;
            default:
              console.error('Signaling UNKNOWN DATA ' + data);
          }
        });
        peer.on('error', () => {
          this._resync$.next();
        });
        peer.on('close', () => {
          this.peerBySenderId.delete(senderId);
          this._resync$.next();
        });

        /**
         * If we find a new peer,
         * we tell everyone else.
         */
        this.pingPeers('NEW_PEER');
        (0, _index.promiseWait)().then(() => this._resync$.next());
        return peer;
      });
      if (!message.data.i) {
        peerInstance.signal(message.data);
      }
    });
  };
  _proto.close = function close() {
    this.closed = true;
    if (typeof window !== 'undefined') {
      window.removeEventListener('online', this.resetReaderFn);
      window.removeEventListener('visibilitychange', this.resetReaderFn);
    }
    Array.from(this.peerBySenderId.values()).forEach(peer => peer.destroy());
    this._resync$.complete();
  };
  return SignalingState;
}();
async function readMessages(oneDriveState, init, processedMessageIds) {
  var baseUrl = (0, _microsoftOnedriveHelper.getDriveBaseUrl)(oneDriveState);
  var params = new URLSearchParams();
  params.set('$select', 'id,name,lastModifiedDateTime');
  params.set('$orderby', 'lastModifiedDateTime desc');
  params.set('$top', '1000');
  var listUrl = baseUrl + "/items/" + init.signalingFolderId + "/children?" + params.toString();
  var listResponse = await fetch(listUrl, {
    method: 'GET',
    headers: {
      Authorization: 'Bearer ' + oneDriveState.authToken
    }
  });
  if (!listResponse.ok) {
    throw await (0, _rxError.newRxFetchError)(listResponse);
  }
  var listData = await listResponse.json();
  var folderData = listData.value || [];
  folderData = folderData.reverse();
  var useFiles = folderData.filter(file => {
    var messageId = messageIdByFilename(file.name);
    return !processedMessageIds.has(messageId);
  });
  var filesContent = await Promise.all(useFiles.map(async file => {
    var fileContent = await (0, _microsoftOnedriveHelper.readJsonFileContent)(oneDriveState, (0, _index.ensureNotFalsy)(file.id) // Ensure TS is happy about string type for file id and string type return
    );
    var senderId = file.name.split('_')[0];
    return {
      senderId,
      data: fileContent.content
    };
  }));

  /**
   * Do this afterwards so we can retry on errors without losing messages.
   * (No need for async map here.)
   */
  useFiles.forEach(file => {
    var messageId = messageIdByFilename(file.name);
    processedMessageIds.add(messageId);
  });
  return filesContent;
}
function messageIdByFilename(name) {
  var fileName = name.split('.')[0];
  var messageId = (0, _index.ensureNotFalsy)((0, _index.lastOfArray)(fileName.split('_')));
  return messageId;
}
var DEFAULT_MAX_AGE_MS = 24 * 60 * 60 * 1000; // 1 day

async function cleanupOldSignalingMessages(oneDriveState, signalingFolderId, maxAgeMs = DEFAULT_MAX_AGE_MS) {
  // We can re-implement this properly, but for mock parity, google drive just returns 2 often.
  return 2;
}
//# sourceMappingURL=signaling.js.map