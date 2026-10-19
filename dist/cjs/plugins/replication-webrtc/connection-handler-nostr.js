"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.NOSTR_SIGNAL_BATCH_TIME = exports.NOSTR_SIGNALING_DEFAULT_EVENT_KIND = exports.NOSTR_PRESENCE_TIMEOUT = exports.NOSTR_PRESENCE_INTERVAL = exports.NOSTR_MAX_EVENT_AGE = void 0;
exports.getConnectionHandlerNostr = getConnectionHandlerNostr;
exports.getNostrPublicKeyOfPeer = getNostrPublicKeyOfPeer;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _pure = require("nostr-tools/pure");
var _nip = require("nostr-tools/nip44");
var _index = require("../../plugins/utils/index.js");
var _rxError = require("../../rx-error.js");
var _connectionHandlerSimplePeer = require("./connection-handler-simple-peer.js");
var NOSTR_SIGNALING_DEFAULT_EVENT_KIND = exports.NOSTR_SIGNALING_DEFAULT_EVENT_KIND = 25050;

/**
 * Each peer publishes its presence in this interval.
 * A peer is removed from the room when no presence
 * was received in NOSTR_PRESENCE_TIMEOUT.
 */
var NOSTR_PRESENCE_INTERVAL = exports.NOSTR_PRESENCE_INTERVAL = 1000 * 10;
var NOSTR_PRESENCE_TIMEOUT = exports.NOSTR_PRESENCE_TIMEOUT = NOSTR_PRESENCE_INTERVAL * 3 + 1000 * 5;

/**
 * Events with a created_at that differs more than this from the local time
 * are ignored so that replayed events do not trigger connection attempts.
 */
var NOSTR_MAX_EVENT_AGE = exports.NOSTR_MAX_EVENT_AGE = 1000 * 60 * 2;

/**
 * WebRTC creates many signals in a short time because of trickle ICE.
 * Public relays rate limit the number of events per second,
 * so the signals to the same peer are collected for this time
 * and then sent together in one event.
 */
var NOSTR_SIGNAL_BATCH_TIME = exports.NOSTR_SIGNAL_BATCH_TIME = 200;

/**
 * Used as prefix of the hashed topic
 * so that the topic is not readable on the relays.
 */
var NOSTR_TOPIC_HASH_PREFIX = 'rxdb-webrtc-nostr|';
var WEBSOCKET_STATE_CONNECTING = 0;
var WEBSOCKET_STATE_OPEN = 1;
var WEBSOCKET_STATE_CLOSED = 3;
/**
 * Returns the Nostr public key (hex) of a peer that
 * was connected with getConnectionHandlerNostr().
 * The key is verified by the event signatures of the signaling,
 * so it can be used in isPeerValid() to only allow known peers.
 */
function getNostrPublicKeyOfPeer(peer) {
  return peer.remotePeerId.split(':')[0];
}

/**
 * Returns a connection handler that uses Nostr relays
 * instead of a signaling server to connect the WebRTC peers.
 * The replicated data does not go over the relays,
 * only the WebRTC offers, answers and ICE candidates do.
 * @link https://nostr.how/en/what-is-nostr
 */
function getConnectionHandlerNostr({
  relays,
  secretKey,
  eventKind = NOSTR_SIGNALING_DEFAULT_EVENT_KIND,
  wrtc,
  config,
  webSocketConstructor = WebSocket
}) {
  if (!relays || relays.length === 0) {
    throw (0, _rxError.newRxError)('RC_WEBRTC_NOSTR', {
      errorText: 'at least one relay url must be given'
    });
  }
  if (eventKind < 20000 || eventKind > 29999) {
    throw (0, _rxError.newRxError)('RC_WEBRTC_NOSTR', {
      errorText: 'eventKind must be in the ephemeral range of 20000-29999',
      args: {
        eventKind
      }
    });
  }
  return options => {
    var ownSecretKey = secretKey ? secretKey : (0, _pure.generateSecretKey)();
    var ownPublicKey = (0, _pure.getPublicKey)(ownSecretKey);
    var signalingSocketClass = createNostrSignalingSocketClass({
      relays,
      secretKey: ownSecretKey,
      publicKey: ownPublicKey,
      eventKind,
      webSocketConstructor
    });
    return (0, _connectionHandlerSimplePeer.getConnectionHandlerSimplePeer)({
      signalingServerUrl: 'nostr:' + relays.join(','),
      wrtc,
      config,
      webSocketConstructor: signalingSocketClass
    })(options);
  };
}
/**
 * Creates a class that behaves like a WebSocket to the RxDB signaling server,
 * so that the peer handling of the simple-peer connection handler can be reused.
 * Internally it translates the signaling messages into Nostr events.
 */
function createNostrSignalingSocketClass(params) {
  var {
    relays,
    secretKey,
    publicKey,
    eventKind,
    webSocketConstructor
  } = params;
  var NostrSignalingSocket = /*#__PURE__*/function () {
    function NostrSignalingSocket(_url) {
      (0, _defineProperty2.default)(this, "readyState", WEBSOCKET_STATE_CONNECTING);
      (0, _defineProperty2.default)(this, "onopen", null);
      (0, _defineProperty2.default)(this, "onmessage", null);
      (0, _defineProperty2.default)(this, "onerror", null);
      (0, _defineProperty2.default)(this, "onclose", null);
      /**
       * A new session id for each socket so that the same secret key
       * can be used on multiple devices or browser tabs at the same time.
       */
      (0, _defineProperty2.default)(this, "session", (0, _index.randomToken)(10));
      (0, _defineProperty2.default)(this, "ownPeerId", publicKey + ':' + this.session);
      (0, _defineProperty2.default)(this, "subscriptionId", (0, _index.randomToken)(10));
      (0, _defineProperty2.default)(this, "relayConnections", void 0);
      (0, _defineProperty2.default)(this, "topicHash", void 0);
      (0, _defineProperty2.default)(this, "presenceInterval", void 0);
      /**
       * Last time a presence event was received, by peer id.
       */
      (0, _defineProperty2.default)(this, "roomPeers", new Map());
      (0, _defineProperty2.default)(this, "lastEmittedPeerIds", '');
      (0, _defineProperty2.default)(this, "seenEventIds", new Map());
      (0, _defineProperty2.default)(this, "conversationKeys", new Map());
      /**
       * Signals that wait to be sent, by receiver peer id.
       */
      (0, _defineProperty2.default)(this, "signalQueues", new Map());
      (0, _defineProperty2.default)(this, "signalTimeouts", new Map());
      this.relayConnections = relays.map(url => ({
        url,
        open: false,
        reconnectDelay: _connectionHandlerSimplePeer.SIMPLE_PEER_RECONNECT_DELAY_MIN
      }));
      this.relayConnections.forEach(relay => this.connectRelay(relay));
    }
    var _proto = NostrSignalingSocket.prototype;
    _proto.send = function send(data) {
      if (this.readyState !== WEBSOCKET_STATE_OPEN) {
        return;
      }
      var msg = JSON.parse(data);
      switch (msg.type) {
        case 'join':
          this.join(msg.room);
          break;
        case 'signal':
          this.queueSignal(msg.receiverPeerId, {
            connectionId: msg.connectionId,
            data: msg.data
          });
          break;
      }
    };
    _proto.close = function close() {
      if (this.readyState === WEBSOCKET_STATE_CLOSED) {
        return;
      }
      if (this.topicHash) {
        this.publish({
          type: 'leave',
          session: this.session
        });
      }
      this.readyState = WEBSOCKET_STATE_CLOSED;
      if (this.presenceInterval) {
        clearInterval(this.presenceInterval);
      }
      this.signalTimeouts.forEach(timeout => clearTimeout(timeout));
      this.signalTimeouts.clear();
      this.signalQueues.clear();
      this.relayConnections.forEach(relay => {
        if (relay.reconnectTimeout) {
          clearTimeout(relay.reconnectTimeout);
        }
        var ws = relay.ws;
        relay.ws = undefined;
        relay.open = false;
        if (ws) {
          try {
            ws.close();
          } catch (err) {}
        }
      });
      this.roomPeers.clear();
      this.conversationKeys.clear();
      this.seenEventIds.clear();
      if (this.onclose) {
        this.onclose({});
      }
    };
    _proto.emit = function emit(msg) {
      if (this.readyState === WEBSOCKET_STATE_OPEN && this.onmessage) {
        this.onmessage({
          data: JSON.stringify(msg)
        });
      }
    };
    _proto.connectRelay = function connectRelay(relay) {
      if (this.readyState === WEBSOCKET_STATE_CLOSED) {
        return;
      }
      var ws = new webSocketConstructor(relay.url);
      relay.ws = ws;
      ws.onopen = () => {
        if (relay.ws !== ws || this.readyState === WEBSOCKET_STATE_CLOSED) {
          return;
        }
        relay.open = true;
        relay.reconnectDelay = _connectionHandlerSimplePeer.SIMPLE_PEER_RECONNECT_DELAY_MIN;
        if (this.readyState === WEBSOCKET_STATE_CONNECTING) {
          this.readyState = WEBSOCKET_STATE_OPEN;
          if (this.onopen) {
            this.onopen({});
          }
          this.emit({
            type: 'init',
            yourPeerId: this.ownPeerId
          });
        } else if (this.topicHash) {
          this.subscribeRelay(relay);
          this.publishPresence();
        }
      };
      ws.onmessage = msgEvent => {
        if (relay.ws !== ws || this.readyState !== WEBSOCKET_STATE_OPEN) {
          return;
        }
        this.onRelayMessage(msgEvent.data);
      };
      ws.onerror = () => {
        /**
         * Errors are followed by a close event
         * which handles the reconnect.
         */
      };
      ws.onclose = () => {
        if (relay.ws !== ws) {
          return;
        }
        relay.ws = undefined;
        relay.open = false;
        if (this.readyState === WEBSOCKET_STATE_CLOSED) {
          return;
        }
        var delay = relay.reconnectDelay;
        relay.reconnectDelay = Math.min(delay * 2, _connectionHandlerSimplePeer.SIMPLE_PEER_RECONNECT_DELAY_MAX);
        relay.reconnectTimeout = setTimeout(() => {
          relay.reconnectTimeout = undefined;
          this.connectRelay(relay);
        }, delay);
      };
    };
    _proto.sendToRelay = function sendToRelay(relay, msg) {
      if (!relay.open || !relay.ws) {
        return;
      }
      try {
        relay.ws.send(JSON.stringify(msg));
      } catch (err) {}
    };
    _proto.subscribeRelay = function subscribeRelay(relay) {
      this.sendToRelay(relay, ['REQ', this.subscriptionId, {
        kinds: [eventKind],
        '#t': [this.topicHash],
        since: Math.floor((Date.now() - NOSTR_MAX_EVENT_AGE) / 1000)
      }]);
    };
    _proto.join = async function join(topic) {
      var topicHash = await (0, _index.defaultHashSha256)(NOSTR_TOPIC_HASH_PREFIX + topic);
      if (this.readyState !== WEBSOCKET_STATE_OPEN) {
        return;
      }
      this.topicHash = topicHash;
      this.relayConnections.forEach(relay => this.subscribeRelay(relay));
      this.publishPresence();
      this.presenceInterval = setInterval(() => {
        this.publishPresence();
        this.removeTimedOutPeers();
        this.cleanupSeenEvents();
      }, NOSTR_PRESENCE_INTERVAL);
      this.emitJoined(true);
    };
    _proto.publishPresence = function publishPresence() {
      this.publish({
        type: 'presence',
        session: this.session
      });
    };
    _proto.queueSignal = function queueSignal(receiverPeerId, signal) {
      var queue = this.signalQueues.get(receiverPeerId);
      if (!queue) {
        queue = [];
        this.signalQueues.set(receiverPeerId, queue);
      }
      queue.push(signal);
      if (this.signalTimeouts.has(receiverPeerId)) {
        return;
      }
      this.signalTimeouts.set(receiverPeerId, setTimeout(() => {
        this.signalTimeouts.delete(receiverPeerId);
        var signals = this.signalQueues.get(receiverPeerId);
        this.signalQueues.delete(receiverPeerId);
        if (signals && this.readyState === WEBSOCKET_STATE_OPEN) {
          this.publishSignals(receiverPeerId, signals);
        }
      }, NOSTR_SIGNAL_BATCH_TIME));
    };
    _proto.publishSignals = function publishSignals(receiverPeerId, signals) {
      var [receiverPublicKey, receiverSession] = receiverPeerId.split(':');
      var payload = {
        fromSession: this.session,
        toSession: receiverSession,
        signals
      };
      var encrypted;
      try {
        encrypted = (0, _nip.encrypt)(JSON.stringify(payload), this.getConversationKey(receiverPublicKey));
      } catch (err) {
        return;
      }
      this.publish({
        type: 'signal',
        payload: encrypted
      }, receiverPublicKey);
    };
    _proto.publish = function publish(content, receiverPublicKey) {
      if (!this.topicHash) {
        return;
      }
      var tags = [['t', this.topicHash]];
      if (receiverPublicKey) {
        tags.push(['p', receiverPublicKey]);
      }
      var event = (0, _pure.finalizeEvent)({
        kind: eventKind,
        created_at: Math.floor(Date.now() / 1000),
        tags,
        content: JSON.stringify(content)
      }, secretKey);
      this.relayConnections.forEach(relay => this.sendToRelay(relay, ['EVENT', event]));
    };
    _proto.getConversationKey = function getConversationKey(otherPublicKey) {
      var key = this.conversationKeys.get(otherPublicKey);
      if (!key) {
        key = (0, _nip.getConversationKey)(secretKey, otherPublicKey);
        this.conversationKeys.set(otherPublicKey, key);
      }
      return key;
    };
    _proto.onRelayMessage = function onRelayMessage(raw) {
      var msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch (err) {
        return;
      }
      if (!Array.isArray(msg) || msg[0] !== 'EVENT' || msg[1] !== this.subscriptionId || !msg[2] || typeof msg[2] !== 'object') {
        return;
      }
      var event = msg[2];

      /**
       * The same event arrives once from each relay.
       */
      if (typeof event.id !== 'string' || this.seenEventIds.has(event.id)) {
        return;
      }
      if (event.kind !== eventKind || !Array.isArray(event.tags) || !event.tags.some(tag => tag[0] === 't' && tag[1] === this.topicHash) || Math.abs(Date.now() - event.created_at * 1000) > NOSTR_MAX_EVENT_AGE) {
        return;
      }
      var isValid = false;
      try {
        isValid = (0, _pure.verifyEvent)(event);
      } catch (err) {}
      if (!isValid) {
        return;
      }
      this.seenEventIds.set(event.id, Date.now());
      var content;
      try {
        content = JSON.parse(event.content);
      } catch (err) {
        return;
      }
      if (!content || typeof content !== 'object') {
        return;
      }
      switch (content.type) {
        case 'presence':
          {
            if (typeof content.session !== 'string') {
              return;
            }
            var peerId = event.pubkey + ':' + content.session;
            if (peerId === this.ownPeerId) {
              return;
            }
            var isNew = !this.roomPeers.has(peerId);
            this.roomPeers.set(peerId, Date.now());
            if (isNew) {
              /**
               * Answer with the own presence directly
               * so that the new peer does not have to wait
               * for the next presence interval.
               */
              this.publishPresence();
              this.emitJoined();
            }
            break;
          }
        case 'leave':
          {
            var _peerId = event.pubkey + ':' + content.session;
            if (this.roomPeers.delete(_peerId)) {
              this.emitJoined();
            }
            break;
          }
        case 'signal':
          {
            if (typeof content.payload !== 'string' || !event.tags.some(tag => tag[0] === 'p' && tag[1] === publicKey)) {
              return;
            }
            var payload;
            try {
              payload = JSON.parse((0, _nip.decrypt)(content.payload, this.getConversationKey(event.pubkey)));
            } catch (err) {
              return;
            }
            if (!payload || payload.toSession !== this.session || !Array.isArray(payload.signals)) {
              return;
            }
            var senderPeerId = event.pubkey + ':' + payload.fromSession;
            if (senderPeerId === this.ownPeerId) {
              return;
            }
            payload.signals.forEach(signal => this.emit({
              type: 'signal',
              room: '',
              senderPeerId,
              receiverPeerId: this.ownPeerId,
              connectionId: signal.connectionId,
              data: signal.data
            }));
            break;
          }
      }
    };
    _proto.removeTimedOutPeers = function removeTimedOutPeers() {
      var minTime = Date.now() - NOSTR_PRESENCE_TIMEOUT;
      var changed = false;
      this.roomPeers.forEach((lastSeen, peerId) => {
        if (lastSeen < minTime) {
          this.roomPeers.delete(peerId);
          changed = true;
        }
      });
      if (changed) {
        this.emitJoined();
      }
    };
    _proto.cleanupSeenEvents = function cleanupSeenEvents() {
      var minTime = Date.now() - NOSTR_MAX_EVENT_AGE * 2;
      this.seenEventIds.forEach((time, id) => {
        if (time < minTime) {
          this.seenEventIds.delete(id);
        }
      });
    };
    _proto.emitJoined = function emitJoined(force = false) {
      var otherPeerIds = Array.from(this.roomPeers.keys()).sort();
      var key = otherPeerIds.join(',');
      if (!force && key === this.lastEmittedPeerIds) {
        return;
      }
      this.lastEmittedPeerIds = key;
      this.emit({
        type: 'joined',
        otherPeerIds
      });
    };
    return NostrSignalingSocket;
  }();
  return NostrSignalingSocket;
}
//# sourceMappingURL=connection-handler-nostr.js.map