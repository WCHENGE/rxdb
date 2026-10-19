"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.SIMPLE_PEER_RECONNECT_DELAY_MIN = exports.SIMPLE_PEER_RECONNECT_DELAY_MAX = exports.SIMPLE_PEER_PING_INTERVAL = exports.SIMPLE_PEER_MAX_MESSAGE_LENGTH = exports.SIMPLE_PEER_CONNECT_TIMEOUT = exports.DEFAULT_SIGNALING_SERVER = void 0;
exports.createSimplePeerWrtc = createSimplePeerWrtc;
exports.ensureProcessNextTickIsSet = ensureProcessNextTickIsSet;
exports.getConnectionHandlerSimplePeer = getConnectionHandlerSimplePeer;
var _inheritsLoose2 = _interopRequireDefault(require("@babel/runtime/helpers/inheritsLoose"));
var _rxjs = require("rxjs");
var _index = require("../../plugins/utils/index.js");
var _simplepeerMin = _interopRequireDefault(require("simple-peer/simplepeer.min.js"));
var _rxError = require("../../rx-error.js");
var Peer = _simplepeerMin.default;
var WEBSOCKET_STATE_OPEN = 1;
function sendMessage(ws, msg) {
  if (!ws || ws.readyState !== WEBSOCKET_STATE_OPEN) {
    return false;
  }
  try {
    ws.send(JSON.stringify(msg));
    return true;
  } catch (err) {
    return false;
  }
}
var DEFAULT_SIGNALING_SERVER_HOSTNAME = 'signaling.rxdb.info';
var DEFAULT_SIGNALING_SERVER = exports.DEFAULT_SIGNALING_SERVER = 'wss://' + DEFAULT_SIGNALING_SERVER_HOSTNAME + '/';
var defaultServerWarningShown = false;
/**
 * Creates a simple-peer compatible wrtc object from a WebRTC polyfill
 * like node-datachannel/polyfill. This is needed because some polyfills
 * return RTCSessionDescription objects with read-only properties
 * that simple-peer tries to mutate.
 */
function createSimplePeerWrtc(polyfill) {
  var OrigRTCPeerConnection = polyfill.RTCPeerConnection;
  var SimplePeerCompatRTCPeerConnection = /*#__PURE__*/function (_OrigRTCPeerConnectio) {
    function SimplePeerCompatRTCPeerConnection() {
      return _OrigRTCPeerConnectio.apply(this, arguments) || this;
    }
    (0, _inheritsLoose2.default)(SimplePeerCompatRTCPeerConnection, _OrigRTCPeerConnectio);
    var _proto = SimplePeerCompatRTCPeerConnection.prototype;
    _proto.createOffer = async function createOffer() {
      var offer = await _OrigRTCPeerConnectio.prototype.createOffer.call(this);
      return {
        type: offer.type,
        sdp: offer.sdp
      };
    };
    _proto.createAnswer = async function createAnswer() {
      var answer = await _OrigRTCPeerConnectio.prototype.createAnswer.call(this);
      return {
        type: answer.type,
        sdp: answer.sdp
      };
    };
    return SimplePeerCompatRTCPeerConnection;
  }(OrigRTCPeerConnection);
  return {
    RTCPeerConnection: SimplePeerCompatRTCPeerConnection,
    RTCSessionDescription: polyfill.RTCSessionDescription,
    RTCIceCandidate: polyfill.RTCIceCandidate
  };
}
var SIMPLE_PEER_PING_INTERVAL = exports.SIMPLE_PEER_PING_INTERVAL = 1000 * 60 * 2;

/**
 * If a peer connection is not established in this time,
 * it is destroyed and a new connection attempt is started.
 */
var SIMPLE_PEER_CONNECT_TIMEOUT = exports.SIMPLE_PEER_CONNECT_TIMEOUT = 1000 * 15;

/**
 * Min and max delay between reconnection attempts
 * to the signaling server and to other peers.
 * The delay doubles on each failed attempt.
 */
var SIMPLE_PEER_RECONNECT_DELAY_MIN = exports.SIMPLE_PEER_RECONNECT_DELAY_MIN = 500;
var SIMPLE_PEER_RECONNECT_DELAY_MAX = exports.SIMPLE_PEER_RECONNECT_DELAY_MAX = 1000 * 15;

/**
 * Messages bigger than this are split into chunks
 * because WebRTC data channels have a message size limit
 * which can be as low as 64 KiB depending on the browser.
 * The size is measured in string length and one character can
 * be up to 3 bytes in UTF-8.
 */
var SIMPLE_PEER_MAX_MESSAGE_LENGTH = exports.SIMPLE_PEER_MAX_MESSAGE_LENGTH = 1024 * 16;
/**
 * Returns a connection handler that uses simple-peer and the signaling server.
 */
function getConnectionHandlerSimplePeer({
  signalingServerUrl = DEFAULT_SIGNALING_SERVER,
  wrtc,
  config,
  webSocketConstructor = WebSocket
}) {
  ensureProcessNextTickIsSet();
  if (signalingServerUrl.includes(DEFAULT_SIGNALING_SERVER_HOSTNAME) && !defaultServerWarningShown) {
    defaultServerWarningShown = true;
    console.warn(['RxDB Warning: You are using the RxDB WebRTC replication plugin', 'but you did not specify your own signaling server url.', 'By default it will use a signaling server provided by RxDB at ' + DEFAULT_SIGNALING_SERVER, 'This server is made for demonstration purposes and tryouts. It is not reliable and might be offline at any time.', 'In production you must always use your own signaling server instead.', 'Learn how to run your own server at https://rxdb.info/replication-webrtc.html', 'Also leave a ⭐ at the RxDB github repo 🙏 https://github.com/pubkey/rxdb 🙏'].join(' '));
  }
  var creator = async options => {
    var connect$ = new _rxjs.Subject();
    var disconnect$ = new _rxjs.Subject();
    var message$ = new _rxjs.Subject();
    var response$ = new _rxjs.Subject();
    var error$ = new _rxjs.Subject();

    /**
     * The current peer connection by remote peer id.
     */
    var peers = new Map();
    /**
     * The ids of the other peers that are in the room,
     * as reported by the signaling server.
     */
    var roomPeerIds = new Set();
    var peerReconnectDelay = new Map();
    var peerReconnectTimeouts = new Map();
    var closed = false;
    var ownPeerId;
    var socket = undefined;
    var socketReconnectDelay = SIMPLE_PEER_RECONNECT_DELAY_MIN;
    var socketReconnectTimeout;

    /**
     * Send ping signals to the server.
     */
    var pingInterval = setInterval(() => {
      sendMessage(socket, {
        type: 'ping'
      });
    }, SIMPLE_PEER_PING_INTERVAL / 2);
    function isInitiator(remotePeerId) {
      return remotePeerId > (0, _index.ensureNotFalsy)(ownPeerId);
    }
    function endPeer(state) {
      if (state.ended) {
        return;
      }
      state.ended = true;
      if (state.connectTimeout) {
        clearTimeout(state.connectTimeout);
      }
      if (peers.get(state.remotePeerId) === state) {
        peers.delete(state.remotePeerId);
      }
      state.chunks.clear();
      try {
        state.peer.destroy();
      } catch (err) {}
      if (state.connected && !closed) {
        disconnect$.next(state.peer);
      }
      if (state.initiator) {
        scheduleReconnect(state.remotePeerId);
      }
    }

    /**
     * Only the initiator side actively reconnects.
     * The other side creates its peer when the offer arrives.
     */
    function scheduleReconnect(remotePeerId) {
      if (closed || !roomPeerIds.has(remotePeerId) || peers.has(remotePeerId) || peerReconnectTimeouts.has(remotePeerId)) {
        return;
      }
      var delay = peerReconnectDelay.get(remotePeerId) || SIMPLE_PEER_RECONNECT_DELAY_MIN;
      peerReconnectDelay.set(remotePeerId, Math.min(delay * 2, SIMPLE_PEER_RECONNECT_DELAY_MAX));
      peerReconnectTimeouts.set(remotePeerId, setTimeout(() => {
        peerReconnectTimeouts.delete(remotePeerId);
        if (!closed && ownPeerId && roomPeerIds.has(remotePeerId) && !peers.has(remotePeerId) && isInitiator(remotePeerId)) {
          createPeerConnection(remotePeerId, true, (0, _index.randomToken)(10));
        }
      }, delay));
    }
    function processIncomingData(state, messageOrResponse) {
      if (messageOrResponse.chunk) {
        var chunk = messageOrResponse.chunk;
        var parts = state.chunks.get(chunk.id);
        if (!parts) {
          parts = [];
          state.chunks.set(chunk.id, parts);
        }
        parts[chunk.index] = chunk.data;
        if (parts.filter(p => typeof p === 'string').length < chunk.total) {
          return;
        }
        state.chunks.delete(chunk.id);
        messageOrResponse = JSON.parse(parts.join(''));
      }
      if (typeof messageOrResponse.method === 'string') {
        message$.next({
          peer: state.peer,
          message: messageOrResponse
        });
      } else {
        response$.next({
          peer: state.peer,
          response: messageOrResponse
        });
      }
    }
    function createPeerConnection(remotePeerId, initiator, connectionId) {
      var previous = peers.get(remotePeerId);
      if (previous) {
        endPeer(previous);
      }
      var newSimplePeer = new Peer({
        initiator,
        wrtc,
        config,
        trickle: true
      });
      newSimplePeer.id = (0, _index.randomToken)(10);
      newSimplePeer.remotePeerId = remotePeerId;
      var state = {
        remotePeerId,
        connectionId,
        initiator,
        peer: newSimplePeer,
        connected: false,
        ended: false,
        chunks: new Map()
      };
      peers.set(remotePeerId, state);
      state.connectTimeout = setTimeout(() => {
        if (!state.connected) {
          endPeer(state);
        }
      }, SIMPLE_PEER_CONNECT_TIMEOUT);
      newSimplePeer.on('signal', signal => {
        if (state.ended || !ownPeerId) {
          return;
        }
        var signalMessage = {
          type: 'signal',
          senderPeerId: ownPeerId,
          receiverPeerId: remotePeerId,
          room: options.topic,
          data: signal
        };
        if (connectionId) {
          signalMessage.connectionId = connectionId;
        }
        sendMessage(socket, signalMessage);
      });
      newSimplePeer.on('data', data => {
        if (state.ended) {
          return;
        }
        var parsed;
        try {
          parsed = JSON.parse(data.toString());
        } catch (err) {
          return;
        }
        if (parsed && typeof parsed === 'object') {
          processIncomingData(state, parsed);
        }
      });
      newSimplePeer.on('error', error => {
        if (state.ended) {
          return;
        }
        if (!closed) {
          error$.next((0, _rxError.newRxError)('RC_WEBRTC_PEER', {
            error: (0, _index.errorToPlainJson)(error)
          }));
        }
        endPeer(state);
      });
      newSimplePeer.on('connect', () => {
        if (state.ended) {
          return;
        }
        state.connected = true;
        if (state.connectTimeout) {
          clearTimeout(state.connectTimeout);
        }
        peerReconnectDelay.delete(remotePeerId);
        connect$.next(newSimplePeer);
      });
      newSimplePeer.on('close', () => endPeer(state));
      return state;
    }
    function onSignal(msg) {
      if (!ownPeerId || msg.receiverPeerId !== ownPeerId) {
        return;
      }
      var remotePeerId = msg.senderPeerId;
      var state = peers.get(remotePeerId);
      if (isInitiator(remotePeerId)) {
        /**
         * Older RxDB versions do not send the connectionId,
         * so a missing one must be accepted.
         */
        if (!state || state.ended || msg.connectionId && msg.connectionId !== state.connectionId) {
          return;
        }
      } else {
        var isOffer = msg.data && msg.data.type === 'offer';
        var isOutdated = !state || state.ended || state.connectionId !== msg.connectionId;
        if (isOutdated) {
          if (!isOffer) {
            return;
          }
          state = createPeerConnection(remotePeerId, false, msg.connectionId);
        }
      }
      try {
        (0, _index.ensureNotFalsy)(state).peer.signal(msg.data);
      } catch (err) {
        endPeer((0, _index.ensureNotFalsy)(state));
      }
    }
    function onJoined(msg) {
      if (!ownPeerId) {
        return;
      }
      roomPeerIds = new Set(msg.otherPeerIds.filter(id => id !== ownPeerId));

      /**
       * Peers that left the room and never got connected
       * can be removed. Connected peers are kept because the
       * WebRTC connection can still work when only the connection
       * to the signaling server was lost.
       */
      Array.from(peers.values()).forEach(state => {
        if (!roomPeerIds.has(state.remotePeerId) && !state.connected) {
          endPeer(state);
        }
      });
      Array.from(peerReconnectTimeouts.entries()).forEach(([remotePeerId, timeout]) => {
        if (!roomPeerIds.has(remotePeerId)) {
          clearTimeout(timeout);
          peerReconnectTimeouts.delete(remotePeerId);
          peerReconnectDelay.delete(remotePeerId);
        }
      });
      roomPeerIds.forEach(remotePeerId => {
        if (!peers.has(remotePeerId) && !peerReconnectTimeouts.has(remotePeerId) && isInitiator(remotePeerId)) {
          createPeerConnection(remotePeerId, true, (0, _index.randomToken)(10));
        }
      });
    }
    function onSocketMessage(msgEvent) {
      var msg;
      try {
        msg = JSON.parse(msgEvent.data.toString());
      } catch (err) {
        return;
      }
      switch (msg.type) {
        case 'init':
          ownPeerId = msg.yourPeerId;
          sendMessage(socket, {
            type: 'join',
            room: options.topic
          });
          break;
        case 'joined':
          onJoined(msg);
          break;
        case 'signal':
          onSignal(msg);
          break;
      }
    }

    /**
     * Creates the WebSocket connection to the signaling server.
     * On disconnects it reconnects with an increasing delay
     * so that it does not spin when the server is not reachable.
     */
    function createSocket() {
      if (closed) {
        return;
      }
      var ws = new webSocketConstructor(signalingServerUrl);
      socket = ws;
      ws.onopen = () => {
        socketReconnectDelay = SIMPLE_PEER_RECONNECT_DELAY_MIN;
      };
      ws.onmessage = msgEvent => {
        if (socket === ws && !closed) {
          onSocketMessage(msgEvent);
        }
      };
      ws.onerror = () => {
        /**
         * Errors are followed by a close event
         * which handles the reconnect.
         */
      };
      ws.onclose = () => {
        if (socket !== ws) {
          return;
        }
        socket = undefined;
        ownPeerId = undefined;
        if (closed) {
          return;
        }
        var delay = socketReconnectDelay;
        socketReconnectDelay = Math.min(delay * 2, SIMPLE_PEER_RECONNECT_DELAY_MAX);
        socketReconnectTimeout = setTimeout(() => {
          socketReconnectTimeout = undefined;
          createSocket();
        }, delay);
      };
    }
    createSocket();
    var handler = {
      error$,
      connect$,
      disconnect$,
      message$,
      response$,
      async send(peer, message) {
        if (peer.destroyed) {
          throw (0, _rxError.newRxError)('RC_WEBRTC_PEER', {
            errorText: 'peer is destroyed'
          });
        }
        var messageString = JSON.stringify(message);
        if (messageString.length <= SIMPLE_PEER_MAX_MESSAGE_LENGTH) {
          peer.send(messageString);
          return;
        }
        var chunkId = (0, _index.randomToken)(10);
        var total = Math.ceil(messageString.length / SIMPLE_PEER_MAX_MESSAGE_LENGTH);
        for (var index = 0; index < total; index++) {
          var chunk = {
            chunk: {
              id: chunkId,
              index,
              total,
              data: messageString.slice(index * SIMPLE_PEER_MAX_MESSAGE_LENGTH, (index + 1) * SIMPLE_PEER_MAX_MESSAGE_LENGTH)
            }
          };
          peer.send(JSON.stringify(chunk));
        }
      },
      close() {
        if (closed) {
          return _index.PROMISE_RESOLVE_VOID;
        }
        closed = true;
        clearInterval(pingInterval);
        if (socketReconnectTimeout) {
          clearTimeout(socketReconnectTimeout);
        }
        peerReconnectTimeouts.forEach(timeout => clearTimeout(timeout));
        peerReconnectTimeouts.clear();
        Array.from(peers.values()).forEach(state => endPeer(state));
        if (socket) {
          var ws = socket;
          socket = undefined;
          try {
            ws.close();
          } catch (err) {}
        }
        error$.complete();
        connect$.complete();
        disconnect$.complete();
        message$.complete();
        response$.complete();
        return _index.PROMISE_RESOLVE_VOID;
      }
    };
    return handler;
  };
  return creator;
}

/**
 * Multiple people had problems because it requires to have
 * the nextTick() method in the runtime. So we check here and
 * throw a helpful error.
 */
function ensureProcessNextTickIsSet() {
  if (typeof process === 'undefined' || typeof process.nextTick !== 'function') {
    throw (0, _rxError.newRxError)('RC7');
  }
}
//# sourceMappingURL=connection-handler-simple-peer.js.map