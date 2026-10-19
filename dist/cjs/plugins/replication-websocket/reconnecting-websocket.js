"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ReconnectingWebSocket = void 0;
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _rxError = require("../../rx-error.js");
/**
 * A minimal websocket client that reconnects with an
 * exponential backoff when the connection is lost.
 * Messages that are sent while the client is not connected,
 * are queued and flushed as soon as the connection is open again.
 *
 * This replaces the unmaintained 'reconnecting-websocket' npm module
 * which was the only dependency of the websocket replication.
 */

var DEFAULT_OPTIONS = {
  minReconnectionDelay: 1000,
  maxReconnectionDelay: 10000,
  reconnectionDelayGrowFactor: 1.3,
  connectionTimeout: 4000,
  minUptime: 5000,
  maxRetries: Infinity
};
var ReconnectingWebSocket = exports.ReconnectingWebSocket = /*#__PURE__*/function () {
  function ReconnectingWebSocket(url, protocols = [], options = {}) {
    (0, _defineProperty2.default)(this, "onopen", null);
    (0, _defineProperty2.default)(this, "onclose", null);
    (0, _defineProperty2.default)(this, "onerror", null);
    (0, _defineProperty2.default)(this, "onmessage", null);
    (0, _defineProperty2.default)(this, "socket", null);
    /**
     * Amount of connection attempts that were started
     * since the last working connection.
     * Is -1 before the first attempt so that the
     * initial connect does not have to wait.
     */
    (0, _defineProperty2.default)(this, "retryCount", -1);
    (0, _defineProperty2.default)(this, "shouldReconnect", true);
    (0, _defineProperty2.default)(this, "isConnecting", false);
    (0, _defineProperty2.default)(this, "messageQueue", []);
    (0, _defineProperty2.default)(this, "connectionTimeoutId", void 0);
    (0, _defineProperty2.default)(this, "uptimeTimeoutId", void 0);
    (0, _defineProperty2.default)(this, "reconnectTimeoutId", void 0);
    this.url = url;
    this.protocols = protocols;
    this.options = options;
    this.connect();
  }
  var _proto = ReconnectingWebSocket.prototype;
  /**
   * Sends the data to the server.
   * When the client is not connected, the data is queued
   * and sent as soon as the connection is open again.
   */
  _proto.send = function send(data) {
    if (this.socket && this.socket.readyState === 1) {
      this.socket.send(data);
    } else {
      this.messageQueue.push(data);
    }
  }

  /**
   * Closes the connection and stops reconnecting.
   */;
  _proto.close = function close(code = 1000, reason) {
    this.shouldReconnect = false;
    this.clearTimeouts();
    var socket = this.socket;
    if (!socket) {
      return;
    }
    this.removeHandlers(socket);
    this.socket = null;
    try {
      socket.close(code, reason);
    } catch (err) {
      // the socket was not open, ignore
    }
  };
  _proto.getOption = function getOption(key) {
    var value = this.options[key];
    return typeof value === 'number' ? value : DEFAULT_OPTIONS[key];
  };
  _proto.getNextDelay = function getNextDelay() {
    if (this.retryCount < 1) {
      return 0;
    }
    var delay = this.getOption('minReconnectionDelay') * Math.pow(this.getOption('reconnectionDelayGrowFactor'), this.retryCount - 1);
    return Math.min(delay, this.getOption('maxReconnectionDelay'));
  };
  _proto.connect = function connect() {
    if (this.isConnecting || !this.shouldReconnect || this.retryCount >= this.getOption('maxRetries')) {
      return;
    }
    this.isConnecting = true;
    this.retryCount = this.retryCount + 1;
    this.reconnectTimeoutId = setTimeout(() => {
      if (!this.shouldReconnect) {
        this.isConnecting = false;
        return;
      }
      var WebSocketCtor = this.options.WebSocket ? this.options.WebSocket : typeof WebSocket !== 'undefined' ? WebSocket : undefined;
      var socket = new WebSocketCtor(this.url, this.protocols);
      this.socket = socket;
      this.isConnecting = false;
      socket.onopen = event => this.handleOpen(socket, event);
      socket.onmessage = event => {
        if (this.onmessage) {
          this.onmessage(event);
        }
      };
      socket.onerror = event => this.handleError(event);
      socket.onclose = event => this.handleClose(event);
      this.connectionTimeoutId = setTimeout(() => this.handleError({
        type: 'error',
        message: 'TIMEOUT',
        error: (0, _rxError.newRxError)('RC_WEBSOCKET_TIMEOUT', {
          url: this.url,
          args: {
            connectionTimeout: this.getOption('connectionTimeout')
          }
        }),
        target: this
      }), this.getOption('connectionTimeout'));
    }, this.getNextDelay());
  };
  _proto.handleOpen = function handleOpen(socket, event) {
    clearTimeout(this.connectionTimeoutId);
    /**
     * Only reset the retry counter when the connection
     * stayed open long enough. Otherwise a server that accepts and
     * instantly drops connections would cause a reconnect loop
     * without any delay.
     */
    this.uptimeTimeoutId = setTimeout(() => {
      this.retryCount = 0;
    }, this.getOption('minUptime'));
    var queue = this.messageQueue;
    this.messageQueue = [];
    queue.forEach(message => socket.send(message));
    if (this.onopen) {
      this.onopen(event);
    }
  };
  _proto.handleError = function handleError(event) {
    /**
     * An error always breaks the current connection,
     * so the socket is thrown away and a new one is created.
     */
    var socket = this.socket;
    this.clearTimeouts();
    if (socket) {
      this.removeHandlers(socket);
      this.socket = null;
      try {
        socket.close();
      } catch (err) {
        // the socket was not open, ignore
      }
    }
    if (this.onclose) {
      this.onclose({
        type: 'close',
        code: 1000,
        reason: '',
        wasClean: false,
        target: this
      });
    }
    if (this.onerror) {
      this.onerror(event);
    }
    this.connect();
  };
  _proto.handleClose = function handleClose(event) {
    this.clearTimeouts();
    if (this.socket) {
      this.removeHandlers(this.socket);
      this.socket = null;
    }
    if (this.onclose) {
      this.onclose(event);
    }
    this.connect();
  };
  _proto.removeHandlers = function removeHandlers(socket) {
    socket.onopen = null;
    socket.onclose = null;
    socket.onerror = null;
    socket.onmessage = null;
  };
  _proto.clearTimeouts = function clearTimeouts() {
    clearTimeout(this.connectionTimeoutId);
    clearTimeout(this.uptimeTimeoutId);
    clearTimeout(this.reconnectTimeoutId);
  };
  return (0, _createClass2.default)(ReconnectingWebSocket, [{
    key: "readyState",
    get: function () {
      return this.socket ? this.socket.readyState : 0;
    }
  }]);
}();
//# sourceMappingURL=reconnecting-websocket.js.map