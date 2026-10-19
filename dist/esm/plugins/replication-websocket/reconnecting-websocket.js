import { newRxError } from "../../rx-error.js";

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
export class ReconnectingWebSocket {
  onopen = null;
  onclose = null;
  onerror = null;
  onmessage = null;
  socket = null;
  /**
   * Amount of connection attempts that were started
   * since the last working connection.
   * Is -1 before the first attempt so that the
   * initial connect does not have to wait.
   */
  retryCount = -1;
  shouldReconnect = true;
  isConnecting = false;
  messageQueue = [];
  connectionTimeoutId;
  uptimeTimeoutId;
  reconnectTimeoutId;
  constructor(url, protocols = [], options = {}) {
    this.url = url;
    this.protocols = protocols;
    this.options = options;
    this.connect();
  }
  get readyState() {
    return this.socket ? this.socket.readyState : 0;
  }

  /**
   * Sends the data to the server.
   * When the client is not connected, the data is queued
   * and sent as soon as the connection is open again.
   */
  send(data) {
    if (this.socket && this.socket.readyState === 1) {
      this.socket.send(data);
    } else {
      this.messageQueue.push(data);
    }
  }

  /**
   * Closes the connection and stops reconnecting.
   */
  close(code = 1000, reason) {
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
  }
  getOption(key) {
    var value = this.options[key];
    return typeof value === 'number' ? value : DEFAULT_OPTIONS[key];
  }
  getNextDelay() {
    if (this.retryCount < 1) {
      return 0;
    }
    var delay = this.getOption('minReconnectionDelay') * Math.pow(this.getOption('reconnectionDelayGrowFactor'), this.retryCount - 1);
    return Math.min(delay, this.getOption('maxReconnectionDelay'));
  }
  connect() {
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
        error: newRxError('RC_WEBSOCKET_TIMEOUT', {
          url: this.url,
          args: {
            connectionTimeout: this.getOption('connectionTimeout')
          }
        }),
        target: this
      }), this.getOption('connectionTimeout'));
    }, this.getNextDelay());
  }
  handleOpen(socket, event) {
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
  }
  handleError(event) {
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
  }
  handleClose(event) {
    this.clearTimeouts();
    if (this.socket) {
      this.removeHandlers(this.socket);
      this.socket = null;
    }
    if (this.onclose) {
      this.onclose(event);
    }
    this.connect();
  }
  removeHandlers(socket) {
    socket.onopen = null;
    socket.onclose = null;
    socket.onerror = null;
    socket.onmessage = null;
  }
  clearTimeouts() {
    clearTimeout(this.connectionTimeoutId);
    clearTimeout(this.uptimeTimeoutId);
    clearTimeout(this.reconnectTimeoutId);
  }
}
//# sourceMappingURL=reconnecting-websocket.js.map