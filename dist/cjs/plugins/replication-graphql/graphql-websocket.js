"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.GRAPHQL_WEBSOCKET_BY_URL = void 0;
exports.getGraphQLWebSocket = getGraphQLWebSocket;
exports.removeGraphQLWebSocketRef = removeGraphQLWebSocketRef;
var _client = require("graphql-ws/client");
var _index = require("../../plugins/utils/index.js");
var _ws = require("ws");
/**
 * Import from 'graphql-ws/client' instead of 'graphql-ws' so that
 * only the zero-dependency client is bundled. The package root also
 * contains the server code which pulls in the whole graphql package.
 * @link https://github.com/enisdenjo/graphql-ws/blob/master/CHANGELOG.md#600
 */

var GRAPHQL_WEBSOCKET_BY_URL = exports.GRAPHQL_WEBSOCKET_BY_URL = new Map();
function getGraphQLWebSocket(url, headers, options = {}) {
  var has = (0, _index.getFromMapOrCreate)(GRAPHQL_WEBSOCKET_BY_URL, url, () => {
    var connectionParamsHeaders = headers ? {
      headers
    } : undefined;
    var wsClient = (0, _client.createClient)({
      ...options,
      url,
      shouldRetry: () => true,
      webSocketImpl: _ws.WebSocket,
      connectionParams: options.connectionParams || connectionParamsHeaders
    });
    return {
      url,
      socket: wsClient,
      refCount: 1
    };
  }, value => {
    value.refCount = value.refCount + 1;
  });
  return has.socket;
}
function removeGraphQLWebSocketRef(url) {
  var obj = (0, _index.getFromMapOrThrow)(GRAPHQL_WEBSOCKET_BY_URL, url);
  obj.refCount = obj.refCount - 1;
  if (obj.refCount === 0) {
    GRAPHQL_WEBSOCKET_BY_URL.delete(url);
    obj.socket.dispose();
  }
}
//# sourceMappingURL=graphql-websocket.js.map