import { Subject } from 'rxjs';
import { PROMISE_RESOLVE_VOID, blobToBase64String, clone, createBlobFromBase64 } from "../../plugins/utils/index.js";
import { createWebSocketClient, startSocketServer } from "../replication-websocket/index.js";
import { exposeRxStorageRemote } from "../storage-remote/remote.js";
import { getRxStorageRemote } from "../storage-remote/rx-storage-remote.js";
import { createErrorAnswer } from "../storage-remote/storage-remote-helpers.js";
/**
 * WebSocket transport needs Blob<->base64 conversion at the JSON boundary.
 * These helpers handle the two message types that carry Blob data:
 * - bulkWrite request: document._attachments[id].data (Blob)
 * - getAttachmentData response: msg.return (Blob)
 */
async function serializeBlobsForWs(msg) {
  var msgForJson = clone(msg);
  if (msgForJson.method === 'bulkWrite' && Array.isArray(msgForJson.params)) {
    var documentWrites = msgForJson.params[0];
    if (Array.isArray(documentWrites)) {
      var toConvert = [];
      for (var row of documentWrites) {
        if (row.document?._attachments) {
          for (var attachment of Object.values(row.document._attachments)) {
            if (attachment.data instanceof Blob) {
              toConvert.push({
                attachment,
                blob: attachment.data
              });
            }
          }
        }
      }
      if (toConvert.length > 0) {
        var base64Results = await Promise.all(toConvert.map(entry => blobToBase64String(entry.blob)));
        for (var i = 0; i < toConvert.length; i++) {
          toConvert[i].attachment.data = base64Results[i];
        }
      }
    }
  } else if (msgForJson.method === 'getAttachmentData' && msgForJson.return instanceof Blob) {
    msgForJson.return = await blobToBase64String(msgForJson.return);
  }
  return JSON.stringify(msgForJson);
}
async function deserializeBlobsFromWs(msg) {
  if (msg.method === 'bulkWrite' && Array.isArray(msg.params)) {
    var documentWrites = msg.params[0];
    if (Array.isArray(documentWrites)) {
      for (var row of documentWrites) {
        if (row.document?._attachments) {
          for (var attachment of Object.values(row.document._attachments)) {
            if (typeof attachment.data === 'string') {
              attachment.data = await createBlobFromBase64(attachment.data, attachment.type || '');
            }
          }
        }
      }
    }
  } else if (msg.method === 'getAttachmentData' && typeof msg.return === 'string') {
    msg.return = await createBlobFromBase64(msg.return, '');
  }
  return msg;
}
export function startRxStorageRemoteWebsocketServer(options) {
  options.perMessageDeflate = true;
  var serverState = startSocketServer(options);
  var websocketByConnectionId = new Map();
  var messages$ = new Subject();
  var exposeSettings = {
    messages$: messages$.asObservable(),
    storage: options.storage,
    database: options.database,
    customRequestHandler: options.customRequestHandler,
    send(msg) {
      var ws = websocketByConnectionId.get(msg.connectionId);
      if (!ws) {
        // client disconnected, silently drop the message
        return;
      }
      serializeBlobsForWs(msg).then(serialized => {
        try {
          ws.send(serialized);
        } catch (err) {
          // WebSocket might have transitioned to CLOSING or CLOSED state
        }
      }).catch(() => {
        // serialization error, silently drop
      });
    },
    fakeVersion: options.fakeVersion
  };
  var exposeState = exposeRxStorageRemote(exposeSettings);
  serverState.onConnection$.subscribe(ws => {
    var onCloseHandlers = [];
    var connectionIds = new Set();
    ws.onclose = () => {
      onCloseHandlers.map(fn => fn());
      connectionIds.forEach(id => websocketByConnectionId.delete(id));
    };
    ws.on('message', messageString => {
      void (async () => {
        var message;
        try {
          message = JSON.parse(messageString);
          await deserializeBlobsFromWs(message);
        } catch (err) {
          console.error('RxDB WebSocket server: failed to parse or deserialize message', err);
          return;
        }
        var connectionId = message.connectionId;
        if (!websocketByConnectionId.has(connectionId)) {
          if (message.method !== 'create' && message.method !== 'custom') {
            try {
              ws.send(JSON.stringify(createErrorAnswer(message, new Error('First call must be a create call but is: ' + JSON.stringify(message)))));
            } catch (err) {
              // WebSocket might be in CLOSING or CLOSED state
            }
            return;
          }
          websocketByConnectionId.set(connectionId, ws);
          connectionIds.add(connectionId);
        }
        messages$.next(message);
      })();
    });
  });
  return {
    serverState,
    exposeState
  };
}
export function getRxStorageRemoteWebsocket(options) {
  var identifier = [options.url, 'rx-remote-storage-websocket'].join('');
  var storage = getRxStorageRemote({
    identifier,
    mode: options.mode,
    async messageChannelCreator() {
      var messages$ = new Subject();
      var websocketClient = await createWebSocketClient(options);
      websocketClient.message$.subscribe(msg => {
        void deserializeBlobsFromWs(msg).then(deserialized => {
          messages$.next(deserialized);
        }).catch(err => {
          console.error('RxDB WebSocket client: failed to deserialize incoming message', err);
        });
      });
      return {
        messages$,
        async send(msg) {
          var serialized = await serializeBlobsForWs(msg);
          websocketClient.socket.send(serialized);
        },
        close() {
          websocketClient.socket.close();
          return PROMISE_RESOLVE_VOID;
        }
      };
    }
  });
  return storage;
}
export * from "./types.js";
//# sourceMappingURL=index.js.map