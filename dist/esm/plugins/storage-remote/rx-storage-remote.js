import { firstValueFrom, filter, Subject } from 'rxjs';
import { RXDB_VERSION, randomToken } from "../../plugins/utils/index.js";
import { closeMessageChannel, getMessageChannel } from "./message-channel-cache.js";
export class RxStorageRemote {
  name = 'remote';
  rxdbVersion = RXDB_VERSION;
  seed = randomToken(10);
  lastRequestId = 0;
  messageChannelIfOneMode;
  constructor(settings) {
    this.settings = settings;
    if (settings.mode === 'one') {
      this.messageChannelIfOneMode = getMessageChannel(settings, [], true);
    }
  }
  getRequestId() {
    var newId = this.lastRequestId++;
    return this.seed + '|' + newId;
  }
  async createStorageInstance(params) {
    var connectionId = 'c|' + this.getRequestId();
    var cacheKeys = ['mode-' + this.settings.mode];
    switch (this.settings.mode) {
      case 'collection':
        cacheKeys.push('collection-' + params.collectionName);
      // eslint-disable-next-line no-fallthrough
      case 'database':
        cacheKeys.push('database-' + params.databaseName);
      // eslint-disable-next-line no-fallthrough
      case 'storage':
        cacheKeys.push('seed-' + this.seed);
    }
    var messageChannel = await (this.messageChannelIfOneMode ? this.messageChannelIfOneMode : getMessageChannel(this.settings, cacheKeys));
    var requestId = this.getRequestId();
    var waitForOkPromise = firstValueFrom(messageChannel.messages$.pipe(filter(msg => msg.answerTo === requestId)));
    messageChannel.send({
      connectionId,
      method: 'create',
      version: RXDB_VERSION,
      requestId,
      params
    });
    var waitForOkResult = await waitForOkPromise;
    if (waitForOkResult.error) {
      await closeMessageChannel(messageChannel);
      throw new Error('could not create instance ' + JSON.stringify(waitForOkResult.error));
    }

    /**
     * SECURITY: Remove the password from the stored params
     * so it does not leak through JSON.stringify() or other
     * enumeration of the storage instance internals.
     * The password has already been sent to the remote side
     * via the message channel and is no longer needed locally.
     */
    var paramsWithoutPassword = Object.assign({}, params);
    delete paramsWithoutPassword.password;
    return new RxStorageInstanceRemote(this, params.databaseName, params.collectionName, params.schema, {
      params: paramsWithoutPassword,
      connectionId,
      messageChannel
    }, params.options);
  }
  async customRequest(data) {
    var messageChannel = await this.settings.messageChannelCreator();
    var requestId = this.getRequestId();
    var connectionId = 'custom|request|' + requestId;
    var waitForAnswerPromise = firstValueFrom(messageChannel.messages$.pipe(filter(msg => msg.answerTo === requestId)));
    messageChannel.send({
      connectionId,
      method: 'custom',
      version: RXDB_VERSION,
      requestId,
      params: data
    });
    var response = await waitForAnswerPromise;
    if (response.error) {
      await messageChannel.close();
      throw new Error('could not run customRequest(): ' + JSON.stringify({
        data,
        error: response.error
      }));
    } else {
      await messageChannel.close();
      return response.return;
    }
  }
}

/**
 * Because postMessage() can be very slow on complex objects,
 * and some RxStorage implementations do need a JSON-string internally
 * anyway, it is allowed to transfer a string instead of an object
 * which must then be JSON.parse()-ed before RxDB can use it.
 * @link https://surma.dev/things/is-postmessage-slow/
 */
function getMessageReturn(msg) {
  if (msg.method === 'getAttachmentData') {
    return msg.return;
  } else {
    if (typeof msg.return === 'string') {
      return JSON.parse(msg.return);
    } else {
      return msg.return;
    }
  }
}
export class RxStorageInstanceRemote {
  changes$ = new Subject();
  subs = [];
  closed;
  messages$;
  constructor(storage, databaseName, collectionName, schema, internals, options) {
    this.storage = storage;
    this.databaseName = databaseName;
    this.collectionName = collectionName;
    this.schema = schema;
    this.internals = internals;
    this.options = options;
    this.messages$ = this.internals.messageChannel.messages$.pipe(filter(msg => msg.connectionId === this.internals.connectionId));
    this.subs.push(this.messages$.subscribe(msg => {
      if (msg.method === 'changeStream') {
        this.changes$.next(getMessageReturn(msg));
      }
    }));
  }
  async requestRemote(methodName, params) {
    var requestId = this.storage.getRequestId();
    var responsePromise = firstValueFrom(this.messages$.pipe(filter(msg => msg.answerTo === requestId)));
    var message = {
      connectionId: this.internals.connectionId,
      requestId,
      version: RXDB_VERSION,
      method: methodName,
      params
    };
    this.internals.messageChannel.send(message);
    var response = await responsePromise;
    if (response.error) {
      throw new Error('could not requestRemote: ' + JSON.stringify({
        methodName,
        params,
        error: response.error
      }, null, 4));
    } else {
      return getMessageReturn(response);
    }
  }
  bulkWrite(documentWrites, context) {
    return this.requestRemote('bulkWrite', [documentWrites, context]);
  }
  findDocumentsById(ids, deleted) {
    return this.requestRemote('findDocumentsById', [ids, deleted]);
  }
  query(preparedQuery) {
    return this.requestRemote('query', [preparedQuery]);
  }
  count(preparedQuery) {
    return this.requestRemote('count', [preparedQuery]);
  }
  getAttachmentData(documentId, attachmentId, digest) {
    return this.requestRemote('getAttachmentData', [documentId, attachmentId, digest]);
  }
  getChangedDocumentsSince(limit, checkpoint) {
    return this.requestRemote('getChangedDocumentsSince', [limit, checkpoint]);
  }
  changeStream() {
    return this.changes$.asObservable();
  }
  cleanup(minDeletedTime) {
    return this.requestRemote('cleanup', [minDeletedTime]);
  }
  async close() {
    if (this.closed) {
      return this.closed;
    }
    this.closed = (async () => {
      this.subs.forEach(sub => sub.unsubscribe());
      this.changes$.complete();
      await this.requestRemote('close', []);
      await closeMessageChannel(this.internals.messageChannel);
    })();
    return this.closed;
  }
  async remove() {
    if (this.closed) {
      throw new Error('already closed');
    }
    this.closed = (async () => {
      this.subs.forEach(sub => sub.unsubscribe());
      this.changes$.complete();
      await this.requestRemote('remove', []);
      await closeMessageChannel(this.internals.messageChannel);
    })();
    return this.closed;
  }
}
export function getRxStorageRemote(settings) {
  var withDefaults = Object.assign({
    mode: 'storage'
  }, settings);
  return new RxStorageRemote(withDefaults);
}
//# sourceMappingURL=rx-storage-remote.js.map