import _Peer from 'simple-peer';
import { ensureProcessNextTickIsSet } from '../replication-webrtc/connection-handler-simple-peer';
import { ensureNotFalsy, getFromMapOrCreate, lastOfArray, now, PROMISE_RESOLVE_VOID, promiseWait, promiseWaitSkippable, randomToken } from "../utils/index.js";
import { applyDriveSpace, deleteFile, insertMultipartFile, readJsonFileContent } from "./google-drive-helper.js";
import { Subject } from 'rxjs';
import { newRxFetchError } from "../../rx-error.js";
/**
 * Timings on when to call the google drive
 * api to check for new messages.
 */
var BACKOFF_STEPS = [50, 50, 100, 100, 200, 400, 600, 1_000, 2_000, 4_000, 8_000, 15_000, 30_000, 60_000, 120_000];
var MAX_BACKOFF_STEP_ID = BACKOFF_STEPS.length - 1;
export class SignalingState {
  sessionId = randomToken(12);
  processedMessageIds = new Set();

  /**
   * Emits whenever a new connection
   * is there or some connection
   * told us to RESYNC
   */
  _resync$ = new Subject();
  resync$ = this._resync$.asObservable();
  peerBySenderId = new Map();
  processQueue = PROMISE_RESOLVE_VOID;
  backoffStepId = 0;
  skipBackoffTime;
  closed = false;
  resetReaderFn = () => {
    this.resetReadLoop();
  };
  constructor(googleDriveOptions, init, signalingOptions) {
    this.googleDriveOptions = googleDriveOptions;
    this.init = init;
    this.signalingOptions = signalingOptions;
    ensureProcessNextTickIsSet();
    cleanupOldSignalingMessages(this.googleDriveOptions, this.init.signalingFolderId).catch(() => {});

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
        var time = BACKOFF_STEPS[this.backoffStepId];
        await this.processNewMessages();
        var skippable = promiseWaitSkippable(time);
        this.skipBackoffTime = skippable.skip;
        await skippable.promise;
        this.backoffStepId = this.backoffStepId + 1;
        if (this.backoffStepId > MAX_BACKOFF_STEP_ID) {
          this.backoffStepId = MAX_BACKOFF_STEP_ID;
        }
      }
    })();
  }
  async sendMessage(data) {
    var messageId = randomToken(12);
    var fileName = [this.sessionId, now(), messageId].join('_') + '.json';

    // add here so we skip these
    this.processedMessageIds.add(messageId);
    await insertMultipartFile(this.googleDriveOptions, this.init.signalingFolderId, fileName, data);
  }
  async pingPeers(message) {
    Array.from(this.peerBySenderId.values()).forEach(peer => {
      if (peer.connected) {
        peer.send(message);
      }
    });
  }
  async resetReadLoop() {
    await this.processNewMessages();
    this.backoffStepId = 0;
    if (this.skipBackoffTime) {
      this.skipBackoffTime();
    }
  }
  async processNewMessages() {
    this.processQueue = this.processQueue.then(() => this._processNewMessages().catch(() => {}));
    return this.processQueue;
  }
  async _processNewMessages() {
    var messages = await readMessages(this.googleDriveOptions, this.init, this.processedMessageIds);
    if (messages.length > 0) {
      this._resync$.next();
    }
    messages.forEach(message => {
      var senderId = message.senderId;
      if (senderId === this.sessionId) {
        return;
      }
      var peerInstance;
      peerInstance = getFromMapOrCreate(this.peerBySenderId, senderId, () => {
        var peer = new _Peer({
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
        promiseWait().then(() => this._resync$.next());
        return peer;
      });
      if (!message.data.i) {
        peerInstance.signal(message.data);
      }
    });
  }
  close() {
    this.closed = true;
    if (typeof window !== 'undefined') {
      window.removeEventListener('online', this.resetReaderFn);
      window.removeEventListener('visibilitychange', this.resetReaderFn);
    }
    Array.from(this.peerBySenderId.values()).forEach(peer => peer.destroy());
    this._resync$.complete();
  }
}
export async function readMessages(googleDriveOptions, init, processedMessageIds) {
  // ----------------------------
  // INLINE: readFolderById logic
  // ----------------------------
  var query = ["'" + init.signalingFolderId + "' in parents", "trashed = false" // If you want to restrict to json only:
  // `mimeType = 'application/json'`
  // If you prefix signaling files:
  // `name contains 'sig__'`
  ].join(' and ');
  var fields = 'files(id,name,mimeType,createdTime,parents),nextPageToken';
  var params = new URLSearchParams();
  params.set('q', query);
  params.set('fields', fields);
  /**
   * Only fetch the "newest" page.
   * Later invert the order.
   */
  params.set('orderBy', 'createdTime desc');
  params.set('pageSize', '1000');
  applyDriveSpace(googleDriveOptions, params);
  var listUrl = googleDriveOptions.apiEndpoint + '/drive/v3/files?' + params.toString();
  var listResponse = await fetch(listUrl, {
    method: 'GET',
    headers: {
      Authorization: 'Bearer ' + googleDriveOptions.authToken
    }
  });
  if (!listResponse.ok) {
    throw await newRxFetchError(listResponse);
  }
  var listData = await listResponse.json();
  var folderData = listData.files || [];
  folderData = folderData.reverse();
  var useFiles = folderData.filter(file => {
    var messageId = messageIdByFilename(file.name);
    return !processedMessageIds.has(messageId);
  });
  var filesContent = await Promise.all(useFiles.map(async file => {
    var fileContent = await readJsonFileContent(googleDriveOptions, file.id);
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
  var messageId = ensureNotFalsy(lastOfArray(fileName.split('_')));
  return messageId;
}
var DEFAULT_MAX_AGE_MS = 24 * 60 * 60 * 1000; // 1 day

export async function cleanupOldSignalingMessages(googleDriveOptions, signalingFolderId, maxAgeMs = DEFAULT_MAX_AGE_MS) {
  return 2;
  var cutoffDate = new Date(Date.now() - maxAgeMs).toISOString();

  // Hardcoded folderId + query parts (Drive will do the filtering server-side)
  var query = ["'" + signalingFolderId + "' in parents", "trashed = false", "mimeType = 'application/json'", "createdTime < '" + cutoffDate + "'" // Recommended if folder may contain other JSON:
  // `name contains 'sig__'`
  ].join(' and ');

  // Hardcoded fields for cleanup
  var fields = 'files(id,name,createdTime),nextPageToken';
  var params = new URLSearchParams();
  params.set('q', query);
  params.set('fields', fields);
  params.set('orderBy', 'createdTime asc');
  params.set('pageSize', '1000');
  applyDriveSpace(googleDriveOptions, params);
  var url = googleDriveOptions.apiEndpoint + '/drive/v3/files?' + params.toString();
  var listResponse = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: 'Bearer ' + googleDriveOptions.authToken
    }
  });
  if (!listResponse.ok) {
    throw await newRxFetchError(listResponse);
  }
  var listData = await listResponse.json();
  var oldFiles = listData.files || [];
  if (!oldFiles.length) {
    return 0;
  }
  await Promise.all(oldFiles.map(file => deleteFile(googleDriveOptions, file.id).catch(() => {})));
  return oldFiles.length;
}
//# sourceMappingURL=signaling.js.map