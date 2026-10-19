import { RxDBLeaderElectionPlugin } from "../leader-election/index.js";
import { RxReplicationState, startReplicationOnLeaderShip } from "../replication/index.js";
import { addRxPlugin } from "../../index.js";
import { Subject } from 'rxjs';
import { initDriveStructure } from "./init.js";
import { handleUpstreamBatch } from "./upstream.js";
import { fetchChanges } from "./downstream.js";
import { runInTransaction } from "./transaction.js";
import { ensureProcessNextTickIsSet } from "../replication-webrtc/connection-handler-simple-peer.js";
import { SignalingState } from "./signaling.js";
import { deserializeDocAttachments, serializeDocAttachments } from "./document-handling.js";
export * from "./google-drive-types.js";
export * from "./google-drive-helper.js";
export * from "./transaction.js";
export * from "./init.js";
export * from "./document-handling.js";
export * from "./multipart.js";
export * from "./downstream.js";
export * from "./upstream.js";
export * from "./signaling.js";
export var DEFAULT_TRANSACTION_TIMEOUT = 60 * 1000;
export class RxGoogleDriveReplicationState extends RxReplicationState {
  /**
   * Only exists on live replication
   */
  signalingState;
  constructor(googleDrive, driveStructure, replicationIdentifierHash, collection, pull, push, signalingOptions, live = true, retryTime = 1000 * 5, autoStart = true) {
    super(replicationIdentifierHash, collection, '_deleted', pull, push, live, retryTime, autoStart);
    this.googleDrive = googleDrive;
    this.driveStructure = driveStructure;
    this.replicationIdentifierHash = replicationIdentifierHash;
    this.collection = collection;
    this.pull = pull;
    this.push = push;
    this.signalingOptions = signalingOptions;
    this.live = live;
    this.retryTime = retryTime;
    this.autoStart = autoStart;
  }

  /**
   * Notify other peers that something
   * has or might have changed so that
   * they can pull from their checkpoints.
   */
  async notifyPeers() {
    if (this.signalingState) {
      await this.signalingState.pingPeers('RESYNC');
    }
  }
}
export async function replicateGoogleDrive(options) {
  var collection = options.collection;
  addRxPlugin(RxDBLeaderElectionPlugin);
  var googleDriveOptionsWithDefaults = Object.assign({
    apiEndpoint: 'https://www.googleapis.com',
    transactionTimeout: DEFAULT_TRANSACTION_TIMEOUT,
    space: 'drive',
    folderPath: ''
  }, options.googleDrive);
  if (typeof googleDriveOptionsWithDefaults.folderPath !== 'string') {
    googleDriveOptionsWithDefaults.folderPath = '';
  }
  var driveStructure = await initDriveStructure(googleDriveOptionsWithDefaults);

  /**
   * When true, attachment binary data is serialised as base64 inside the
   * document JSON file stored on Google Drive.  Defaults to true only when
   * the collection schema has `attachments: {}` defined.  Can be disabled
   * explicitly by passing `attachments: false` in the options.
   */
  var replicateAttachments = options.attachments !== false && !!collection.schema.jsonSchema.attachments;
  var replicationState;
  var pullStream$ = new Subject();
  var replicationPrimitivesPull;
  options.live = typeof options.live === 'undefined' ? true : options.live;
  options.waitForLeadership = typeof options.waitForLeadership === 'undefined' ? true : options.waitForLeadership;
  if (options.pull) {
    replicationPrimitivesPull = {
      async handler(lastPulledCheckpoint, batchSize) {
        return runInTransaction(googleDriveOptionsWithDefaults, driveStructure, collection.schema.primaryPath, async () => {
          var changes = await fetchChanges(googleDriveOptionsWithDefaults, driveStructure, lastPulledCheckpoint, batchSize);
          /**
           * Convert base64 attachment data that was stored in the
           * Google Drive JSON file back to Blobs so that the
           * downstream replication protocol can write them to the
           * fork storage instance correctly.
           */
          if (replicateAttachments) {
            await Promise.all(changes.documents.map(doc => deserializeDocAttachments(doc)));
          }
          return changes;
        });
      },
      batchSize: options.pull.batchSize,
      modifier: options.pull.modifier,
      stream$: pullStream$.asObservable(),
      initialCheckpoint: options.pull.initialCheckpoint
    };
  }
  var replicationPrimitivesPush;
  if (options.push) {
    replicationPrimitivesPush = {
      async handler(rows) {
        /**
         * Convert Blob attachment data to base64 strings before the
         * rows are written to the WAL file or stored as document JSON
         * in Google Drive.  We create new row objects so the originals
         * (which the replication protocol still uses for meta updates)
         * are not mutated.
         * When attachments are disabled we still run the conversion so
         * that Blob values are stripped rather than serialised as `{}`
         * by JSON.stringify.
         */
        var rowsForDrive = await Promise.all(rows.map(async row => {
          var newState = await serializeDocAttachments(row.newDocumentState, replicateAttachments);
          return {
            ...row,
            newDocumentState: newState
          };
        }));
        return runInTransaction(googleDriveOptionsWithDefaults, driveStructure, collection.schema.primaryPath, async () => {
          var conflicts = await handleUpstreamBatch(googleDriveOptionsWithDefaults, driveStructure, options.collection.schema.primaryPath, rowsForDrive);
          /**
           * When attachment replication is enabled, deserialise the
           * base64 attachment data that Google Drive stored in the
           * `_attachments_data` field back into Blobs on each conflict
           * document before returning them to the replication protocol.
           *
           * The protocol passes these as `realMasterState` to
           * `resolveConflictError`, which (with the fix in
           * `conflicts.ts`) then writes the resolved state — including
           * the correct attachment Blob — to the fork instance.
           * Without this deserialisation the fork would receive stubs
           * with no binary data and the attachment would be lost.
           */
          if (replicateAttachments && conflicts.length > 0) {
            await Promise.all(conflicts.map(c => deserializeDocAttachments(c)));
          }
          return conflicts;
        }, () => replicationState.notifyPeers().catch(() => {}));
      },
      batchSize: options.push.batchSize,
      modifier: options.push.modifier
    };
  }
  replicationState = new RxGoogleDriveReplicationState(googleDriveOptionsWithDefaults, driveStructure, options.replicationIdentifier, collection, replicationPrimitivesPull, replicationPrimitivesPush, options.signalingOptions, options.live, options.retryTime, options.autoStart);

  /**
   * Google drive has no websocket or server-send-events
   * to observe file changes. Therefore we use WebRTC to
   * connect clients which then can ping each other on changes.
   * Instead of a signaling server, we use the google-drive itself
   * to exchange signaling data.
   */
  if (options.live && options.pull) {
    ensureProcessNextTickIsSet();
    var startBefore = replicationState.start.bind(replicationState);
    var cancelBefore = replicationState.cancel.bind(replicationState);
    replicationState.start = () => {
      replicationState.signalingState = new SignalingState(replicationState.googleDrive, replicationState.driveStructure, options.signalingOptions ? options.signalingOptions : {});
      var sub = replicationState.signalingState.resync$.subscribe(() => {
        replicationState.reSync();
      });
      replicationState.cancel = () => {
        sub.unsubscribe();
        replicationState.signalingState?.close();
        return cancelBefore();
      };
      return startBefore();
    };
  }
  startReplicationOnLeaderShip(options.waitForLeadership, replicationState);
  return replicationState;
}
//# sourceMappingURL=index.js.map