import * as path from 'node:path';
import { BehaviorSubject, firstValueFrom, Subject } from 'rxjs';
import { filter, map } from 'rxjs';
import { getFromMapOrCreate, PROMISE_RESOLVE_FALSE, PROMISE_RESOLVE_TRUE, PROMISE_RESOLVE_VOID } from "../../plugins/utils/index.js";
import { clearFolder, deleteFolder, documentFolder, ensureFolderExists, getMeta, prepareFolders, setMeta, writeJsonToFile, writeToFile } from "./file-util.js";
import { getChangedDocumentsSince } from "../../rx-storage-helper.js";

/**
 * Backups a single documents,
 * returns the paths to all written files
 */
export async function backupSingleDocument(rxDocument, options) {
  var data = rxDocument.toJSON(true);
  var writtenFiles = [];
  var docFolder = documentFolder(options, rxDocument.primary);
  await clearFolder(docFolder);
  var fileLocation = path.join(docFolder, 'document.json');
  await writeJsonToFile(fileLocation, data);
  writtenFiles.push(fileLocation);
  if (options.attachments) {
    var attachmentsFolder = path.join(docFolder, 'attachments');
    ensureFolderExists(attachmentsFolder);
    var attachments = rxDocument.allAttachments();
    await Promise.all(attachments.map(async attachment => {
      var content = await attachment.getData();
      var attachmentFileLocation = path.join(attachmentsFolder, attachment.id);
      await writeToFile(attachmentFileLocation, content);
      writtenFiles.push(attachmentFileLocation);
    }));
  }
  return writtenFiles;
}
var BACKUP_STATES_BY_DB = new WeakMap();
function addToBackupStates(db, state) {
  var ar = getFromMapOrCreate(BACKUP_STATES_BY_DB, db, () => []);
  ar.push(state);
}
export class RxBackupState {
  isStopped = false;
  subs = [];
  persistRunning = PROMISE_RESOLVE_VOID;
  initialReplicationDone$ = new BehaviorSubject(false);
  internalWriteEvents$ = new Subject();
  writeEvents$ = this.internalWriteEvents$.asObservable();
  constructor(database, options) {
    this.database = database;
    this.options = options;
    if (!this.options.batchSize) {
      this.options.batchSize = 10;
    }
    addToBackupStates(database, this);
    prepareFolders(database, options);
  }

  /**
   * Persists all data from all collections,
   * beginning from the oldest sequence checkpoint
   * to the newest one.
   * Do not call this while it is already running.
   * Returns true if there are more documents to process
   */
  persistOnce() {
    return this.persistRunning = this.persistRunning.then(() => this._persistOnce());
  }
  async _persistOnce() {
    var _this = this;
    var meta = await getMeta(this.options);
    await Promise.all(Object.entries(this.database.collections).map(async ([collectionName, collection]) => {
      var primaryKey = collection.schema.primaryPath;
      var processedDocuments = new Set();
      await this.database.requestIdlePromise();
      if (!meta.collectionStates[collectionName]) {
        meta.collectionStates[collectionName] = {};
      }
      var lastCheckpoint = meta.collectionStates[collectionName].checkpoint;
      var hasMore = true;
      var _loop = async function () {
        await _this.database.requestIdlePromise();
        var changesResult = await getChangedDocumentsSince(collection.storageInstance, _this.options.batchSize ? _this.options.batchSize : 0, lastCheckpoint);
        if (changesResult.documents.length === 0) {
          hasMore = false;
          return 1; // continue
        }
        lastCheckpoint = changesResult.checkpoint;
        meta.collectionStates[collectionName].checkpoint = lastCheckpoint;
        var docIds = changesResult.documents.map(doc => doc[primaryKey]).filter(id => {
          if (processedDocuments.has(id)) {
            return false;
          } else {
            processedDocuments.add(id);
            return true;
          }
        }).filter((elem, pos, arr) => arr.indexOf(elem) === pos); // unique
        await _this.database.requestIdlePromise();
        var docs = await collection.findByIds(docIds).exec();
        await Promise.all(Array.from(docs.values()).map(async doc => {
          var writtenFiles = await backupSingleDocument(doc, _this.options);
          _this.internalWriteEvents$.next({
            collectionName: collection.name,
            documentId: doc.primary,
            files: writtenFiles,
            deleted: false
          });
        }));
        // handle deleted documents
        await Promise.all(docIds.filter(docId => !docs.has(docId)).map(async docId => {
          await deleteFolder(documentFolder(_this.options, docId));
          _this.internalWriteEvents$.next({
            collectionName: collection.name,
            documentId: docId,
            files: [],
            deleted: true
          });
        }));
      };
      while (hasMore && !this.isStopped) {
        if (await _loop()) continue;
      }
      meta.collectionStates[collectionName].checkpoint = lastCheckpoint;
      await setMeta(this.options, meta);
    }));
    if (!this.initialReplicationDone$.getValue()) {
      this.initialReplicationDone$.next(true);
    }
  }
  watchForChanges() {
    var collections = Object.values(this.database.collections);
    collections.forEach(collection => {
      var changes$ = collection.storageInstance.changeStream();
      var sub = changes$.subscribe(() => {
        this.persistOnce();
      });
      this.subs.push(sub);
    });
  }

  /**
   * Returns a promise that resolves when the initial backup is done
   * and the filesystem is in sync with the database state
   */
  awaitInitialBackup() {
    return firstValueFrom(this.initialReplicationDone$.pipe(filter(v => !!v), map(() => true)));
  }
  cancel() {
    if (this.isStopped) {
      return PROMISE_RESOLVE_FALSE;
    }
    this.isStopped = true;
    this.subs.forEach(sub => sub.unsubscribe());
    return PROMISE_RESOLVE_TRUE;
  }
}
export function backup(options) {
  var backupState = new RxBackupState(this, options);
  backupState.persistOnce();
  if (options.live) {
    backupState.watchForChanges();
  }
  return backupState;
}
export * from "./file-util.js";
export var RxDBBackupPlugin = {
  name: 'backup',
  rxdb: true,
  prototypes: {
    RxDatabase(proto) {
      proto.backup = backup;
    }
  },
  hooks: {
    preCloseRxDatabase: {
      after: function preCloseRxDatabase(db) {
        var states = BACKUP_STATES_BY_DB.get(db);
        if (states) {
          states.forEach(state => state.cancel());
        }
      }
    }
  }
};
//# sourceMappingURL=index.js.map