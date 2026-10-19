import { ensureFolderExists, listFilesInFolder, createEmptyFile, fillFileIfEtagMatches, readJsonFileContent } from "./microsoft-onedrive-helper.js";
import { newRxError } from "../../rx-error.js";
import { randomToken } from "../utils/utils-string.js";
import { TRANSACTION_FILE_NAME } from "./transaction.js";
import { ensureNotFalsy } from "../utils/index.js";
import { WAL_FILE_NAME } from "./upstream.js";
var NON_ALLOWED_ROOT_FOLDERS = ['/', '', null, false, undefined, 'root'];
export async function initDriveStructure(oneDriveState) {
  NON_ALLOWED_ROOT_FOLDERS.forEach(nonAllowed => {
    if (oneDriveState.folderPath === nonAllowed) {
      throw newRxError('ODR1', {
        folderPath: oneDriveState.folderPath
      });
    }
  });

  // root folder
  var rootFolderId = await ensureFolderExists(oneDriveState, oneDriveState.folderPath);
  var rootFolderContent = await listFilesInFolder(oneDriveState, rootFolderId);

  /**
   * Folder must either be empty
   * or already used as a RxDB onedrive sync target.
   */
  var hasRxDBJson = rootFolderContent.find(file => file.name === 'rxdb.json');
  var hasOther = rootFolderContent.find(file => file.name !== 'rxdb.json');
  if (hasOther && !hasRxDBJson) {
    throw newRxError('ODR9', {
      folderPath: oneDriveState.folderPath
    });
  }

  /**
   * Create rxdb.json file.
   * This must always be the first step.
   */
  var rxdbJson = await createEmptyFile(oneDriveState, rootFolderId, 'rxdb.json');
  var replicationIdentifier;
  if (rxdbJson.size === 0) {
    var rxdbJsonData = await fillFileIfEtagMatches(oneDriveState, rxdbJson.fileId, rxdbJson.etag, {
      replicationIdentifier: randomToken(10)
    });
    replicationIdentifier = ensureNotFalsy(rxdbJsonData.content).replicationIdentifier;
  } else {
    var _rxdbJsonData = await readJsonFileContent(oneDriveState, rxdbJson.fileId);
    replicationIdentifier = ensureNotFalsy(_rxdbJsonData.content).replicationIdentifier;
  }

  // docs folder
  var docsFolderId = await ensureFolderExists(oneDriveState, oneDriveState.folderPath + '/docs');

  // signaling folder
  var signalingFolderId = await ensureFolderExists(oneDriveState, oneDriveState.folderPath + '/signaling');

  // transaction file
  var transactionFile = await createEmptyFile(oneDriveState, rootFolderId, TRANSACTION_FILE_NAME);

  // WAL file
  var walFile = await createEmptyFile(oneDriveState, rootFolderId, WAL_FILE_NAME);
  return {
    rootFolderId,
    docsFolderId,
    signalingFolderId,
    replicationIdentifier,
    rxdbJson,
    transactionFile,
    walFile
  };
}
//# sourceMappingURL=init.js.map