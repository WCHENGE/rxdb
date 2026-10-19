import { ensureFolderExists, readFolder, createEmptyFile, fillFileIfEtagMatches, readJsonFileContent } from "./google-drive-helper.js";
import { newRxError } from "../../rx-error.js";
import { randomToken } from "../utils/utils-string.js";
import { TRANSACTION_FILE_NAME } from "./transaction.js";
import { ensureNotFalsy } from "../utils/index.js";
import { WAL_FILE_NAME } from "./upstream.js";
var NON_ALLOWED_ROOT_FOLDERS = ['/', '', null, false, undefined, 'root'];
export async function initDriveStructure(googleDriveOptions) {
  /**
   * The appDataFolder is a dedicated, hidden per-app space.
   * Storing data directly in its root is allowed, so an empty
   * folderPath is valid there. For the regular 'drive' space we
   * must not use the root folder.
   */
  if (googleDriveOptions.space !== 'appDataFolder') {
    NON_ALLOWED_ROOT_FOLDERS.forEach(nonAllowed => {
      if (googleDriveOptions.folderPath === nonAllowed) {
        throw newRxError('GDR1', {
          folderPath: googleDriveOptions.folderPath
        });
      }
    });
  }

  // root folder
  var rootFolderId = await ensureFolderExists(googleDriveOptions, googleDriveOptions.folderPath);
  var rootFolderContent = await readFolder(googleDriveOptions, googleDriveOptions.folderPath);

  /**
   * Folder but either be empty
   * or already used as a RxDB google-drive sync target.
   */
  var hasRxDBJson = rootFolderContent.find(file => file.name === 'rxdb.json');
  var hasOther = rootFolderContent.find(file => file.name !== 'rxdb.json');
  if (hasOther && !hasRxDBJson) {
    throw newRxError('GDR9', {
      folderPath: googleDriveOptions.folderPath
    });
  }

  /**
   * Create rxdb.json file.
   * This must always be the first step.
   */
  var rxdbJson = await createEmptyFile(googleDriveOptions, rootFolderId, 'rxdb.json');
  var replicationIdentifier;
  if (rxdbJson.size === 0) {
    var rxdbJsonData = await fillFileIfEtagMatches(googleDriveOptions, rxdbJson.fileId, rxdbJson.etag, {
      replicationIdentifier: randomToken(10)
    });
    replicationIdentifier = ensureNotFalsy(rxdbJsonData.content).replicationIdentifier;
  } else {
    var _rxdbJsonData = await readJsonFileContent(googleDriveOptions, rxdbJson.fileId);
    replicationIdentifier = ensureNotFalsy(_rxdbJsonData.content).replicationIdentifier;
  }

  // docs folder
  var docsFolderId = await ensureFolderExists(googleDriveOptions, googleDriveOptions.folderPath + '/docs');

  // signaling folder
  var signalingFolderId = await ensureFolderExists(googleDriveOptions, googleDriveOptions.folderPath + '/signaling');

  // transaction file
  var transactionFile = await createEmptyFile(googleDriveOptions, rootFolderId, TRANSACTION_FILE_NAME);

  // WAL file
  var walFile = await createEmptyFile(googleDriveOptions, rootFolderId, WAL_FILE_NAME);
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