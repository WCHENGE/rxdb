"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.initDriveStructure = initDriveStructure;
var _googleDriveHelper = require("./google-drive-helper.js");
var _rxError = require("../../rx-error.js");
var _utilsString = require("../utils/utils-string.js");
var _transaction = require("./transaction.js");
var _index = require("../utils/index.js");
var _upstream = require("./upstream.js");
var NON_ALLOWED_ROOT_FOLDERS = ['/', '', null, false, undefined, 'root'];
async function initDriveStructure(googleDriveOptions) {
  /**
   * The appDataFolder is a dedicated, hidden per-app space.
   * Storing data directly in its root is allowed, so an empty
   * folderPath is valid there. For the regular 'drive' space we
   * must not use the root folder.
   */
  if (googleDriveOptions.space !== 'appDataFolder') {
    NON_ALLOWED_ROOT_FOLDERS.forEach(nonAllowed => {
      if (googleDriveOptions.folderPath === nonAllowed) {
        throw (0, _rxError.newRxError)('GDR1', {
          folderPath: googleDriveOptions.folderPath
        });
      }
    });
  }

  // root folder
  var rootFolderId = await (0, _googleDriveHelper.ensureFolderExists)(googleDriveOptions, googleDriveOptions.folderPath);
  var rootFolderContent = await (0, _googleDriveHelper.readFolder)(googleDriveOptions, googleDriveOptions.folderPath);

  /**
   * Folder but either be empty
   * or already used as a RxDB google-drive sync target.
   */
  var hasRxDBJson = rootFolderContent.find(file => file.name === 'rxdb.json');
  var hasOther = rootFolderContent.find(file => file.name !== 'rxdb.json');
  if (hasOther && !hasRxDBJson) {
    throw (0, _rxError.newRxError)('GDR9', {
      folderPath: googleDriveOptions.folderPath
    });
  }

  /**
   * Create rxdb.json file.
   * This must always be the first step.
   */
  var rxdbJson = await (0, _googleDriveHelper.createEmptyFile)(googleDriveOptions, rootFolderId, 'rxdb.json');
  var replicationIdentifier;
  if (rxdbJson.size === 0) {
    var rxdbJsonData = await (0, _googleDriveHelper.fillFileIfEtagMatches)(googleDriveOptions, rxdbJson.fileId, rxdbJson.etag, {
      replicationIdentifier: (0, _utilsString.randomToken)(10)
    });
    replicationIdentifier = (0, _index.ensureNotFalsy)(rxdbJsonData.content).replicationIdentifier;
  } else {
    var _rxdbJsonData = await (0, _googleDriveHelper.readJsonFileContent)(googleDriveOptions, rxdbJson.fileId);
    replicationIdentifier = (0, _index.ensureNotFalsy)(_rxdbJsonData.content).replicationIdentifier;
  }

  // docs folder
  var docsFolderId = await (0, _googleDriveHelper.ensureFolderExists)(googleDriveOptions, googleDriveOptions.folderPath + '/docs');

  // signaling folder
  var signalingFolderId = await (0, _googleDriveHelper.ensureFolderExists)(googleDriveOptions, googleDriveOptions.folderPath + '/signaling');

  // transaction file
  var transactionFile = await (0, _googleDriveHelper.createEmptyFile)(googleDriveOptions, rootFolderId, _transaction.TRANSACTION_FILE_NAME);

  // WAL file
  var walFile = await (0, _googleDriveHelper.createEmptyFile)(googleDriveOptions, rootFolderId, _upstream.WAL_FILE_NAME);
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