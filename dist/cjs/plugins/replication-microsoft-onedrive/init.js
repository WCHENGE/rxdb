"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.initDriveStructure = initDriveStructure;
var _microsoftOnedriveHelper = require("./microsoft-onedrive-helper.js");
var _rxError = require("../../rx-error.js");
var _utilsString = require("../utils/utils-string.js");
var _transaction = require("./transaction.js");
var _index = require("../utils/index.js");
var _upstream = require("./upstream.js");
var NON_ALLOWED_ROOT_FOLDERS = ['/', '', null, false, undefined, 'root'];
async function initDriveStructure(oneDriveState) {
  NON_ALLOWED_ROOT_FOLDERS.forEach(nonAllowed => {
    if (oneDriveState.folderPath === nonAllowed) {
      throw (0, _rxError.newRxError)('ODR1', {
        folderPath: oneDriveState.folderPath
      });
    }
  });

  // root folder
  var rootFolderId = await (0, _microsoftOnedriveHelper.ensureFolderExists)(oneDriveState, oneDriveState.folderPath);
  var rootFolderContent = await (0, _microsoftOnedriveHelper.listFilesInFolder)(oneDriveState, rootFolderId);

  /**
   * Folder must either be empty
   * or already used as a RxDB onedrive sync target.
   */
  var hasRxDBJson = rootFolderContent.find(file => file.name === 'rxdb.json');
  var hasOther = rootFolderContent.find(file => file.name !== 'rxdb.json');
  if (hasOther && !hasRxDBJson) {
    throw (0, _rxError.newRxError)('ODR9', {
      folderPath: oneDriveState.folderPath
    });
  }

  /**
   * Create rxdb.json file.
   * This must always be the first step.
   */
  var rxdbJson = await (0, _microsoftOnedriveHelper.createEmptyFile)(oneDriveState, rootFolderId, 'rxdb.json');
  var replicationIdentifier;
  if (rxdbJson.size === 0) {
    var rxdbJsonData = await (0, _microsoftOnedriveHelper.fillFileIfEtagMatches)(oneDriveState, rxdbJson.fileId, rxdbJson.etag, {
      replicationIdentifier: (0, _utilsString.randomToken)(10)
    });
    replicationIdentifier = (0, _index.ensureNotFalsy)(rxdbJsonData.content).replicationIdentifier;
  } else {
    var _rxdbJsonData = await (0, _microsoftOnedriveHelper.readJsonFileContent)(oneDriveState, rxdbJson.fileId);
    replicationIdentifier = (0, _index.ensureNotFalsy)(_rxdbJsonData.content).replicationIdentifier;
  }

  // docs folder
  var docsFolderId = await (0, _microsoftOnedriveHelper.ensureFolderExists)(oneDriveState, oneDriveState.folderPath + '/docs');

  // signaling folder
  var signalingFolderId = await (0, _microsoftOnedriveHelper.ensureFolderExists)(oneDriveState, oneDriveState.folderPath + '/signaling');

  // transaction file
  var transactionFile = await (0, _microsoftOnedriveHelper.createEmptyFile)(oneDriveState, rootFolderId, _transaction.TRANSACTION_FILE_NAME);

  // WAL file
  var walFile = await (0, _microsoftOnedriveHelper.createEmptyFile)(oneDriveState, rootFolderId, _upstream.WAL_FILE_NAME);
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