"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.fetchChanges = fetchChanges;
exports.fetchChangesFiles = fetchChangesFiles;
var _rxError = require("../../rx-error.js");
var _index = require("../utils/index.js");
var _documentHandling = require("./document-handling.js");
var _microsoftOnedriveHelper = require("./microsoft-onedrive-helper.js");
async function fetchChanges(oneDriveState, init, checkpoint, batchSize = 10) {
  var filesResult = await fetchChangesFiles(oneDriveState, init, checkpoint, batchSize);
  var contents = await (0, _documentHandling.fetchDocumentContents)(oneDriveState, filesResult.files.map(file => file.id));
  return {
    checkpoint: filesResult.checkpoint,
    documents: contents.ordered
  };
}
async function fetchChangesFiles(oneDriveState, init, checkpoint, batchSize = 10) {
  var baseUrl = (0, _microsoftOnedriveHelper.getDriveBaseUrl)(oneDriveState);

  // Intentionally overfetch in case multiple docs have the same lastModifiedDateTime.
  var OVERFETCH_AMOUNT = 6;
  var url = baseUrl + "/items/" + init.docsFolderId + "/children?";
  var params = new URLSearchParams();
  params.set('$top', (batchSize + OVERFETCH_AMOUNT).toString());
  params.set('$select', 'id,name,eTag,lastModifiedDateTime,size,file');
  params.set('$orderby', 'lastModifiedDateTime asc,name asc');
  if (checkpoint) {
    // Warning: MS Graph $filter with string timestamps 
    params.set('$filter', "lastModifiedDateTime ge '" + checkpoint.lastModifiedDateTime + "'");
  }
  url += params.toString();
  var res = await fetch(url, {
    headers: {
      Authorization: "Bearer " + oneDriveState.authToken
    }
  });
  if (!res.ok) {
    throw await (0, _rxError.newRxFetchError)(res);
  }
  var data = await res.json();
  var files = data.value || [];
  if (checkpoint) {
    files = files.filter(file => {
      if (file.lastModifiedDateTime === checkpoint.lastModifiedDateTime) {
        // If it's the exact same time, check if we already processed it
        var docId = file.name.split('.')[0];
        return !checkpoint.docIdsWithSameModifiedTime?.includes(docId);
      }
      return true;
    });
  }
  files = files.slice(0, batchSize);
  var first = files[0];
  var newCheckpoint = checkpoint;
  var last = (0, _index.lastOfArray)(files);
  if (last) {
    var lastModified = (0, _index.ensureNotFalsy)(last.lastModifiedDateTime);
    var docIdsWithSameModifiedTime = files.filter(file => file.lastModifiedDateTime === lastModified).map(file => file.name.split('.')[0]);
    if (checkpoint && first && first.lastModifiedDateTime === checkpoint.lastModifiedDateTime) {
      var oldIds = checkpoint.docIdsWithSameModifiedTime || [];
      docIdsWithSameModifiedTime = docIdsWithSameModifiedTime.concat(oldIds);
    }
    newCheckpoint = {
      docIdsWithSameModifiedTime,
      lastModifiedDateTime: lastModified
    };
    // using 'any' to dynamically assign docIdsWithSameModifiedTime, 
    // as doing it on the type strictly might conflict with standard Checkpoint usage in RxDB.
  }
  return {
    checkpoint: newCheckpoint,
    files
  };
}
//# sourceMappingURL=downstream.js.map