"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.WAL_FILE_NAME = exports.DRIVE_MAX_BULK_SIZE = void 0;
exports.fetchConflicts = fetchConflicts;
exports.handleUpstreamBatch = handleUpstreamBatch;
exports.processWalFile = processWalFile;
exports.readWalContent = readWalContent;
exports.writeToWal = writeToWal;
var _rxError = require("../../rx-error.js");
var _helper = require("../../replication-protocol/helper.js");
var _index = require("../utils/index.js");
var _documentHandling = require("./document-handling.js");
var _microsoftOnedriveHelper = require("./microsoft-onedrive-helper.js");
var WAL_FILE_NAME = exports.WAL_FILE_NAME = 'rxdb-wal.json';

// Batch insert limit in Graph api is often not fully restricted for files unless using batch endpoints, 
// a reasonable size is 100-250 to avoid timeout.
var DRIVE_MAX_BULK_SIZE = exports.DRIVE_MAX_BULK_SIZE = 250;
async function fetchConflicts(oneDriveState, init, primaryPath, writeRows) {
  if (writeRows.length > DRIVE_MAX_BULK_SIZE) {
    throw (0, _rxError.newRxError)('ODR18', {
      args: {
        DRIVE_MAX_BULK_SIZE
      }
    });
  }
  var ids = writeRows.map(row => row.newDocumentState[primaryPath]);
  var filesMeta = await (0, _documentHandling.getDocumentFiles)(oneDriveState, init, ids);
  var fileIdByDocId = new Map();
  var fileIds = filesMeta.files.map(f => {
    var fileId = (0, _index.ensureNotFalsy)(f.id);
    var docId = f.name.split('.')[0];
    fileIdByDocId.set(docId, fileId);
    return fileId;
  });
  var contentsByFileId = await (0, _documentHandling.fetchDocumentContents)(oneDriveState, fileIds);
  var conflicts = [];
  var nonConflicts = [];
  writeRows.forEach(row => {
    var docId = row.newDocumentState[primaryPath];
    var fileContent;
    var fileId = fileIdByDocId.get(docId);
    if (fileId) {
      fileContent = contentsByFileId.byId[fileId];
    }
    if (row.assumedMasterState) {
      if (!(0, _index.deepEqual)((0, _helper.stripAllAttachmentDataForComparison)(row.assumedMasterState), (0, _helper.stripAllAttachmentDataForComparison)(fileContent))) {
        conflicts.push((0, _index.ensureNotFalsy)(fileContent));
      } else {
        nonConflicts.push(row);
      }
    } else if (fileContent) {
      conflicts.push(fileContent);
    } else {
      nonConflicts.push(row);
    }
  });
  if (nonConflicts.length + conflicts.length !== writeRows.length) {
    throw (0, _rxError.newRxError)('SNH', {
      pushRows: writeRows,
      args: {
        nonConflicts,
        conflicts,
        contentsByFileId: contentsByFileId.byId
      }
    });
  }
  return {
    conflicts,
    nonConflicts
  };
}
async function writeToWal(oneDriveState, init, writeRows) {
  var walFileId = init.walFile.fileId;
  var baseUrl = (0, _microsoftOnedriveHelper.getDriveBaseUrl)(oneDriveState);
  var metaUrl = baseUrl + "/items/" + encodeURIComponent(walFileId) + "?$select=id,size,eTag";
  var metaRes = await fetch(metaUrl, {
    method: "GET",
    headers: {
      Authorization: "Bearer " + oneDriveState.authToken
    }
  });
  if (!metaRes.ok) {
    throw await (0, _rxError.newRxFetchError)(metaRes);
  }
  var meta = await metaRes.json();
  var sizeNum = meta.size || 0;
  if (writeRows && sizeNum > 0) {
    throw (0, _rxError.newRxError)("ODR19", {
      args: {
        sizeNum,
        walFileId,
        meta,
        writeRows: writeRows?.length
      }
    });
  }
  var etag = (0, _index.ensureNotFalsy)(meta.eTag, 'etag missing');
  var writeResult = await (0, _microsoftOnedriveHelper.fillFileIfEtagMatches)(oneDriveState, walFileId, etag, writeRows);
  if (writeResult.status !== 200 && writeResult.status !== 201) {
    throw (0, _rxError.newRxError)("ODR19", {
      args: {
        walFileId,
        meta,
        writeRows: writeRows?.length
      }
    });
  }
}
async function readWalContent(oneDriveState, init) {
  var walFileId = init.walFile.fileId;
  var baseUrl = (0, _microsoftOnedriveHelper.getDriveBaseUrl)(oneDriveState);
  var contentUrl = baseUrl + "/items/" + encodeURIComponent(walFileId) + "/content";
  var res = await fetch(contentUrl, {
    method: "GET",
    headers: {
      Authorization: "Bearer " + oneDriveState.authToken
    }
  });
  if (!res.ok) {
    throw await (0, _rxError.newRxFetchError)(res);
  }
  var etag = res.headers.get("etag") || res.headers.get("ETag");
  if (!etag) {
    var metaRes = await fetch(baseUrl + "/items/" + encodeURIComponent(walFileId) + "?$select=eTag", {
      headers: {
        Authorization: "Bearer " + oneDriveState.authToken
      }
    });
    var meta = await metaRes.json();
    etag = meta.eTag;
  }
  var text = await res.text();
  if (!text || !text.trim()) {
    return {
      etag: (0, _index.ensureNotFalsy)(etag),
      rows: undefined
    };
  }
  return {
    etag: (0, _index.ensureNotFalsy)(etag),
    rows: JSON.parse(text)
  };
}

/**
 * Here we read the WAL file content
 * and sort the content into the actual
 * document files.
 * Notice that when the JavaScript process
 * exists at any point here, we need to have
 * a recoverable state on the next run. So this
 * must be idempotent.
 */
async function processWalFile(oneDriveState, init, primaryPath) {
  var content = await readWalContent(oneDriveState, init);
  if (!content.rows) {
    return;
  }
  var docIds = content.rows.map(row => row.newDocumentState[primaryPath]);
  var docFiles = await (0, _documentHandling.getDocumentFiles)(oneDriveState, init, docIds);
  var fileIdByDocId = {};
  docFiles.files.forEach(file => {
    var docId = file.name.split('.')[0];
    fileIdByDocId[docId] = file.id;
  });
  var toInsert = [];
  var toUpdate = [];
  content.rows.filter(row => {
    var docId = row.newDocumentState[primaryPath];
    var fileExists = fileIdByDocId[docId];
    if (!fileExists) {
      toInsert.push(row.newDocumentState);
    } else {
      toUpdate.push(row.newDocumentState);
    }
  });
  await Promise.all([(0, _documentHandling.insertDocumentFiles)(oneDriveState, init, primaryPath, toInsert), (0, _documentHandling.updateDocumentFiles)(oneDriveState, primaryPath, toUpdate, fileIdByDocId)]);

  // overwrite wal with emptyness
  await writeToWal(oneDriveState, init, undefined);
}
async function handleUpstreamBatch(oneDriveState, init, primaryPath, writeRows) {
  var conflictResult = await fetchConflicts(oneDriveState, init, primaryPath, writeRows);
  await writeToWal(oneDriveState, init, conflictResult.nonConflicts);
  return conflictResult.conflicts;
}
//# sourceMappingURL=upstream.js.map