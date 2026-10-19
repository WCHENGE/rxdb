"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.WAL_FILE_NAME = void 0;
exports.fetchConflicts = fetchConflicts;
exports.handleUpstreamBatch = handleUpstreamBatch;
exports.processWalFile = processWalFile;
exports.readWalContent = readWalContent;
exports.writeToWal = writeToWal;
var _rxError = require("../../rx-error.js");
var _helper = require("../../replication-protocol/helper.js");
var _index = require("../utils/index.js");
var _documentHandling = require("./document-handling.js");
var _googleDriveHelper = require("./google-drive-helper.js");
var WAL_FILE_NAME = exports.WAL_FILE_NAME = 'rxdb-wal.json';
async function fetchConflicts(googleDriveOptions, init, primaryPath, writeRows) {
  if (writeRows.length > _googleDriveHelper.DRIVE_MAX_BULK_SIZE) {
    throw (0, _rxError.newRxError)('GDR18', {
      args: {
        DRIVE_MAX_BULK_SIZE: _googleDriveHelper.DRIVE_MAX_BULK_SIZE
      }
    });
  }
  var ids = writeRows.map(row => row.newDocumentState[primaryPath]);
  var filesMeta = await (0, _documentHandling.getDocumentFiles)(googleDriveOptions, init, ids);
  var fileIdByDocId = new Map();
  var fileIds = filesMeta.files.map(f => {
    var fileId = (0, _index.ensureNotFalsy)(f.id);
    var docId = f.name.split('.')[0];
    fileIdByDocId.set(docId, fileId);
    return fileId;
  });
  var contentsByFileId = await (0, _documentHandling.fetchDocumentContents)(googleDriveOptions, fileIds);
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
async function writeToWal(googleDriveOptions, init, writeRows) {
  var walFileId = init.walFile.fileId;
  var metaUrl = googleDriveOptions.apiEndpoint + ("/drive/v2/files/" + encodeURIComponent(walFileId) + "?") + new URLSearchParams({
    fields: "id,fileSize,mimeType,title,etag",
    supportsAllDrives: "true"
  }).toString();
  var metaRes = await fetch(metaUrl, {
    method: "GET",
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken
    }
  });
  if (!metaRes.ok) {
    throw await (0, _rxError.newRxFetchError)(metaRes);
  }
  var meta = await metaRes.json();
  var sizeStr = meta.fileSize ?? "0";
  var sizeNum = Number(sizeStr);
  if (writeRows && (!meta.fileSize || sizeNum > 0)) {
    throw (0, _rxError.newRxError)("GDR19", {
      args: {
        sizeNum,
        walFileId,
        size: meta.size,
        meta,
        writeRows: writeRows?.length
      }
    });
  }
  var etag = (0, _index.ensureNotFalsy)(metaRes.headers.get("etag"), 'etag missing');
  var writeResult = await (0, _googleDriveHelper.fillFileIfEtagMatches)(googleDriveOptions, walFileId, etag, writeRows);
  if (writeResult.status !== 200) {
    throw (0, _rxError.newRxError)("GDR19", {
      args: {
        walFileId,
        meta,
        writeRows: writeRows?.length
      }
    });
  }
}
async function readWalContent(googleDriveOptions, init) {
  var walFileId = init.walFile.fileId;
  var contentUrl = googleDriveOptions.apiEndpoint + ("/drive/v2/files/" + encodeURIComponent(walFileId) + "?alt=media");
  var res = await fetch(contentUrl, {
    method: "GET",
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken
    }
  });
  if (!res.ok) {
    throw await (0, _rxError.newRxFetchError)(res);
  }
  var etag = (0, _index.ensureNotFalsy)(res.headers.get("etag"), "etag missing on WAL read");
  var text = await res.text();

  // If empty or whitespace → no WAL entries
  if (!text || !text.trim()) {
    return {
      etag,
      rows: undefined
    };
  }
  return {
    etag,
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
async function processWalFile(googleDriveOptions, init, primaryPath) {
  var content = await readWalContent(googleDriveOptions, init);
  if (!content.rows) {
    return;
  }
  var docIds = content.rows.map(row => row.newDocumentState[primaryPath]);
  var docFiles = await (0, _documentHandling.getDocumentFiles)(googleDriveOptions, init, docIds);
  var fileMetaByDocId = {};

  /**
   * The Google Drive v3 list API no longer supports "etag" as a field selector,
   * so we fetch the etag for each file individually via the v2 API.
   * Each file requires one additional request to retrieve its current etag.
   * All requests are made in parallel to minimize total latency.
   */
  await Promise.all(docFiles.files.map(async file => {
    var fileId = (0, _index.ensureNotFalsy)(file.id);
    var docId = file.name.split('.')[0];
    var etag = await (0, _googleDriveHelper.getFileEtag)(googleDriveOptions, fileId);
    fileMetaByDocId[docId] = {
      fileId,
      etag
    };
  }));
  var toInsert = [];
  var toUpdate = [];
  content.rows.filter(row => {
    var docId = row.newDocumentState[primaryPath];
    var fileExists = fileMetaByDocId[docId];
    if (!fileExists) {
      toInsert.push(row.newDocumentState);
    } else {
      toUpdate.push(row.newDocumentState);
    }
  });
  await Promise.all([(0, _documentHandling.insertDocumentFiles)(googleDriveOptions, init, primaryPath, toInsert), (0, _documentHandling.updateDocumentFiles)(googleDriveOptions, primaryPath, toUpdate, fileMetaByDocId)]);

  // overwrite wal with emptyness
  await writeToWal(googleDriveOptions, init, undefined);
}
async function handleUpstreamBatch(googleDriveOptions, init, primaryPath, writeRows) {
  var conflictResult = await fetchConflicts(googleDriveOptions, init, primaryPath, writeRows);
  await writeToWal(googleDriveOptions, init, conflictResult.nonConflicts);
  return conflictResult.conflicts;
}
//# sourceMappingURL=upstream.js.map