import { newRxError, newRxFetchError } from "../../rx-error.js";
import { stripAllAttachmentDataForComparison } from "../../replication-protocol/helper.js";
import { deepEqual, ensureNotFalsy } from "../utils/index.js";
import { fetchDocumentContents, getDocumentFiles, insertDocumentFiles, updateDocumentFiles } from "./document-handling.js";
import { fillFileIfEtagMatches, getDriveBaseUrl } from "./microsoft-onedrive-helper.js";
export var WAL_FILE_NAME = 'rxdb-wal.json';

// Batch insert limit in Graph api is often not fully restricted for files unless using batch endpoints, 
// a reasonable size is 100-250 to avoid timeout.
export var DRIVE_MAX_BULK_SIZE = 250;
export async function fetchConflicts(oneDriveState, init, primaryPath, writeRows) {
  if (writeRows.length > DRIVE_MAX_BULK_SIZE) {
    throw newRxError('ODR18', {
      args: {
        DRIVE_MAX_BULK_SIZE
      }
    });
  }
  var ids = writeRows.map(row => row.newDocumentState[primaryPath]);
  var filesMeta = await getDocumentFiles(oneDriveState, init, ids);
  var fileIdByDocId = new Map();
  var fileIds = filesMeta.files.map(f => {
    var fileId = ensureNotFalsy(f.id);
    var docId = f.name.split('.')[0];
    fileIdByDocId.set(docId, fileId);
    return fileId;
  });
  var contentsByFileId = await fetchDocumentContents(oneDriveState, fileIds);
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
      if (!deepEqual(stripAllAttachmentDataForComparison(row.assumedMasterState), stripAllAttachmentDataForComparison(fileContent))) {
        conflicts.push(ensureNotFalsy(fileContent));
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
    throw newRxError('SNH', {
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
export async function writeToWal(oneDriveState, init, writeRows) {
  var walFileId = init.walFile.fileId;
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var metaUrl = baseUrl + "/items/" + encodeURIComponent(walFileId) + "?$select=id,size,eTag";
  var metaRes = await fetch(metaUrl, {
    method: "GET",
    headers: {
      Authorization: "Bearer " + oneDriveState.authToken
    }
  });
  if (!metaRes.ok) {
    throw await newRxFetchError(metaRes);
  }
  var meta = await metaRes.json();
  var sizeNum = meta.size || 0;
  if (writeRows && sizeNum > 0) {
    throw newRxError("ODR19", {
      args: {
        sizeNum,
        walFileId,
        meta,
        writeRows: writeRows?.length
      }
    });
  }
  var etag = ensureNotFalsy(meta.eTag, 'etag missing');
  var writeResult = await fillFileIfEtagMatches(oneDriveState, walFileId, etag, writeRows);
  if (writeResult.status !== 200 && writeResult.status !== 201) {
    throw newRxError("ODR19", {
      args: {
        walFileId,
        meta,
        writeRows: writeRows?.length
      }
    });
  }
}
export async function readWalContent(oneDriveState, init) {
  var walFileId = init.walFile.fileId;
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var contentUrl = baseUrl + "/items/" + encodeURIComponent(walFileId) + "/content";
  var res = await fetch(contentUrl, {
    method: "GET",
    headers: {
      Authorization: "Bearer " + oneDriveState.authToken
    }
  });
  if (!res.ok) {
    throw await newRxFetchError(res);
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
      etag: ensureNotFalsy(etag),
      rows: undefined
    };
  }
  return {
    etag: ensureNotFalsy(etag),
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
export async function processWalFile(oneDriveState, init, primaryPath) {
  var content = await readWalContent(oneDriveState, init);
  if (!content.rows) {
    return;
  }
  var docIds = content.rows.map(row => row.newDocumentState[primaryPath]);
  var docFiles = await getDocumentFiles(oneDriveState, init, docIds);
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
  await Promise.all([insertDocumentFiles(oneDriveState, init, primaryPath, toInsert), updateDocumentFiles(oneDriveState, primaryPath, toUpdate, fileIdByDocId)]);

  // overwrite wal with emptyness
  await writeToWal(oneDriveState, init, undefined);
}
export async function handleUpstreamBatch(oneDriveState, init, primaryPath, writeRows) {
  var conflictResult = await fetchConflicts(oneDriveState, init, primaryPath, writeRows);
  await writeToWal(oneDriveState, init, conflictResult.nonConflicts);
  return conflictResult.conflicts;
}
//# sourceMappingURL=upstream.js.map