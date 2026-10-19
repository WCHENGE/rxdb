import { newRxError, newRxFetchError } from "../../rx-error.js";
import { stripAllAttachmentDataForComparison } from "../../replication-protocol/helper.js";
import { deepEqual, ensureNotFalsy } from "../utils/index.js";
import { fetchDocumentContents, getDocumentFiles, insertDocumentFiles, updateDocumentFiles } from "./document-handling.js";
import { DRIVE_MAX_BULK_SIZE, fillFileIfEtagMatches, getFileEtag } from "./google-drive-helper.js";
export var WAL_FILE_NAME = 'rxdb-wal.json';
export async function fetchConflicts(googleDriveOptions, init, primaryPath, writeRows) {
  if (writeRows.length > DRIVE_MAX_BULK_SIZE) {
    throw newRxError('GDR18', {
      args: {
        DRIVE_MAX_BULK_SIZE
      }
    });
  }
  var ids = writeRows.map(row => row.newDocumentState[primaryPath]);
  var filesMeta = await getDocumentFiles(googleDriveOptions, init, ids);
  var fileIdByDocId = new Map();
  var fileIds = filesMeta.files.map(f => {
    var fileId = ensureNotFalsy(f.id);
    var docId = f.name.split('.')[0];
    fileIdByDocId.set(docId, fileId);
    return fileId;
  });
  var contentsByFileId = await fetchDocumentContents(googleDriveOptions, fileIds);
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
export async function writeToWal(googleDriveOptions, init, writeRows) {
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
    throw await newRxFetchError(metaRes);
  }
  var meta = await metaRes.json();
  var sizeStr = meta.fileSize ?? "0";
  var sizeNum = Number(sizeStr);
  if (writeRows && (!meta.fileSize || sizeNum > 0)) {
    throw newRxError("GDR19", {
      args: {
        sizeNum,
        walFileId,
        size: meta.size,
        meta,
        writeRows: writeRows?.length
      }
    });
  }
  var etag = ensureNotFalsy(metaRes.headers.get("etag"), 'etag missing');
  var writeResult = await fillFileIfEtagMatches(googleDriveOptions, walFileId, etag, writeRows);
  if (writeResult.status !== 200) {
    throw newRxError("GDR19", {
      args: {
        walFileId,
        meta,
        writeRows: writeRows?.length
      }
    });
  }
}
export async function readWalContent(googleDriveOptions, init) {
  var walFileId = init.walFile.fileId;
  var contentUrl = googleDriveOptions.apiEndpoint + ("/drive/v2/files/" + encodeURIComponent(walFileId) + "?alt=media");
  var res = await fetch(contentUrl, {
    method: "GET",
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken
    }
  });
  if (!res.ok) {
    throw await newRxFetchError(res);
  }
  var etag = ensureNotFalsy(res.headers.get("etag"), "etag missing on WAL read");
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
export async function processWalFile(googleDriveOptions, init, primaryPath) {
  var content = await readWalContent(googleDriveOptions, init);
  if (!content.rows) {
    return;
  }
  var docIds = content.rows.map(row => row.newDocumentState[primaryPath]);
  var docFiles = await getDocumentFiles(googleDriveOptions, init, docIds);
  var fileMetaByDocId = {};

  /**
   * The Google Drive v3 list API no longer supports "etag" as a field selector,
   * so we fetch the etag for each file individually via the v2 API.
   * Each file requires one additional request to retrieve its current etag.
   * All requests are made in parallel to minimize total latency.
   */
  await Promise.all(docFiles.files.map(async file => {
    var fileId = ensureNotFalsy(file.id);
    var docId = file.name.split('.')[0];
    var etag = await getFileEtag(googleDriveOptions, fileId);
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
  await Promise.all([insertDocumentFiles(googleDriveOptions, init, primaryPath, toInsert), updateDocumentFiles(googleDriveOptions, primaryPath, toUpdate, fileMetaByDocId)]);

  // overwrite wal with emptyness
  await writeToWal(googleDriveOptions, init, undefined);
}
export async function handleUpstreamBatch(googleDriveOptions, init, primaryPath, writeRows) {
  var conflictResult = await fetchConflicts(googleDriveOptions, init, primaryPath, writeRows);
  await writeToWal(googleDriveOptions, init, conflictResult.nonConflicts);
  return conflictResult.conflicts;
}
//# sourceMappingURL=upstream.js.map