import { newRxError, newRxFetchError } from "../../rx-error.js";
import { ensureNotFalsy } from "../utils/index.js";
import { getDriveBaseUrl } from "./microsoft-onedrive-helper.js";
export { serializeDocAttachments, deserializeDocAttachments } from "../../replication-protocol/helper.js";
var MAX_DRIVE_PAGE_SIZE = 999;
export async function getDocumentFiles(oneDriveState, init, docIds) {
  if (docIds.length >= MAX_DRIVE_PAGE_SIZE) {
    throw newRxError('SNH');
  }
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var files = [];

  // Microsoft Graph doesn't support complex "OR" in search easily, but we can do a `$filter` 
  // or just fetch by folder children and then filter locally if amount is small, 
  // or use batch requests. For simplicity we use search where name in (x, y, z) is not supported,
  // so we can use a $batch request to get all files by id/name from the docs folder.

  // An alternative is `$batch` requests to look up by path. 
  // Graph batch has a limit of 20 requests per batch.

  var BATCH_SIZE = 20;
  var _loop = async function () {
    var chunk = docIds.slice(i, i + BATCH_SIZE);
    var driveBaseUrl = getDriveBaseUrl(oneDriveState);
    var apiEndpoint = oneDriveState.apiEndpoint || 'https://graph.microsoft.com/v1.0';
    var pathPrefix = driveBaseUrl.substring(apiEndpoint.length);
    if (!pathPrefix.startsWith('/')) {
      pathPrefix = '/' + pathPrefix;
    }
    var batchRequests = chunk.map((id, index) => {
      return {
        id: index.toString(),
        method: "GET",
        url: pathPrefix + "/items/" + init.docsFolderId + ":/" + encodeURIComponent(id + '.json') + "?$select=id,name,eTag,createdDateTime,lastModifiedDateTime,size,file"
      };
    });

    // The URL for batch is at the domain root, not the drive root.
    // E.g. https://graph.microsoft.com/v1.0/$batch
    var batchUrl = (oneDriveState.apiEndpoint || 'https://graph.microsoft.com/v1.0') + '/$batch';
    var batchBody = JSON.stringify({
      requests: batchRequests
    });
    var res = await fetch(batchUrl, {
      method: 'POST',
      headers: {
        Authorization: "Bearer " + oneDriveState.authToken,
        'Content-Type': 'application/json'
      },
      body: batchBody
    });
    if (!res.ok) {
      throw await newRxFetchError(res, {
        ids: docIds
      });
    }
    var data = await res.json();
    var responses = data.responses || [];
    for (var response of responses) {
      if (response.status === 200) {
        files.push(response.body);
      } else if (response.status !== 404) {
        // If it's a 404, the file doesn't exist, which is fine for getDocumentFiles.
        // It just returns the files that ARE found.
        // If it's another error, we could throw, but skipping is safer to match Google Drive behavior if one fails.
      }
    }
  };
  for (var i = 0; i < docIds.length; i += BATCH_SIZE) {
    await _loop();
  }
  return {
    files
  };
}
export async function insertDocumentFiles(oneDriveState, init, primaryPath, docs) {
  var baseUrl = getDriveBaseUrl(oneDriveState);

  // Run uploads in parallel
  await Promise.all(docs.map(async doc => {
    var id = doc[primaryPath];
    var fileName = id + '.json';
    var url = baseUrl + "/items/" + init.docsFolderId + ":/" + encodeURIComponent(fileName) + ":/content";
    var res = await fetch(url, {
      method: 'PUT',
      headers: {
        Authorization: "Bearer " + oneDriveState.authToken,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(doc)
    });
    if (!res.ok) {
      throw await newRxFetchError(res);
    }
  }));
}
export async function updateDocumentFiles(oneDriveState, primaryPath, docs, fileIdByDocId, concurrency = 5) {
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var queue = docs.slice(0);
  var results = {};
  async function worker() {
    var doc = queue.shift();
    while (doc) {
      var docId = doc[primaryPath];
      var fileId = ensureNotFalsy(fileIdByDocId[docId]);
      var url = baseUrl + "/items/" + encodeURIComponent(fileId) + "/content";
      var res = await fetch(url, {
        method: "PUT",
        headers: {
          Authorization: "Bearer " + oneDriveState.authToken,
          "Content-Type": "application/json; charset=UTF-8"
        },
        body: JSON.stringify(doc)
      });
      if (!res.ok) {
        throw await newRxFetchError(res, {
          args: {
            docId,
            fileId
          }
        });
      }
      var resData = await res.json();
      results[docId] = {
        id: resData.id
      };
      doc = queue.shift();
    }
  }
  await Promise.all(Array.from({
    length: concurrency
  }, () => worker()));
  return results;
}
export async function fetchDocumentContents(oneDriveState, fileIds, concurrency = 5) {
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var byId = {};
  var ordered = new Array(fileIds.length);
  var nextIndex = 0;
  var sleep = ms => new Promise(r => setTimeout(r, ms));
  async function fetchOne(fileId, attempt = 0) {
    var url = baseUrl + "/items/" + encodeURIComponent(fileId) + "/content";
    var res = await fetch(url, {
      headers: {
        Authorization: "Bearer " + oneDriveState.authToken
      }
    });
    if ([429, 500, 502, 503, 504].includes(res.status) && attempt < 4) {
      var backoffMs = 250 * Math.pow(2, attempt) + Math.floor(Math.random() * 200);
      await sleep(backoffMs);
      return fetchOne(fileId, attempt + 1);
    }
    if (!res.ok) throw await newRxFetchError(res, {
      args: {
        fileId
      }
    });
    var text = await res.text();
    return text ? JSON.parse(text) : undefined;
  }
  async function worker() {
    while (true) {
      var i = nextIndex++;
      if (i >= fileIds.length) return;
      var fileId = fileIds[i];
      var doc = await fetchOne(fileId);
      ordered[i] = ensureNotFalsy(doc);
      byId[fileId] = doc;
    }
  }
  await Promise.all(Array.from({
    length: concurrency
  }, () => worker()));
  return {
    byId,
    ordered
  };
}
//# sourceMappingURL=document-handling.js.map