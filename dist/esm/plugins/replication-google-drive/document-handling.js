import { newRxError, newRxFetchError } from "../../rx-error.js";
import { ensureNotFalsy } from "../utils/index.js";
import { applyDriveSpace, insertMultipartFile } from "./google-drive-helper.js";
export { serializeDocAttachments, deserializeDocAttachments } from "../../replication-protocol/helper.js";
var MAX_DRIVE_PAGE_SIZE = 1000;
export async function getDocumentFiles(googleDriveOptions, init, docIds) {
  var fileNames = docIds.map(id => id + '.json');
  var q = fileNames.map(name => "name = '" + name.replace("'", "\\'") + "'").join(' or ');
  q = '(' + q + ') and trashed = false';
  q += ' and \'' + init.docsFolderId + '\' in parents';
  if (docIds.length >= MAX_DRIVE_PAGE_SIZE) {
    throw newRxError('SNH');
  }
  var params = new URLSearchParams({
    q,
    pageSize: MAX_DRIVE_PAGE_SIZE + '',
    fields: "nextPageToken, files(id,name,mimeType,parents,modifiedTime,size)",
    // Shared drives support (safe to include always)
    includeItemsFromAllDrives: "true",
    supportsAllDrives: "true"
  });
  applyDriveSpace(googleDriveOptions, params);
  var url = googleDriveOptions.apiEndpoint + '/drive/v3/files?' + params.toString();
  var res = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken
    }
  });
  if (!res.ok) {
    throw await newRxFetchError(res, {
      ids: docIds
    });
  }
  var data = await res.json();
  return data;
}
export async function batchGetFilesMetadata(googleDriveOptions, fileIds) {
  var boundary = "batch_" + Math.random().toString(16).slice(2);
  var parts = fileIds.map((id, i) => {
    return "--" + boundary + "\r\n" + "Content-Type: application/http\r\n" + ("Content-ID: <item-" + i + ">\r\n\r\n") + ("GET /drive/v3/files/" + encodeURIComponent(id) + "?") + "fields=id,name,mimeType,parents,modifiedTime,size&supportsAllDrives=true HTTP/1.1\r\n\r\n";
  });
  var batchBody = parts.join("") + ("--" + boundary + "--");
  var res = await fetch(googleDriveOptions.apiEndpoint + "/batch/drive/v3", {
    method: "POST",
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken,
      "Content-Type": "multipart/mixed; boundary=" + boundary
    },
    body: batchBody
  });
  if (!res.ok) {
    throw await newRxFetchError(res);
  }

  // multipart/mixed; you can parse it, or just return raw.
  return await res.text();
}
export async function insertDocumentFiles(googleDriveOptions, init, primaryPath, docs) {
  // Run uploads in parallel
  await Promise.all(docs.map(async doc => {
    var id = doc[primaryPath];
    await insertMultipartFile(googleDriveOptions, init.docsFolderId, id + '.json', doc);
  }));
}
export async function updateDocumentFiles(googleDriveOptions, primaryPath, docs, fileMetaByDocId, concurrency = 5) {
  var queue = docs.slice(0);
  var results = {};
  async function worker() {
    while (queue.length) {
      var doc = queue.shift();
      var docId = doc[primaryPath];
      var meta = ensureNotFalsy(fileMetaByDocId[docId]);
      var fileId = meta.fileId;
      var etag = meta.etag;
      var url = googleDriveOptions.apiEndpoint + ("/upload/drive/v2/files/" + encodeURIComponent(fileId)) + "?uploadType=media&supportsAllDrives=true&fields=id";
      var res = await fetch(url, {
        method: "PUT",
        headers: {
          Authorization: "Bearer " + googleDriveOptions.authToken,
          "Content-Type": "application/json; charset=UTF-8",
          "If-Match": etag
        },
        body: JSON.stringify(doc)
      });
      if (res.status === 412) {
        throw newRxError('GDR20', {
          args: {
            docId,
            fileId
          }
        });
      }
      if (!res.ok) {
        throw await newRxFetchError(res, {
          args: {
            docId,
            fileId
          }
        });
      }
      results[docId] = await res.json(); // { id }
    }
  }
  await Promise.all(Array.from({
    length: concurrency
  }, () => worker()));
  return results;
}
export async function fetchDocumentContents(googleDriveOptions, fileIds, concurrency = 5) {
  var byId = {};
  var ordered = new Array(fileIds.length);
  var nextIndex = 0;
  var sleep = ms => new Promise(r => setTimeout(r, ms));
  async function fetchOne(fileId, attempt = 0) {
    var url = googleDriveOptions.apiEndpoint + ("/drive/v3/files/" + encodeURIComponent(fileId) + "?alt=media&supportsAllDrives=true");
    var res = await fetch(url, {
      headers: {
        Authorization: "Bearer " + googleDriveOptions.authToken
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
      byId[fileId] = doc; // if ById allows undefined, remove `as any`
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