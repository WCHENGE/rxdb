import { newRxFetchError } from "../../rx-error.js";
import { ensureNotFalsy, lastOfArray } from "../utils/index.js";
import { fetchDocumentContents } from "./document-handling.js";
import { applyDriveSpace } from "./google-drive-helper.js";
export async function fetchChanges(googleDriveOptions, init, checkpoint, batchSize = 10) {
  var filesResult = await fetchChangesFiles(googleDriveOptions, init, checkpoint, batchSize);
  var contents = await fetchDocumentContents(googleDriveOptions, filesResult.files.map(file => file.id));
  return {
    checkpoint: filesResult.checkpoint,
    documents: contents.ordered
  };
}
export async function fetchChangesFiles(googleDriveOptions, init, checkpoint, batchSize = 10) {
  var queryParts = ["'" + init.docsFolderId + "' in parents", "and trashed = false"];
  if (checkpoint) {
    queryParts.push("and modifiedTime >= '" + checkpoint.modifiedTime + "'");
  }

  /**
   * Intentionally overfetch in case
   * multiple docs have the same modifiedTime.
   * We later have to strip the additional ones.
   */
  var OVERFETCH_AMOUNT = 6;
  var params = new URLSearchParams({
    q: queryParts.join(' '),
    pageSize: batchSize + OVERFETCH_AMOUNT + '',
    orderBy: "modifiedTime asc,name asc",
    fields: "files(id,name,mimeType,parents,modifiedTime,size)",
    supportsAllDrives: "true",
    includeItemsFromAllDrives: "true"
  });
  applyDriveSpace(googleDriveOptions, params);
  var url = googleDriveOptions.apiEndpoint + "/drive/v3/files?" + params.toString();
  var res = await fetch(url, {
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken
    }
  });
  if (!res.ok) {
    throw await newRxFetchError(res);
  }
  var data = await res.json();
  var files = data.files;
  if (checkpoint) {
    files = files.filter(file => !(file.modifiedTime === checkpoint.modifiedTime && checkpoint.docIdsWithSameModifiedTime.includes(file.name)));
  }
  files = files.slice(0, batchSize);
  var first = files[0];
  var newCheckpoint = checkpoint;
  var last = lastOfArray(files);
  if (last) {
    var lastModified = ensureNotFalsy(last.modifiedTime);
    var docIdsWithSameModifiedTime = files.filter(file => file.modifiedTime === lastModified).map(file => file.name);
    if (checkpoint && first.modifiedTime === checkpoint.modifiedTime) {
      docIdsWithSameModifiedTime = docIdsWithSameModifiedTime.concat(checkpoint.docIdsWithSameModifiedTime);
    }
    newCheckpoint = {
      docIdsWithSameModifiedTime,
      modifiedTime: lastModified
    };
  }
  return {
    checkpoint: newCheckpoint,
    files
  };
}
//# sourceMappingURL=downstream.js.map