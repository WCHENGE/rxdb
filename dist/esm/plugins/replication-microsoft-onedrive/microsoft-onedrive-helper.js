import { newRxError, newRxFetchError } from "../../rx-error.js";
import { ensureNotFalsy } from "../utils/index.js";
/**
 * Returns the base URL for Graph API calls up to the drive root.
 * Examples: 
 *   - "https://graph.microsoft.com/v1.0/me/drive" (personal / default)
 *   - "https://graph.microsoft.com/v1.0/drives/12345" 
 */
export function getDriveBaseUrl(oneDriveState) {
  var endpoint = oneDriveState.apiEndpoint || 'https://graph.microsoft.com/v1.0';
  var driveId = oneDriveState.driveId || 'me/drive';
  return endpoint + "/" + driveId;
}
export async function createFolder(oneDriveState, parentId = 'root', folderName) {
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var url = baseUrl + "/items/" + parentId + "/children";
  var body = {
    name: folderName,
    folder: {},
    '@microsoft.graph.conflictBehavior': 'fail'
  };
  var response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + oneDriveState.authToken,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    if (response.status === 409) {
      // someone else created the same folder, return that one instead.
      var found = await findFolder(oneDriveState, parentId, folderName);
      return ensureNotFalsy(found);
    }
    throw await newRxFetchError(response, {
      folderName,
      parentId
    });
  }
  await response.json();
  var foundFolder = await findFolder(oneDriveState, parentId, folderName);
  return ensureNotFalsy(foundFolder);
}
export async function findFolder(oneDriveState, parentId = 'root', folderName) {
  var items = await listFilesInFolder(oneDriveState, parentId);
  var matchingItems = items.filter(i => i.name === folderName);
  if (matchingItems.length === 0) {
    return undefined;
  }

  // Sort by created time so older folders are picked first
  var item = matchingItems.sort((a, b) => {
    var timeA = a.createdDateTime ? new Date(a.createdDateTime).getTime() : 0;
    var timeB = b.createdDateTime ? new Date(b.createdDateTime).getTime() : 0;
    return timeA - timeB;
  })[0];
  if (!item.folder) {
    throw newRxError('ODR3', {
      folderName,
      args: {
        item
      }
    });
  }
  return item.id;
}
export async function ensureFolderExists(oneDriveState, folderPath) {
  var parts = folderPath.split('/').filter(p => p.length > 0);
  var parentId = 'root';
  for (var part of parts) {
    var newParentId = await findFolder(oneDriveState, parentId, part);
    if (newParentId) {
      parentId = newParentId;
    } else {
      parentId = await createFolder(oneDriveState, parentId, part);
    }
  }
  return parentId;
}
export async function createEmptyFile(oneDriveState, parentId, fileName) {
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var url = baseUrl + "/items/" + parentId + "/children";
  var body = {
    name: fileName,
    file: {},
    '@microsoft.graph.conflictBehavior': 'fail'
  };
  var response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + oneDriveState.authToken,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });
  if (!response.ok && response.status !== 409) {
    throw await newRxFetchError(response, {
      folderName: fileName
    });
  }

  // To make idempotent, we always search for the file after creation
  // and take the oldest one in case there are duplicates.
  // This ensures that when two instances concurrently create the same filename,
  // they both converge on the same file afterwards.
  var items = await listFilesInFolder(oneDriveState, parentId);
  var matchingFiles = items.filter(i => i.name === fileName);
  var file = ensureNotFalsy(matchingFiles.sort((a, b) => (a.createdDateTime ? new Date(a.createdDateTime).getTime() : 0) - (b.createdDateTime ? new Date(b.createdDateTime).getTime() : 0))[0]);
  return {
    status: response.status,
    etag: ensureNotFalsy(file.eTag),
    createdTime: file.lastModifiedDateTime,
    fileId: ensureNotFalsy(file.id),
    size: file.size ? file.size : 0
  };
}
export async function fillFileIfEtagMatches(oneDriveState, fileId, etag, jsonContent) {
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var url = baseUrl + "/items/" + encodeURIComponent(fileId) + "/content";
  var writeContent = typeof jsonContent !== 'undefined' ? JSON.stringify(jsonContent) : '';
  var res = await fetch(url, {
    method: "PUT",
    headers: {
      Authorization: "Bearer " + oneDriveState.authToken,
      "Content-Type": "application/json; charset=utf-8",
      "If-Match": etag
    },
    body: writeContent
  });
  if (res.status === 412) {
    return readJsonFileContent(oneDriveState, fileId).then(r => {
      return {
        content: r.content,
        etag: r.etag,
        status: res.status
      };
    });
  }
  if (res.status !== 200 && res.status !== 201) {
    throw await newRxFetchError(res);
  }
  var data = await res.json();
  return {
    content: jsonContent,
    etag: ensureNotFalsy(data.eTag),
    status: res.status
  };
}
export async function deleteIfEtagMatches(oneDriveState, fileId, etag) {
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var url = baseUrl + "/items/" + encodeURIComponent(fileId);
  var res = await fetch(url, {
    method: "DELETE",
    headers: {
      Authorization: "Bearer " + oneDriveState.authToken,
      // Not all item deletions in Microsoft Graph honor If-Match, but we send it.
      // As per KI, we might have to relax it for folders, but this is used for files.
      "If-Match": etag
    }
  });
  if (!res.ok) {
    throw await newRxFetchError(res, {
      args: {
        etag,
        fileId
      }
    });
  }
}
export async function deleteFile(oneDriveState, fileId) {
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var url = baseUrl + "/items/" + encodeURIComponent(fileId);
  var res = await fetch(url, {
    method: "DELETE",
    headers: {
      Authorization: "Bearer " + oneDriveState.authToken
    }
  });
  if (!res.ok) {
    throw await newRxFetchError(res, {
      args: {
        fileId
      }
    });
  }
}
export async function readJsonFileContent(oneDriveState, fileId) {
  var baseUrl = getDriveBaseUrl(oneDriveState);

  // Fetch metadata FIRST to reliably get the ETag, preventing race conditions
  // where old content is fetched via /content and arbitrarily paired with a newer ETag fallback.
  var metaResponse = await fetch(baseUrl + "/items/" + encodeURIComponent(fileId) + "?$select=id,eTag,@microsoft.graph.downloadUrl", {
    headers: {
      Authorization: "Bearer " + oneDriveState.authToken
    }
  });
  if (!metaResponse.ok) {
    if (metaResponse.status === 404) {
      return {
        etag: '',
        content: undefined
      };
    }
    throw await newRxFetchError(metaResponse, {
      args: {
        fileId
      }
    });
  }
  var meta = await metaResponse.json();
  var etag = ensureNotFalsy(meta.eTag, 'eTag missing from metadata');
  var contentText = '';

  // Some mock servers or endpoints may omit the downloadUrl. Fallback to /content if necessary.
  var downloadUrl = meta['@microsoft.graph.downloadUrl'] || baseUrl + "/items/" + encodeURIComponent(fileId) + "/content";
  var contentRes = await fetch(downloadUrl, {
    method: "GET",
    headers: {
      Authorization: "Bearer " + oneDriveState.authToken
    }
  });
  if (contentRes.status === 200 || contentRes.status === 201) {
    contentText = await contentRes.text();
  } else if (contentRes.status !== 404) {
    throw await newRxFetchError(contentRes, {
      args: {
        fileId,
        downloadUrl
      }
    });
  }
  var content = contentText.length > 0 ? JSON.parse(contentText) : undefined;
  return {
    etag,
    content: content
  };
}
export async function listFilesInFolder(oneDriveState, folderId) {
  var baseUrl = getDriveBaseUrl(oneDriveState);
  var url = baseUrl + "/items/" + folderId + "/children?$select=id,name,eTag,createdDateTime,lastModifiedDateTime,size,file,folder";
  var items = [];
  while (url) {
    var res = await fetch(url, {
      headers: {
        Authorization: "Bearer " + oneDriveState.authToken
      }
    });
    if (!res.ok) {
      var text = await res.text().catch(() => "");
      throw new Error("files.list failed: " + res.status + " " + text);
    }
    var data = await res.json();
    var batch = data.value || [];
    items.push(...batch);
    url = data['@odata.nextLink'];
  }
  return items;
}
//# sourceMappingURL=microsoft-onedrive-helper.js.map