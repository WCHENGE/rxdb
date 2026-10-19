"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.FOLDER_MIME_TYPE = exports.DRIVE_MAX_PAGE_SIZE = exports.DRIVE_MAX_BULK_SIZE = exports.DRIVE_API_VERSION = void 0;
exports.applyDriveSpace = applyDriveSpace;
exports.createEmptyFile = createEmptyFile;
exports.createFolder = createFolder;
exports.createMultipartBody = createMultipartBody;
exports.deleteFile = deleteFile;
exports.deleteIfEtagMatches = deleteIfEtagMatches;
exports.driveRootParentId = driveRootParentId;
exports.driveSpaceQuerySuffix = driveSpaceQuerySuffix;
exports.ensureFolderExists = ensureFolderExists;
exports.fillFileIfEtagMatches = fillFileIfEtagMatches;
exports.findFolder = findFolder;
exports.getFileEtag = getFileEtag;
exports.insertMultipartFile = insertMultipartFile;
exports.listFilesInFolder = listFilesInFolder;
exports.readFolder = readFolder;
exports.readJsonFileContent = readJsonFileContent;
var _rxError = require("../../rx-error.js");
var _index = require("../utils/index.js");
var DRIVE_API_VERSION = exports.DRIVE_API_VERSION = 'v3';
var DRIVE_MAX_PAGE_SIZE = exports.DRIVE_MAX_PAGE_SIZE = 1000;
var DRIVE_MAX_BULK_SIZE = exports.DRIVE_MAX_BULK_SIZE = DRIVE_MAX_PAGE_SIZE / 4;
var FOLDER_MIME_TYPE = exports.FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder';

/**
 * The id of the top level parent for the configured space.
 * For 'appDataFolder' this is the literal 'appDataFolder' which Google Drive
 * accepts as a parent id and in queries. For 'drive' it is the regular 'root'.
 */
function driveRootParentId(googleDriveOptions) {
  return googleDriveOptions.space === 'appDataFolder' ? 'appDataFolder' : 'root';
}

/**
 * files.list only returns appDataFolder contents when spaces=appDataFolder
 * is set, even when querying by an explicit parent id.
 */
function applyDriveSpace(googleDriveOptions, params) {
  if (googleDriveOptions.space === 'appDataFolder') {
    params.set('spaces', 'appDataFolder');
  }
}

/**
 * Same as applyDriveSpace() but for URLs that are built via string concatenation.
 * Returns an empty string for the default 'drive' space.
 */
function driveSpaceQuerySuffix(googleDriveOptions) {
  return googleDriveOptions.space === 'appDataFolder' ? '&spaces=appDataFolder' : '';
}
async function createFolder(googleDriveOptions, parentId = 'root', folderName) {
  var url = googleDriveOptions.apiEndpoint + '/drive/v3/files?fields=id,name,mimeType,trashed';
  var body = {
    name: folderName,
    mimeType: FOLDER_MIME_TYPE,
    parents: [parentId]
  };
  var response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + googleDriveOptions.authToken,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });
  if (!response.ok) {
    var errorText = await response.text();
    if (response.status == 409) {
      // someone else created the same folder, return that one instead.
      var found = await findFolder(googleDriveOptions, parentId, folderName);
      return (0, _index.ensureNotFalsy)(found);
    }
    throw await (0, _rxError.newRxFetchError)(response, {
      folderName,
      parentId
    });
  }
  await response.json();

  /**
   * To make the function idempotent, we do not use the id from the creation-response.
   * Instead after creating the folder, we search for it again so that in case
   * some other instance created the same folder, we use the oldest one always.
   */
  var foundFolder = await findFolder(googleDriveOptions, parentId, folderName);
  return (0, _index.ensureNotFalsy)(foundFolder);
}
async function findFolder(googleDriveOptions, parentId = 'root', folderName) {
  var query = "name = '" + folderName + "' and '" + parentId + "' in parents and trashed = false and mimeType = '" + FOLDER_MIME_TYPE + "'";
  /**
   * We sort by createdTime ASC
   * so in case the same folder was created multiple times, we always pick the same
   * one which is the oldest one.
   */
  var searchUrl = googleDriveOptions.apiEndpoint + '/drive/v3/files?fields=files(id,mimeType)&orderBy=createdTime asc&q=' + encodeURIComponent(query) + driveSpaceQuerySuffix(googleDriveOptions);
  var searchResponse = await fetch(searchUrl, {
    method: 'GET',
    headers: {
      Authorization: 'Bearer ' + googleDriveOptions.authToken
    }
  });
  var searchData = await searchResponse.json();
  if (searchData.files && searchData.files.length > 0) {
    var file = searchData.files[0];
    if (file.mimeType !== FOLDER_MIME_TYPE) {
      throw (0, _rxError.newRxError)('GDR3', {
        folderName,
        args: {
          file,
          FOLDER_MIME_TYPE
        }
      });
    }
    return file.id;
  } else {
    return undefined;
  }
}
async function ensureFolderExists(googleDriveOptions, folderPath) {
  var parts = folderPath.split('/').filter(p => p.length > 0);
  var parentId = driveRootParentId(googleDriveOptions);
  for (var part of parts) {
    var newParentId = await findFolder(googleDriveOptions, parentId, part);
    if (newParentId) {
      parentId = newParentId;
    } else {
      parentId = await createFolder(googleDriveOptions, parentId, part);
    }
  }
  return parentId;
}
async function createEmptyFile(googleDriveOptions, parentId, fileName) {
  var url = googleDriveOptions.apiEndpoint + '/drive/v3/files?fields=id';
  var body = {
    name: fileName,
    parents: [parentId],
    mimeType: 'application/json'
  };
  var response = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + googleDriveOptions.authToken,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });

  /**
   * Do not throw on duplicates,
   * if the file is there already, find its id
   * and return that one.
   */
  if (!response.ok && response.status !== 409) {
    throw await (0, _rxError.newRxFetchError)(response, {
      folderName: fileName
    });
  }

  /**
   * For idempotent runs, fetch the file again
   * after creating it.
   */
  var query = ["name = '" + fileName + "'", "'" + parentId + "' in parents", "trashed = false"].join(' and ');
  var url2 = googleDriveOptions.apiEndpoint + '/drive/v3/files' + '?fields=files(id)' + '&orderBy=createdTime asc' + '&q=' + encodeURIComponent(query) + driveSpaceQuerySuffix(googleDriveOptions);
  var res = await fetch(url2, {
    headers: {
      Authorization: 'Bearer ' + googleDriveOptions.authToken
    }
  });
  var data = await res.json();
  var fileId = (0, _index.ensureNotFalsy)(data.files[0]?.id);

  /**
   * Fetch the file metadata via v2 to get the etag (from the ETag response header)
   * and the file size atomically in a single request.
   * This avoids a race condition where the etag could change between
   * the file listing and a separate etag fetch.
   */
  var meta = await getFileMetadataV2(googleDriveOptions, fileId);
  return {
    status: response.status,
    etag: meta.etag,
    createdTime: meta.createdTime,
    fileId,
    size: meta.size
  };
}

/**
 * Fetches file metadata from the Google Drive v2 API.
 * Returns the etag (from the ETag response header), size, and createdTime
 * in a single atomic request.
 *
 * Note: the v2 API uses "createdDate" and "fileSize" as field names;
 * this function maps them to "createdTime" and "size" for consistency
 * with the rest of the codebase.
 */
async function getFileMetadataV2(googleDriveOptions, fileId) {
  var url = googleDriveOptions.apiEndpoint + ("/drive/v2/files/" + encodeURIComponent(fileId));
  var res = await fetch(url, {
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken
    }
  });
  if (!res.ok) {
    throw await (0, _rxError.newRxFetchError)(res, {
      args: {
        fileId
      }
    });
  }
  var meta = await res.json();
  return {
    etag: (0, _index.ensureNotFalsy)(res.headers.get('ETag'), 'ETag missing'),
    // v2 uses "fileSize" and "createdDate" instead of the v3 "size" and "createdTime"
    size: parseInt(meta.fileSize ?? '0', 10),
    createdTime: (0, _index.ensureNotFalsy)(meta.createdDate)
  };
}
async function getFileEtag(googleDriveOptions, fileId) {
  var meta = await getFileMetadataV2(googleDriveOptions, fileId);
  return meta.etag;
}
async function fillFileIfEtagMatches(googleDriveOptions, fileId, etag, jsonContent) {
  var url = "" + googleDriveOptions.apiEndpoint + ("/upload/drive/v2/files/" + encodeURIComponent(fileId)) + "?uploadType=media";
  var writeContent = typeof jsonContent !== 'undefined' ? JSON.stringify(jsonContent) : '';
  var res = await fetch(url, {
    method: "PUT",
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken,
      "Content-Type": "application/json; charset=utf-8",
      "If-Match": etag
    },
    body: writeContent
  });
  if (res.status !== 412 && res.status !== 200) {
    throw await (0, _rxError.newRxFetchError)(res);
  }
  return readJsonFileContent(googleDriveOptions, fileId).then(r => {
    return {
      content: r.content,
      etag: r.etag,
      status: res.status,
      serverTime: r.serverTime
    };
  });
}
async function deleteIfEtagMatches(googleDriveOptions, fileId, etag) {
  var url = "" + googleDriveOptions.apiEndpoint + ("/drive/v2/files/" + encodeURIComponent(fileId));
  var res = await fetch(url, {
    method: "DELETE",
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken,
      "If-Match": etag
    }
  });
  if (!res.ok) {
    throw await (0, _rxError.newRxFetchError)(res, {
      args: {
        etag,
        fileId
      }
    });
  }
  if (res.ok) {
    // Drive v2 returns 204 No Content on successful delete
    return;
  }
}
async function deleteFile(googleDriveOptions, fileId) {
  var url = "" + googleDriveOptions.apiEndpoint + ("/drive/v2/files/" + encodeURIComponent(fileId));
  var res = await fetch(url, {
    method: "DELETE",
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken
    }
  });
  if (!res.ok) {
    throw await (0, _rxError.newRxFetchError)(res, {
      args: {
        fileId
      }
    });
  }
  if (res.ok) {
    // Drive v2 returns 204 No Content on successful delete
    return;
  }
}
async function readJsonFileContent(googleDriveOptions, fileId) {
  var url = "" + googleDriveOptions.apiEndpoint + ("/drive/v2/files/" + encodeURIComponent(fileId) + "?alt=media");
  var res = await fetch(url, {
    method: "GET",
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken,
      Accept: "application/json"
    }
  });
  if (!res.ok) {
    throw await (0, _rxError.newRxFetchError)(res, {
      args: {
        fileId
      }
    });
  }
  var dateHeader = res.headers.get('date');
  var unixMs = Date.parse((0, _index.ensureNotFalsy)(dateHeader));
  var contentType = res.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    var err = new Error("NOT_A_JSON_FILE but " + contentType);
    err.code = "NOT_A_JSON_FILE";
    err.contentType = contentType;
    throw err;
  }
  var contentText = await res.text();
  var content = contentText.length > 0 ? JSON.parse(contentText) : undefined;
  var etag = (0, _index.ensureNotFalsy)(res.headers.get('etag'));
  return {
    etag,
    content: content,
    serverTime: unixMs
  };
}
async function readFolder(googleDriveOptions, folderPath) {
  var parentId = driveRootParentId(googleDriveOptions);
  var parts = folderPath.split('/').filter(p => p.length > 0);

  // Resolve folder path
  for (var part of parts) {
    var _query = "name = '" + part + "' and '" + parentId + "' in parents and trashed = false and mimeType = '" + FOLDER_MIME_TYPE + "'";
    var searchUrl = googleDriveOptions.apiEndpoint + '/drive/v3/files?fields=files(id)&q=' + encodeURIComponent(_query) + driveSpaceQuerySuffix(googleDriveOptions);
    var searchResponse = await fetch(searchUrl, {
      method: 'GET',
      headers: {
        Authorization: 'Bearer ' + googleDriveOptions.authToken
      }
    });
    var searchData = await searchResponse.json();
    if (searchData.files && searchData.files.length > 0) {
      parentId = searchData.files[0].id;
    } else {
      throw (0, _rxError.newRxError)('SNH', {
        folderPath
      });
    }
  }

  // List children
  var query = "'" + parentId + "' in parents and trashed = false";
  var listUrl = googleDriveOptions.apiEndpoint + '/drive/v3/files?fields=files(id,name,mimeType,trashed,parents)&q=' + encodeURIComponent(query) + driveSpaceQuerySuffix(googleDriveOptions);
  var listResponse = await fetch(listUrl, {
    method: 'GET',
    headers: {
      Authorization: 'Bearer ' + googleDriveOptions.authToken
    }
  });
  if (!listResponse.ok) {
    throw await (0, _rxError.newRxFetchError)(listResponse, {
      folderName: folderPath
    });
  }
  var listData = await listResponse.json();
  return listData.files || [];
}
async function insertMultipartFile(googleDriveOptions, folderId, filename, jsonData) {
  var content = JSON.stringify(jsonData);
  var metadata = {
    name: filename,
    mimeType: 'application/json',
    parents: [folderId]
  };
  var postData = createMultipartBody(metadata, content);
  var res = await fetch(googleDriveOptions.apiEndpoint + "/upload/drive/v3/files?uploadType=multipart", {
    method: 'POST',
    headers: {
      Authorization: "Bearer " + googleDriveOptions.authToken,
      'Content-Type': 'multipart/related; boundary="' + postData.boundary + '"'
    },
    body: postData.body
  });
  if (!res.ok) {
    throw await (0, _rxError.newRxFetchError)(res);
  }
}
function createMultipartBody(metadata, content) {
  var multipartBoundary = '-------1337-use-RxDB-7355608-' + Math.random().toString(16).slice(2);
  var delimiter = '\r\n--' + multipartBoundary + '\r\n';
  var closeDelim = '\r\n--' + multipartBoundary + '--';
  var body = delimiter + 'Content-Type: application/json\r\n\r\n' + JSON.stringify(metadata) + delimiter + 'Content-Type: application/json\r\n\r\n' + content + closeDelim;
  return {
    body,
    boundary: multipartBoundary
  };
}
;
async function listFilesInFolder(googleDriveOptions, folderId) {
  var q = "'" + folderId + "' in parents and trashed = false";
  var params = new URLSearchParams({
    q,
    pageSize: "1000",
    // max allowed
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
    var text = await res.text().catch(() => "");
    throw new Error("files.list failed: " + res.status + " " + text);
  }
  var data = await res.json();
  return data.files ?? [];
}
//# sourceMappingURL=google-drive-helper.js.map