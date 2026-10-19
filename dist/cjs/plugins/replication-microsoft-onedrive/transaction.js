"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.TRANSACTION_FILE_NAME = exports.TRANSACTION_BLOCKED_FLAG = void 0;
exports.commitTransaction = commitTransaction;
exports.isTransactionTimedOut = isTransactionTimedOut;
exports.runInTransaction = runInTransaction;
exports.startTransaction = startTransaction;
exports.startTransactionTryOnce = startTransactionTryOnce;
var _microsoftOnedriveHelper = require("./microsoft-onedrive-helper.js");
var _rxError = require("../../rx-error.js");
var _index = require("../utils/index.js");
var _upstream = require("./upstream.js");
var TRANSACTION_FILE_NAME = exports.TRANSACTION_FILE_NAME = 'transaction.json';
var TRANSACTION_BLOCKED_FLAG = exports.TRANSACTION_BLOCKED_FLAG = {
  retry: true
};
async function startTransactionTryOnce(oneDriveState, init) {
  var txFileContent = await (0, _microsoftOnedriveHelper.readJsonFileContent)(oneDriveState, init.transactionFile.fileId);
  var content = txFileContent.content;
  if (!isEmpty(content)) {
    // tx already existed -> retry
    return TRANSACTION_BLOCKED_FLAG;
  }
  var newTxContent = {
    createdAtClientTime: (0, _index.now)()
  };
  var writeResult = await (0, _microsoftOnedriveHelper.fillFileIfEtagMatches)(oneDriveState, init.transactionFile.fileId, txFileContent.etag, newTxContent);
  if (writeResult.status !== 200 && writeResult.status !== 201) {
    return TRANSACTION_BLOCKED_FLAG;
  }
  return {
    etag: writeResult.etag
  };
}
async function startTransaction(oneDriveState, init) {
  var current = await startTransactionTryOnce(oneDriveState, init);
  var attempts = 0;
  while (current.retry) {
    /**
     * Wait a bit.
     * Exponential backoff: 100, 200, 400, 800, ...
     */

    var waitTime = Math.min(100 * 1000, 100 * Math.pow(1.5, attempts));
    await (0, _index.promiseWait)(waitTime);

    /**
     * Check for timeout
     */
    var timeoutState = await isTransactionTimedOut(oneDriveState, init);
    if (timeoutState.expired) {
      await (0, _microsoftOnedriveHelper.fillFileIfEtagMatches)(oneDriveState, init.transactionFile.fileId, timeoutState.etag, undefined);
    }

    /**
     * Try to get the transaction again
     */
    current = await startTransactionTryOnce(oneDriveState, init);
    attempts = attempts + 1;
  }
  return current;
}
async function isTransactionTimedOut(oneDriveState, init) {
  var baseUrl = (0, _microsoftOnedriveHelper.getDriveBaseUrl)(oneDriveState);
  var request = await fetch(baseUrl + "/items/" + init.transactionFile.fileId + "?$select=lastModifiedDateTime,eTag", {
    headers: {
      Authorization: 'Bearer ' + oneDriveState.authToken
    }
  });
  var data = await request.json();
  var dateHeader = request.headers.get('date');
  var serverTime = Date.parse((0, _index.ensureNotFalsy)(dateHeader, 'header time'));
  var transactionCreation = Date.parse((0, _index.ensureNotFalsy)(data.lastModifiedDateTime, 'tx time'));

  /**
   * By definition the HTTP Date header
   * only has seconds precision so if
   * the serverTime is "impossible", set it
   * to the transaction creation time.
   */
  if (serverTime < transactionCreation) {
    serverTime = transactionCreation;
  }
  var transactionAge = serverTime - transactionCreation;
  var timeLeft = (oneDriveState.transactionTimeout || 10000) - transactionAge;
  return {
    timeLeft,
    transactionAge,
    expired: timeLeft <= 0,
    etag: (0, _index.ensureNotFalsy)(data.eTag, 'etag')
  };
}
async function commitTransaction(oneDriveState, init, transactionInput) {
  if (transactionInput.retry) {
    throw (0, _rxError.newRxError)('ODR11', {
      args: {
        transaction: transactionInput
      }
    });
  }
  var transaction = transactionInput;
  var writeResult = await (0, _microsoftOnedriveHelper.fillFileIfEtagMatches)(oneDriveState, init.transactionFile.fileId, transaction.etag, undefined);
  if (writeResult.status !== 200 && writeResult.status !== 201) {
    throw (0, _rxError.newRxError)('ODR11', {
      args: {
        status: writeResult.status,
        etag: writeResult.etag
      }
    });
  }
}
function isEmpty(value) {
  if (value === undefined || value === null) return true;
  if (typeof value === "string") {
    return value.length === 0;
  }
  if (typeof value === "object" && !Array.isArray(value)) {
    if (value.type === 'Buffer' && Array.isArray(value.data) && value.data.length === 0) {
      return true;
    }
    return Object.keys(value).length === 0;
  }
  return false;
}
async function runInTransaction(oneDriveState, init, primaryPath, fn, runAfter) {
  var transaction = await startTransaction(oneDriveState, init);
  await (0, _upstream.processWalFile)(oneDriveState, init, primaryPath);
  var result = await fn();
  await commitTransaction(oneDriveState, init, transaction);
  if (runAfter) {
    await runAfter();
  }
  return result;
}
//# sourceMappingURL=transaction.js.map