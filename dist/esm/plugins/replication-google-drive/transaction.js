import { fillFileIfEtagMatches, readJsonFileContent } from "./google-drive-helper.js";
import { newRxError } from "../../rx-error.js";
import { ensureNotFalsy, now, promiseWait } from "../utils/index.js";
import { processWalFile } from "./upstream.js";
export var TRANSACTION_FILE_NAME = 'transaction.json';
export var TRANSACTION_BLOCKED_FLAG = {
  retry: true
};
export async function startTransactionTryOnce(googleDriveOptions, init) {
  var txFileContent = await readJsonFileContent(googleDriveOptions, init.transactionFile.fileId);
  if (!isEmpty(txFileContent.content)) {
    // tx already existed -> retry
    return TRANSACTION_BLOCKED_FLAG;
  }
  var newTxContent = {
    createdAtClientTime: now()
  };
  var writeResult = await fillFileIfEtagMatches(googleDriveOptions, init.transactionFile.fileId, txFileContent.etag, newTxContent);
  if (writeResult.status !== 200) {
    return TRANSACTION_BLOCKED_FLAG;
  }
  return {
    etag: writeResult.etag
  };
}
export async function startTransaction(googleDriveOptions, init) {
  var current = await startTransactionTryOnce(googleDriveOptions, init);
  var attempts = 0;
  while (current.retry) {
    /**
     * Wait a bit.
     * Exponential backoff: 100, 200, 400, 800, ...
     */

    var waitTime = Math.min(100 * 1000, 100 * Math.pow(1.5, attempts));
    await promiseWait(waitTime);

    /**
     * Check for timeout
     */
    var timeoutState = await isTransactionTimedOut(googleDriveOptions, init);
    if (timeoutState.expired) {
      await fillFileIfEtagMatches(googleDriveOptions, init.transactionFile.fileId, timeoutState.etag, undefined);
    }

    /**
     * Try to get the transaction again
     */
    current = await startTransactionTryOnce(googleDriveOptions, init);
    attempts = attempts + 1;
  }
  return current;
}
export async function isTransactionTimedOut(googleDriveOptions, init) {
  /**
   * Use the v2 API so we can get both the modifiedDate and the ETag
   * in a single request. The v3 API no longer returns etag in the response
   * body, but the v2 API does return it via the ETag response header.
   */
  var request = await fetch(googleDriveOptions.apiEndpoint + '/drive/v2/files/' + init.transactionFile.fileId, {
    headers: {
      Authorization: 'Bearer ' + googleDriveOptions.authToken
    }
  });
  var data = await request.json();
  var dateHeader = request.headers.get('date');
  var serverTime = Date.parse(ensureNotFalsy(dateHeader, 'header time'));

  // v2 uses modifiedDate instead of modifiedTime
  var transactionCreation = Date.parse(ensureNotFalsy(data.modifiedDate, 'tx time'));

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
  var timeLeft = googleDriveOptions.transactionTimeout - transactionAge;
  return {
    timeLeft,
    transactionAge,
    expired: timeLeft <= 0,
    etag: ensureNotFalsy(request.headers.get('ETag'), 'etag')
  };
}
export async function commitTransaction(googleDriveOptions, init, transactionInput) {
  if (transactionInput.retry) {
    throw newRxError('GDR11', {
      args: {
        transaction: transactionInput
      }
    });
  }
  var transaction = transactionInput;
  var writeResult = await fillFileIfEtagMatches(googleDriveOptions, init.transactionFile.fileId, transaction.etag, undefined);
  if (writeResult.status !== 200) {
    throw newRxError('GDR11', {
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
    return Object.keys(value).length === 0;
  }
  return false;
}
export async function runInTransaction(googleDriveOptions, init, primaryPath, fn, runAfter) {
  var transaction = await startTransaction(googleDriveOptions, init);
  await processWalFile(googleDriveOptions, init, primaryPath);
  var result = await fn();
  await commitTransaction(googleDriveOptions, init, transaction);
  if (runAfter) {
    await runAfter();
  }
  return result;
}
//# sourceMappingURL=transaction.js.map