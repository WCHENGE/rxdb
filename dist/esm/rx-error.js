/**
 * here we use custom errors with the additional field 'parameters'
 */

import { overwritable } from "./overwritable.js";
/**
 * transform an object of parameters to a presentable string
 */
function parametersToString(parameters) {
  var ret = '';
  if (Object.keys(parameters).length === 0) return ret;
  ret += '-'.repeat(20) + '\n';
  ret += 'Parameters:\n';
  ret += Object.keys(parameters).map(k => {
    var paramStr = '[object Object]';
    try {
      if (k === 'errors') {
        paramStr = parameters[k].map(err => JSON.stringify(err, Object.getOwnPropertyNames(err)));
      } else {
        paramStr = JSON.stringify(parameters[k], function (_k, v) {
          return v === undefined ? null : v;
        }, 2);
      }
    } catch (e) {}
    return k + ': ' + paramStr;
  }).join('\n');
  ret += '\n';
  return ret;
}
function messageForError(message, code, parameters) {
  return '' + '\n' + message + '\n' + parametersToString(parameters);
}
export class RxError extends Error {
  code;
  message;
  url;
  parameters;
  // always true, use this to detect if it's an rxdb-error
  rxdb;
  constructor(code, message, parameters = {}) {
    var mes = messageForError(message, code, parameters);
    super(mes);
    this.code = code;
    this.message = mes;
    this.url = getErrorUrl(code);
    this.parameters = parameters;
    this.rxdb = true; // tag them as internal
  }
  get name() {
    return 'RxError (' + this.code + ')';
  }
  toString() {
    return this.message;
  }
  get typeError() {
    return false;
  }
}
export class RxTypeError extends TypeError {
  code;
  message;
  url;
  parameters;
  // always true, use this to detect if it's an rxdb-error
  rxdb;
  constructor(code, message, parameters = {}) {
    var mes = messageForError(message, code, parameters);
    super(mes);
    this.code = code;
    this.message = mes;
    this.url = getErrorUrl(code);
    this.parameters = parameters;
    this.rxdb = true; // tag them as internal
  }
  get name() {
    return 'RxTypeError (' + this.code + ')';
  }
  toString() {
    return this.message;
  }
  get typeError() {
    return true;
  }
}
export function getErrorUrl(code) {
  return 'https://rxdb.info/errors.html?console=errors#' + code;
}
export function errorUrlHint(code) {
  return '\nFind out more about this error here: ' + getErrorUrl(code) + ' \n' + 'Still stuck? Ask in the RxDB Discord: https://rxdb.info/chat \n';
}
export function newRxError(code, parameters) {
  return new RxError(code, overwritable.tunnelErrorMessage(code) + errorUrlHint(code), parameters);
}
export function newRxTypeError(code, parameters) {
  return new RxTypeError(code, overwritable.tunnelErrorMessage(code) + errorUrlHint(code), parameters);
}

/**
 * Returns the error if it is a 409 conflict,
 * return false if it is another error.
 */
export function isBulkWriteConflictError(err) {
  if (err && err.status === 409) {
    return err;
  } else {
    return false;
  }
}
var STORAGE_WRITE_ERROR_CODE_TO_MESSAGE = {
  409: 'document write conflict',
  422: 'schema validation error',
  510: 'attachment data missing'
};
export function rxStorageWriteErrorToRxError(err) {
  return newRxError('COL20', {
    name: STORAGE_WRITE_ERROR_CODE_TO_MESSAGE[err.status],
    document: err.documentId,
    writeError: err
  });
}
export async function newRxFetchError(input, additionalParameters) {
  var errorText = await input.text().catch(() => '');
  var parameters = {
    ...additionalParameters,
    ...(input.url ? {
      url: input.url
    } : {}),
    ...(input.status ? {
      status: input.status
    } : {}),
    ...(input.statusText ? {
      statusText: input.statusText
    } : {}),
    ...(errorText ? {
      errorText
    } : {})
  };
  return newRxError('FETCH', parameters);
}
//# sourceMappingURL=rx-error.js.map