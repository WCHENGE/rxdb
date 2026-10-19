"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.WEBRTC_DEFAULT_REQUEST_TIMEOUT = void 0;
exports.isMasterInWebRTCReplication = isMasterInWebRTCReplication;
exports.sendMessageAndAwaitAnswer = sendMessageAndAwaitAnswer;
var _rxjs = require("rxjs");
var _rxError = require("../../rx-error.js");
var _index = require("../utils/index.js");
/**
 * To deterministically define which peer is master and
 * which peer is fork, we compare the storage tokens.
 * But we have to hash them before, to ensure that
 * a storageToken like 'aaaaaa' is not always the master
 * for all peers.
 */
async function isMasterInWebRTCReplication(hashFunction, ownStorageToken, otherStorageToken) {
  var isMaster = (await hashFunction([ownStorageToken, otherStorageToken].join('|'))) > (await hashFunction([otherStorageToken, ownStorageToken].join('|')));
  return isMaster;
}
var WEBRTC_DEFAULT_REQUEST_TIMEOUT = exports.WEBRTC_DEFAULT_REQUEST_TIMEOUT = 1000 * 20;

/**
 * Send a message to the peer and await the answer.
 * @throws with an RxError if the peer disconnected,
 * the connection handler was closed,
 * the remote peer responded with an error
 * or no answer was received before the timeout.
 */
function sendMessageAndAwaitAnswer(handler, peer, message, timeout = WEBRTC_DEFAULT_REQUEST_TIMEOUT) {
  var requestId = message.id;
  return new Promise((res, rej) => {
    var done = false;
    var timeoutId;
    var subs = [];
    function finish(fn) {
      if (done) {
        return;
      }
      done = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
      subs.forEach(sub => sub.unsubscribe());
      fn();
    }
    function fail(errorText, error) {
      finish(() => rej((0, _rxError.newRxError)('RC_WEBRTC_PEER', {
        errorText,
        error,
        args: {
          method: message.method,
          id: requestId
        }
      })));
    }
    subs.push(handler.response$.pipe((0, _rxjs.filter)(d => d.peer === peer), (0, _rxjs.filter)(d => d.response.id === requestId), (0, _rxjs.map)(d => d.response)).subscribe({
      next: response => {
        if (response.error) {
          fail('remote peer responded with an error', response.error);
        } else {
          finish(() => res(response));
        }
      },
      complete: () => fail('connection handler closed')
    }), handler.disconnect$.pipe((0, _rxjs.filter)(p => p === peer)).subscribe({
      next: () => fail('peer disconnected'),
      complete: () => fail('connection handler closed')
    }));
    if (done) {
      subs.forEach(sub => sub.unsubscribe());
      return;
    }
    timeoutId = setTimeout(() => fail('request timed out after ' + timeout + 'ms'), timeout);
    Promise.resolve().then(() => handler.send(peer, message)).catch(err => fail('could not send message', (0, _index.errorToPlainJson)(err)));
  });
}
//# sourceMappingURL=webrtc-helper.js.map