import type { HashFunction } from '../../types/index.js';
import type { WebRTCConnectionHandler, WebRTCMessage, WebRTCResponse } from './webrtc-types.js';
/**
 * To deterministically define which peer is master and
 * which peer is fork, we compare the storage tokens.
 * But we have to hash them before, to ensure that
 * a storageToken like 'aaaaaa' is not always the master
 * for all peers.
 */
export declare function isMasterInWebRTCReplication(hashFunction: HashFunction, ownStorageToken: string, otherStorageToken: string): Promise<boolean>;
export declare const WEBRTC_DEFAULT_REQUEST_TIMEOUT: number;
/**
 * Send a message to the peer and await the answer.
 * @throws with an RxError if the peer disconnected,
 * the connection handler was closed,
 * the remote peer responded with an error
 * or no answer was received before the timeout.
 */
export declare function sendMessageAndAwaitAnswer<PeerType>(handler: WebRTCConnectionHandler<PeerType>, peer: PeerType, message: WebRTCMessage, timeout?: number): Promise<WebRTCResponse>;
