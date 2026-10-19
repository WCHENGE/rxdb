import type { WebRTCConnectionHandlerCreator } from './webrtc-types.js';
import type { Instance as SimplePeerInstance, Options as SimplePeerOptions } from 'simple-peer';
export type SimplePeer = SimplePeerInstance & {
    id: string;
    /**
     * The id of the remote peer as given by the signaling.
     */
    remotePeerId: string;
};
export type SimplePeerInitMessage = {
    type: 'init';
    yourPeerId: string;
};
export type SimplePeerJoinMessage = {
    type: 'join';
    room: string;
};
export type SimplePeerJoinedMessage = {
    type: 'joined';
    otherPeerIds: string[];
};
export type SimplePeerSignalMessage = {
    type: 'signal';
    room: string;
    senderPeerId: string;
    receiverPeerId: string;
    data: any;
    /**
     * Identifies the connection attempt so that signals
     * of outdated attempts are not mixed up with the current one.
     * Optional because older RxDB versions do not send it.
     */
    connectionId?: string;
};
export type SimplePeerPingMessage = {
    type: 'ping';
};
export type PeerMessage = SimplePeerInitMessage | SimplePeerJoinMessage | SimplePeerJoinedMessage | SimplePeerSignalMessage | SimplePeerPingMessage;
export declare const DEFAULT_SIGNALING_SERVER: string;
export type SimplePeerWrtc = SimplePeerOptions['wrtc'];
export type SimplePeerConfig = SimplePeerOptions['config'];
export type SimplePeerWebSocketConstructor = {
    new (url: string): WebSocket;
};
/**
 * Creates a simple-peer compatible wrtc object from a WebRTC polyfill
 * like node-datachannel/polyfill. This is needed because some polyfills
 * return RTCSessionDescription objects with read-only properties
 * that simple-peer tries to mutate.
 */
export declare function createSimplePeerWrtc(polyfill: {
    RTCPeerConnection: any;
    RTCSessionDescription: any;
    RTCIceCandidate: any;
}): SimplePeerWrtc;
export type SimplePeerConnectionHandlerOptions = {
    /**
     * If no server is specified, the default signaling server
     * from signaling.rxdb.info is used.
     * This server is not reliable and you should use
     * your own signaling server instead.
     */
    signalingServerUrl?: string;
    wrtc?: SimplePeerWrtc;
    config?: SimplePeerConfig;
    webSocketConstructor?: SimplePeerWebSocketConstructor;
};
export declare const SIMPLE_PEER_PING_INTERVAL: number;
/**
 * If a peer connection is not established in this time,
 * it is destroyed and a new connection attempt is started.
 */
export declare const SIMPLE_PEER_CONNECT_TIMEOUT: number;
/**
 * Min and max delay between reconnection attempts
 * to the signaling server and to other peers.
 * The delay doubles on each failed attempt.
 */
export declare const SIMPLE_PEER_RECONNECT_DELAY_MIN = 500;
export declare const SIMPLE_PEER_RECONNECT_DELAY_MAX: number;
/**
 * Messages bigger than this are split into chunks
 * because WebRTC data channels have a message size limit
 * which can be as low as 64 KiB depending on the browser.
 * The size is measured in string length and one character can
 * be up to 3 bytes in UTF-8.
 */
export declare const SIMPLE_PEER_MAX_MESSAGE_LENGTH: number;
/**
 * Returns a connection handler that uses simple-peer and the signaling server.
 */
export declare function getConnectionHandlerSimplePeer({ signalingServerUrl, wrtc, config, webSocketConstructor }: SimplePeerConnectionHandlerOptions): WebRTCConnectionHandlerCreator<SimplePeer>;
/**
 * Multiple people had problems because it requires to have
 * the nextTick() method in the runtime. So we check here and
 * throw a helpful error.
 */
export declare function ensureProcessNextTickIsSet(): void;
