import type { Instance as SimplePeerInstance } from 'simple-peer';
import { SimplePeerConfig, SimplePeerWrtc } from '../replication-webrtc/connection-handler-simple-peer';
import { DriveStructure } from './init.js';
import type { OneDriveState } from './microsoft-onedrive-types.js';
export type SignalingOptions = {
    wrtc?: SimplePeerWrtc;
    config?: SimplePeerConfig;
};
export type SIGNAL = 'RESYNC' | 'NEW_PEER';
export declare class SignalingState {
    private oneDriveState;
    private init;
    private signalingOptions;
    readonly sessionId: string;
    readonly processedMessageIds: Set<string>;
    /**
     * Emits whenever a new connection
     * is there or some connection
     * told us to RESYNC
     */
    private _resync$;
    resync$: import("rxjs").Observable<void>;
    peerBySenderId: Map<string, SimplePeerInstance>;
    private processQueue;
    private backoffStepId;
    private skipBackoffTime?;
    closed: boolean;
    resetReaderFn: () => void;
    constructor(oneDriveState: OneDriveState, init: DriveStructure, signalingOptions: SignalingOptions);
    sendMessage(data: any): Promise<void>;
    pingPeers(message: SIGNAL): Promise<void>;
    /**
     * Notify other peers that something changed so that they can pull
     * from their checkpoints. This uses two channels:
     * - The WebRTC datachannel for instant delivery to already connected peers.
     * - A durable signaling message so that peers which are not connected via
     *   WebRTC (handshake not finished yet or the connection dropped) still
     *   learn about the change on their next signaling poll and re-sync.
     *
     * Relying on the WebRTC ping alone is not reliable: a ping to a peer that
     * is momentarily not connected is silently dropped and there is no other
     * trigger that makes the peer pull again, so the change would be stuck
     * until the next unrelated write. The durable message closes that gap.
     */
    notifyResync(): Promise<void>;
    resetReadLoop(): Promise<void>;
    processNewMessages(): Promise<any>;
    _processNewMessages(): Promise<void>;
    close(): void;
}
export declare function readMessages(oneDriveState: OneDriveState, init: DriveStructure, processedMessageIds: Set<string>): Promise<{
    senderId: string;
    data: any;
}[]>;
export declare function cleanupOldSignalingMessages(oneDriveState: OneDriveState, signalingFolderId: string, maxAgeMs?: number): Promise<number>;
