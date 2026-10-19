import type { Instance as SimplePeerInstance } from 'simple-peer';
import { SimplePeerConfig, SimplePeerWrtc } from '../replication-webrtc/connection-handler-simple-peer';
import { DriveStructure } from './init.js';
import { GoogleDriveOptionsWithDefaults } from './google-drive-types.js';
export type SignalingOptions = {
    wrtc?: SimplePeerWrtc;
    config?: SimplePeerConfig;
};
export type SIGNAL = 'RESYNC' | 'NEW_PEER';
export declare class SignalingState {
    private googleDriveOptions;
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
    constructor(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, signalingOptions: SignalingOptions);
    sendMessage(data: any): Promise<void>;
    pingPeers(message: SIGNAL): Promise<void>;
    resetReadLoop(): Promise<void>;
    processNewMessages(): Promise<any>;
    _processNewMessages(): Promise<void>;
    close(): void;
}
export declare function readMessages(googleDriveOptions: GoogleDriveOptionsWithDefaults, init: DriveStructure, processedMessageIds: Set<string>): Promise<{
    senderId: string;
    data: any;
}[]>;
export declare function cleanupOldSignalingMessages(googleDriveOptions: GoogleDriveOptionsWithDefaults, signalingFolderId: string, maxAgeMs?: number): Promise<number>;
