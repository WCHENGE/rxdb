import { BehaviorSubject, Subject, Subscription } from 'rxjs';
import type { RxCollection, RxError, RxReplicationHandler, RxTypeError } from '../../types/index.js';
import type { WebRTCConnectionHandler, WebRTCPeerState, WebRTCReplicationCheckpoint, RxWebRTCReplicationState, SyncOptionsWebRTC } from './webrtc-types.js';
export declare function replicateWebRTC<RxDocType, PeerType>(options: SyncOptionsWebRTC<RxDocType, PeerType>): Promise<RxWebRTCReplicationPool<RxDocType, PeerType>>;
/**
 * Because the WebRTC replication runs between many instances,
 * we use a Pool instead of returning a single replication state.
 */
export declare class RxWebRTCReplicationPool<RxDocType, PeerType> {
    readonly collection: RxCollection<RxDocType, any, any, any>;
    readonly options: SyncOptionsWebRTC<RxDocType, PeerType>;
    readonly connectionHandler: WebRTCConnectionHandler<PeerType>;
    peerStates$: BehaviorSubject<Map<PeerType, WebRTCPeerState<RxDocType, PeerType>>>;
    canceled: boolean;
    masterReplicationHandler: RxReplicationHandler<RxDocType, WebRTCReplicationCheckpoint>;
    subs: Subscription[];
    peerValidity: Map<PeerType, Promise<boolean>>;
    connectedPeers: Set<PeerType>;
    error$: Subject<RxError | RxTypeError>;
    constructor(collection: RxCollection<RxDocType, any, any, any>, options: SyncOptionsWebRTC<RxDocType, PeerType>, connectionHandler: WebRTCConnectionHandler<PeerType>);
    /**
     * Returns true if the peer is valid.
     * The result is cached per peer so that
     * options.isPeerValid() is only called once.
     */
    isPeerValid(peer: PeerType): Promise<boolean>;
    isPeerConnected(peer: PeerType): boolean;
    addPeer(peer: PeerType, peerToken: string, replicationState?: RxWebRTCReplicationState<RxDocType>, subs?: Subscription[]): void;
    removePeer(peer: PeerType): Promise<any>;
    private cleanupPeerState;
    awaitFirstPeer(): Promise<Map<PeerType, WebRTCPeerState<RxDocType, PeerType>>>;
    cancel(): Promise<void>;
}
export * from './webrtc-helper.js';
export * from './signaling-server.js';
export * from './webrtc-types.js';
export * from './connection-handler-simple-peer.js';
export * from './connection-handler-nostr.js';
