import type { WebRTCConnectionHandlerCreator } from './webrtc-types.js';
import type { SimplePeer, SimplePeerConfig, SimplePeerWebSocketConstructor, SimplePeerWrtc } from './connection-handler-simple-peer.js';
export type NostrConnectionHandlerOptions = {
    /**
     * Urls of the Nostr relays that are used for signaling,
     * like 'wss://relay.example.com'.
     * All peers of a topic must share at least one relay.
     */
    relays: string[];
    /**
     * The Nostr secret key of this peer (32 bytes).
     * The public key of it becomes part of the peer id
     * and can be checked in isPeerValid() with getNostrPublicKeyOfPeer().
     * If not set, a new random key is generated on each start.
     */
    secretKey?: Uint8Array;
    /**
     * Kind of the ephemeral Nostr events that are used for signaling.
     * Must be in the ephemeral range of 20000-29999 so that relays
     * forward the events but do not store them.
     * [default=25050]
     */
    eventKind?: number;
    wrtc?: SimplePeerWrtc;
    config?: SimplePeerConfig;
    webSocketConstructor?: SimplePeerWebSocketConstructor;
};
export declare const NOSTR_SIGNALING_DEFAULT_EVENT_KIND = 25050;
/**
 * Each peer publishes its presence in this interval.
 * A peer is removed from the room when no presence
 * was received in NOSTR_PRESENCE_TIMEOUT.
 */
export declare const NOSTR_PRESENCE_INTERVAL: number;
export declare const NOSTR_PRESENCE_TIMEOUT: number;
/**
 * Events with a created_at that differs more than this from the local time
 * are ignored so that replayed events do not trigger connection attempts.
 */
export declare const NOSTR_MAX_EVENT_AGE: number;
/**
 * WebRTC creates many signals in a short time because of trickle ICE.
 * Public relays rate limit the number of events per second,
 * so the signals to the same peer are collected for this time
 * and then sent together in one event.
 */
export declare const NOSTR_SIGNAL_BATCH_TIME = 200;
/**
 * Returns the Nostr public key (hex) of a peer that
 * was connected with getConnectionHandlerNostr().
 * The key is verified by the event signatures of the signaling,
 * so it can be used in isPeerValid() to only allow known peers.
 */
export declare function getNostrPublicKeyOfPeer(peer: SimplePeer): string;
/**
 * Returns a connection handler that uses Nostr relays
 * instead of a signaling server to connect the WebRTC peers.
 * The replicated data does not go over the relays,
 * only the WebRTC offers, answers and ICE candidates do.
 * @link https://nostr.how/en/what-is-nostr
 */
export declare function getConnectionHandlerNostr({ relays, secretKey, eventKind, wrtc, config, webSocketConstructor }: NostrConnectionHandlerOptions): WebRTCConnectionHandlerCreator<SimplePeer>;
