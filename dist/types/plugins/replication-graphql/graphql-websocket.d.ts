/**
 * Import from 'graphql-ws/client' instead of 'graphql-ws' so that
 * only the zero-dependency client is bundled. The package root also
 * contains the server code which pulls in the whole graphql package.
 * @link https://github.com/enisdenjo/graphql-ws/blob/master/CHANGELOG.md#600
 */
import { Client } from 'graphql-ws/client';
import { RxGraphQLPullWSOptions } from '../../types';
export type WebsocketWithRefCount = {
    url: string;
    socket: Client;
    refCount: number;
};
export declare const GRAPHQL_WEBSOCKET_BY_URL: Map<string, WebsocketWithRefCount>;
export declare function getGraphQLWebSocket(url: string, headers?: {
    [k: string]: string;
}, options?: RxGraphQLPullWSOptions): Client;
export declare function removeGraphQLWebSocketRef(url: string): void;
