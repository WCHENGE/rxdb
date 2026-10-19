import type { RxStorageRemoteWebsocketClient, RxStorageRemoteWebsocketClientOptions, RxStorageRemoteWebsocketServerOptions, RxStorageRemoteWebsocketServerState } from './types.js';
export declare function startRxStorageRemoteWebsocketServer(options: RxStorageRemoteWebsocketServerOptions): RxStorageRemoteWebsocketServerState;
export declare function getRxStorageRemoteWebsocket(options: RxStorageRemoteWebsocketClientOptions): RxStorageRemoteWebsocketClient;
export * from './types.js';
