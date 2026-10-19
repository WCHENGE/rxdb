import type { WebsocketServerState } from '../replication-websocket/index.js';
import type { ServerOptions, ClientOptions } from 'ws';
import type { RxDatabase, RxStorage } from '../../types/index.js';
import type { CustomRequestHandler, RxStorageRemoteExposeType, RxStorageRemoteSettings } from '../storage-remote/storage-remote-types.js';
import { RxStorageRemote } from '../storage-remote/index.js';
export type RxStorageRemoteWebsocketServerOptions = Omit<ServerOptions, 'perMessageDeflate'> & {
    storage?: RxStorage<any, any>;
    database?: RxDatabase<any, any, any>;
    customRequestHandler?: CustomRequestHandler<any, any>;
    /**
     * Used in tests to simulate what happens if the remote
     * was build on a different RxDB version.
     */
    fakeVersion?: string;
};
export type RxStorageRemoteWebsocketServerState = {
    serverState: WebsocketServerState;
    exposeState: RxStorageRemoteExposeType;
};
export type RxStorageRemoteWebsocketClientOptions = ClientOptions & {
    url: string;
    mode: RxStorageRemoteSettings['mode'];
};
export type RxStorageRemoteWebsocketClient = RxStorageRemote;
