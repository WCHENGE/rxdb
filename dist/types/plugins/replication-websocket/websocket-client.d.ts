import { RxReplicationState } from '../replication/index.js';
import { WebsocketClientOptions } from './websocket-types.js';
import { Subject, BehaviorSubject } from 'rxjs';
import type { RxError } from '../../types/index.js';
export type WebsocketClient = {
    url: string;
    socket: any;
    connected$: BehaviorSubject<boolean>;
    message$: Subject<any>;
    error$: Subject<RxError>;
};
export declare function createWebSocketClient<RxDocType>(options: WebsocketClientOptions<RxDocType>): Promise<WebsocketClient>;
export declare function replicateWithWebsocketServer<RxDocType, CheckpointType>(options: WebsocketClientOptions<RxDocType>): Promise<RxReplicationState<RxDocType, CheckpointType>>;
