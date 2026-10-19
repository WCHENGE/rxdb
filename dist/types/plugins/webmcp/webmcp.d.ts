import type { RxCollection, RxDatabase } from '../../index.js';
import type { RxWebMCPPlugin, WebMCPOptions, WebMCPLogEvent } from '../../types/plugins/webmcp.js';
import { Subject } from 'rxjs';
export declare function registerWebMCPDatabase(this: RxDatabase, options?: WebMCPOptions): {
    error$: Subject<Error>;
    log$: Subject<WebMCPLogEvent>;
};
export declare function registerWebMCPCollection(this: RxCollection, options?: WebMCPOptions): {
    error$: Subject<Error>;
    log$: Subject<WebMCPLogEvent>;
};
export declare const RxDBWebMCPPlugin: RxWebMCPPlugin;
