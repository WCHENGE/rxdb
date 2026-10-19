import type { RxError, RxTypeError } from '../../types/index.js';
import type { MessageFromRemote, MessageToRemote } from './storage-remote-types.js';
export declare function createErrorAnswer(msg: MessageToRemote, error: Error | TypeError | RxError | RxTypeError): MessageFromRemote;
export declare function createAnswer(msg: MessageToRemote, ret: any): MessageFromRemote;
