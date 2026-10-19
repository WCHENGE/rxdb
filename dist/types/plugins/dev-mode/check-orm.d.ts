import type { KeyFunctionMap, RxJsonSchema } from '../../types/index.js';
/**
 * Built-in property and method names on RxAttachment instances.
 * Hard-coded here (instead of read from the RxAttachment class)
 * so the dev-mode plugin does not have to import the attachments plugin.
 * ORM attachment-methods that share a name with any of these would
 * silently shadow the built-in method on each attachment instance.
 */
export declare const RX_ATTACHMENT_RESERVED_NAMES: string[];
/**
 * checks if the given static methods are allowed
 * @throws if not allowed
 */
export declare function checkOrmMethods(statics?: KeyFunctionMap, additionalReservedNames?: string[]): void;
export declare function checkOrmDocumentMethods<RxDocType>(schema: RxJsonSchema<RxDocType>, methods?: any): void;
