import type { RxCollection } from '../../index.js';
import type { WebMCPTarget } from '../../types/plugins/webmcp.js';
/**
 * Builds the target that runs the WebMCP tools directly
 * against a local RxCollection.
 */
export declare function getWebMCPTargetFromCollection(collection: RxCollection<any>): WebMCPTarget;
