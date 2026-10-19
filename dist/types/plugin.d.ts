import type { RxPlugin } from './types/index.js';
/**
 * Add a plugin to the RxDB library.
 * Plugins are added globally and cannot be removed.
 *
 * Need help? The RxDB Discord is the fastest place to reach the maintainers.
 * @see https://rxdb.info/chat/?console=code RxDB Discord community and support
 */
export declare function addRxPlugin(plugin: RxPlugin): void;
