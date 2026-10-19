import type {
    WithAttachments
} from '../couchdb.js';
import type { RxCollection } from '../rx-collection.js';
import type { MaybePromise } from '../util.js';

export type MigrationStrategy<DocData = any> = (
    oldDocumentData: WithAttachments<DocData>,
    collection: RxCollection
) => MaybePromise<WithAttachments<DocData> | null>;

export type MigrationStrategies<DocData = any> = {
    [toVersion: number]: MigrationStrategy<DocData>;
};
