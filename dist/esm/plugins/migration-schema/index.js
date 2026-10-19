import { shareReplay } from 'rxjs';
import { getFromMapOrCreate, PROMISE_RESOLVE_FALSE, RXJS_SHARE_REPLAY_DEFAULTS } from "../../plugins/utils/index.js";
import { RxMigrationState } from "./rx-migration-state.js";
import { getMigrationStateByDatabase, mustMigrate, onDatabaseClose } from "./migration-helpers.js";
import { addRxPlugin } from "../../plugin.js";
import { RxDBLocalDocumentsPlugin } from "../local-documents/index.js";
export var DATA_MIGRATOR_BY_COLLECTION = new WeakMap();
export var RxDBMigrationPlugin = {
  name: 'migration-schema',
  rxdb: true,
  init() {
    addRxPlugin(RxDBLocalDocumentsPlugin);
  },
  hooks: {
    preCloseRxDatabase: {
      after: onDatabaseClose
    },
    /**
     * Block writes to the new collection while a migration is pending.
     * For schemas with version > 0 we optimistically set the flag, then
     * release it once we know no migration is actually needed. If a
     * migration is needed, the flag stays set until startMigration()
     * runs to completion (or fails/cancels), which clears it.
     */
    createRxCollection: {
      after: i => {
        var collection = i.collection;
        if (collection.schema.version === 0) {
          return;
        }
        collection.migrationInProgress = true;
        collection.migrationNeeded().then(needed => {
          if (!needed) {
            collection.migrationInProgress = false;
          }
        }).catch(() => {
          collection.migrationInProgress = false;
        });
      }
    }
  },
  prototypes: {
    RxDatabase: proto => {
      proto.migrationStates = function () {
        return getMigrationStateByDatabase(this).pipe(shareReplay(RXJS_SHARE_REPLAY_DEFAULTS));
      };
    },
    RxCollection: proto => {
      proto.getMigrationState = function () {
        return getFromMapOrCreate(DATA_MIGRATOR_BY_COLLECTION, this, () => new RxMigrationState(this.asRxCollection, this.migrationStrategies));
      };
      proto.migrationNeeded = function () {
        if (this.schema.version === 0) {
          return PROMISE_RESOLVE_FALSE;
        }
        return mustMigrate(this.getMigrationState());
      };
    }
  }
};
export var RxDBMigrationSchemaPlugin = RxDBMigrationPlugin;
export * from "./rx-migration-state.js";
export * from "./migration-helpers.js";
export * from "./migration-types.js";
//# sourceMappingURL=index.js.map