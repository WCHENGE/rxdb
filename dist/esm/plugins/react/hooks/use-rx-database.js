import { useContext } from 'react';
import { RxDatabaseContext } from "../database-context.js";
import { newRxError } from "../../../rx-error.js";

/**
 * The `useRxDatabase` hook retrieves the RxDB database instance from context.
 *
 * @returns The RxDB database instance.
 * @throws {Error} Throws an error if the component is not wrapped in a <RxDatabaseProvider>.
 * This ensures the database context is properly initialized before use.
 */
export function useRxDatabase() {
  var database = useContext(RxDatabaseContext);
  if (database == null) {
    throw newRxError('R2');
  }
  return database;
}
//# sourceMappingURL=use-rx-database.js.map