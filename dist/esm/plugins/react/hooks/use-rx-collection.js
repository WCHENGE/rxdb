import { useState, useEffect } from 'react';
import { filter } from 'rxjs';
import { useRxDatabase } from "./use-rx-database.js";

/**
 * The `useRxCollection` hook retrieves an RxDB collection by name and maintains
 * its state as the collection lifecycle changes.
 *
 * @param {string} name The name of the collection to retrieve.
 * @returns The RxCollection instance or null
 * if the collection does not exist or has been closed.
 */
export function useRxCollection(name) {
  var [collection, setCollection] = useState(null);
  var database = useRxDatabase();
  useEffect(() => {
    if (database == null) {
      return;
    }
    var dbCollection = database.collections[name];
    if (dbCollection != null) {
      setCollection(dbCollection);
    }
    database.collections$.pipe(filter(evt => evt.collection.name === name)).subscribe(evt => {
      if (evt.type == 'ADDED') {
        setCollection(evt.collection);
      } else if (evt.type == 'CLOSED') {
        setCollection(null);
      }
    });
  }, [database, name]);
  return collection;
}
//# sourceMappingURL=use-rx-collection.js.map