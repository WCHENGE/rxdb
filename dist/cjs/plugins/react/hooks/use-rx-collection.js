"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useRxCollection = useRxCollection;
var _react = require("react");
var _rxjs = require("rxjs");
var _useRxDatabase = require("./use-rx-database.js");
/**
 * The `useRxCollection` hook retrieves an RxDB collection by name and maintains
 * its state as the collection lifecycle changes.
 *
 * @param {string} name The name of the collection to retrieve.
 * @returns The RxCollection instance or null
 * if the collection does not exist or has been closed.
 */
function useRxCollection(name) {
  var [collection, setCollection] = (0, _react.useState)(null);
  var database = (0, _useRxDatabase.useRxDatabase)();
  (0, _react.useEffect)(() => {
    if (database == null) {
      return;
    }
    var dbCollection = database.collections[name];
    if (dbCollection != null) {
      setCollection(dbCollection);
    }
    database.collections$.pipe((0, _rxjs.filter)(evt => evt.collection.name === name)).subscribe(evt => {
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