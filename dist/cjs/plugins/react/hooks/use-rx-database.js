"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useRxDatabase = useRxDatabase;
var _react = require("react");
var _databaseContext = require("../database-context.js");
var _rxError = require("../../../rx-error.js");
/**
 * The `useRxDatabase` hook retrieves the RxDB database instance from context.
 *
 * @returns The RxDB database instance.
 * @throws {Error} Throws an error if the component is not wrapped in a <RxDatabaseProvider>.
 * This ensures the database context is properly initialized before use.
 */
function useRxDatabase() {
  var database = (0, _react.useContext)(_databaseContext.RxDatabaseContext);
  if (database == null) {
    throw (0, _rxError.newRxError)('R2');
  }
  return database;
}
//# sourceMappingURL=use-rx-database.js.map