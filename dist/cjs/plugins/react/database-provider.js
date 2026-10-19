"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxDatabaseProvider = RxDatabaseProvider;
var _react = _interopRequireDefault(require("react"));
var _rxDatabase = require("../../rx-database.js");
var _databaseContext = require("./database-context.js");
var _rxError = require("../../rx-error.js");
/**
 * RxDatabaseProvider is a React context provider component for RxDB.
 * It ensures that a valid RxDatabase instance is passed and makes it available
 * to all descendant components via React Context.
 *
 * @param {RxDatabaseProviderProps} props - The provider props.
 * @param {RxDatabase} props.database - The RxDatabase instance to provide.
 * @param {React.ReactNode} props.children - The child components that will have access to the database.
 * @throws {Error} If the provided database is not a valid RxDatabase instance.
 * @returns {ReactElement} The context provider wrapping the children.
 *
 * @example
 * <RxDatabaseProvider database={myDatabase}>
 *   <MyComponent />
 * </RxDatabaseProvider>
 */
function RxDatabaseProvider({
  children,
  database
}) {
  if (!(0, _rxDatabase.isRxDatabase)(database)) {
    throw (0, _rxError.newRxError)('R1', {
      database
    });
  }
  return /*#__PURE__*/_react.default.createElement(_databaseContext.Provider, {
    value: database
  }, children);
}
//# sourceMappingURL=database-provider.js.map