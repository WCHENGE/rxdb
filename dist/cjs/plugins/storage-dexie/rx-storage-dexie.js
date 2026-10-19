"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxStorageDexie = void 0;
exports.getRxStorageDexie = getRxStorageDexie;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _dexieHelper = require("./dexie-helper.js");
var _rxStorageInstanceDexie = require("./rx-storage-instance-dexie.js");
var _rxStorageHelper = require("../../rx-storage-helper.js");
var _utilsRxdbVersion = require("../utils/utils-rxdb-version.js");
var _rxError = require("../../rx-error.js");
var RxStorageDexie = exports.RxStorageDexie = /*#__PURE__*/function () {
  function RxStorageDexie(settings) {
    (0, _defineProperty2.default)(this, "name", _dexieHelper.RX_STORAGE_NAME_DEXIE);
    (0, _defineProperty2.default)(this, "rxdbVersion", _utilsRxdbVersion.RXDB_VERSION);
    this.settings = settings;
  }
  var _proto = RxStorageDexie.prototype;
  _proto.createStorageInstance = function createStorageInstance(params) {
    (0, _rxStorageHelper.ensureRxStorageInstanceParamsAreCorrect)(params);

    /**
     * Dexie does not support non-required indexes and must throw if that is used.
     * @link https://github.com/pubkey/rxdb/pull/6643#issuecomment-2505310082
     */
    if (params.schema.indexes) {
      var indexFields = params.schema.indexes.flat();
      indexFields.filter(indexField => !indexField.includes('.')).forEach(indexField => {
        if (!params.schema.required || !params.schema.required.includes(indexField)) {
          throw (0, _rxError.newRxError)('DXE1', {
            field: indexField,
            schema: params.schema
          });
        }
      });
    }
    return (0, _rxStorageInstanceDexie.createDexieStorageInstance)(this, params, this.settings);
  };
  return RxStorageDexie;
}();
function getRxStorageDexie(settings = {}) {
  var storage = new RxStorageDexie(settings);
  return storage;
}
//# sourceMappingURL=rx-storage-dexie.js.map