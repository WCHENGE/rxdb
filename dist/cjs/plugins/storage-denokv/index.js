"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxStorageDenoKV = void 0;
exports.getRxStorageDenoKV = getRxStorageDenoKV;
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _rxStorageHelper = require("../../rx-storage-helper.js");
var _denokvHelper = require("./denokv-helper.js");
var _rxStorageInstanceDenokv = require("./rx-storage-instance-denokv.js");
var _utilsRxdbVersion = require("../utils/utils-rxdb-version.js");
var RxStorageDenoKV = exports.RxStorageDenoKV = /*#__PURE__*/function () {
  function RxStorageDenoKV(settings) {
    (0, _defineProperty2.default)(this, "name", _denokvHelper.RX_STORAGE_NAME_DENOKV);
    (0, _defineProperty2.default)(this, "rxdbVersion", _utilsRxdbVersion.RXDB_VERSION);
    this.settings = settings;
  }
  var _proto = RxStorageDenoKV.prototype;
  _proto.createStorageInstance = function createStorageInstance(params) {
    (0, _rxStorageHelper.ensureRxStorageInstanceParamsAreCorrect)(params);
    return (0, _rxStorageInstanceDenokv.createDenoKVStorageInstance)(this, params, this.settings);
  };
  return RxStorageDenoKV;
}();
function getRxStorageDenoKV(settings = {
  consistencyLevel: 'strong'
}) {
  var storage = new RxStorageDenoKV(settings);
  return storage;
}
//# sourceMappingURL=index.js.map