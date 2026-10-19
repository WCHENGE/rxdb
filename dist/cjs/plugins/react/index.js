"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var _exportNames = {
  RxDatabaseProvider: true
};
Object.defineProperty(exports, "RxDatabaseProvider", {
  enumerable: true,
  get: function () {
    return _databaseProvider.RxDatabaseProvider;
  }
});
var _databaseProvider = require("./database-provider.js");
var _index = require("./hooks/index.js");
Object.keys(_index).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (Object.prototype.hasOwnProperty.call(_exportNames, key)) return;
  if (key in exports && exports[key] === _index[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _index[key];
    }
  });
});
//# sourceMappingURL=index.js.map