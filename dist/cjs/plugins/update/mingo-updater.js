"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.mingoUpdater = mingoUpdater;
var _updater = require("mingo/updater");
var _index = require("../utils/index.js");
/**
 * Custom build of the mingo updater for smaller build size
 */

function mingoUpdater(d, op) {
  var cloned = (0, _index.clone)(d);
  (0, _updater.update)(cloned, op, undefined, undefined, {
    cloneMode: "none"
  });
  return cloned;
}
//# sourceMappingURL=mingo-updater.js.map