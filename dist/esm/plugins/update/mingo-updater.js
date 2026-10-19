/**
 * Custom build of the mingo updater for smaller build size
 */
import { update } from "mingo/updater";
import { clone } from "../utils/index.js";
export function mingoUpdater(d, op) {
  var cloned = clone(d);
  update(cloned, op, undefined, undefined, {
    cloneMode: "none"
  });
  return cloned;
}
//# sourceMappingURL=mingo-updater.js.map