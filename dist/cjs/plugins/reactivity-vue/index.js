"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.VueRxReactivityFactory = void 0;
var _vue = require("vue");
/**
 * Type-level function (ReactivityLambda) for Vue refs.
 * Use this as the Reactivity type parameter for properly typed refs.
 *
 * @example
 * const db = await createRxDatabase<MyCollections, any, any, VueRefReactivityLambda>({
 *     reactivity: VueRxReactivityFactory
 * });
 * const ref = doc.age$$; // Ref<number>
 */

var VueRxReactivityFactory = exports.VueRxReactivityFactory = {
  fromObservable(obs, initialValue) {
    var ref = (0, _vue.shallowRef)(initialValue);
    var sub = obs.subscribe(value => {
      ref.value = value;
      (0, _vue.triggerRef)(ref);
    });
    (0, _vue.onScopeDispose)(() => {
      sub.unsubscribe();
    });
    return ref;
  }
};
//# sourceMappingURL=index.js.map