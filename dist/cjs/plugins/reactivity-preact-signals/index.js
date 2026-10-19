"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.PreactSignalsRxReactivityFactory = exports.PREACT_SIGNAL_STATE = void 0;
var _signalsCore = require("@preact/signals-core");
/**
 * Type-level function (ReactivityLambda) for Preact signals.
 * Use this as the Reactivity type parameter for properly typed signals.
 *
 * @example
 * const db = await createRxDatabase<MyCollections, any, any, PreactSignalReactivityLambda>({
 *     reactivity: PreactSignalsRxReactivityFactory
 * });
 * const signal = doc.age$$; // Signal<number>
 */

/**
 * Exported to debug and test
 * the behavior
 */
var PREACT_SIGNAL_STATE = exports.PREACT_SIGNAL_STATE = {
  subscribeCount: 0,
  signalBySubscription: new WeakMap(),
  aliveSubscription: new Set()
};
function removeSubscription(sub) {
  var isAlive = PREACT_SIGNAL_STATE.aliveSubscription.has(sub);
  if (isAlive) {
    PREACT_SIGNAL_STATE.aliveSubscription.delete(sub);
    PREACT_SIGNAL_STATE.signalBySubscription.delete(sub);
    PREACT_SIGNAL_STATE.subscribeCount = PREACT_SIGNAL_STATE.subscribeCount - 1;
    sub.unsubscribe();
  }
}
function cleanupCallback(sub) {
  // called when a signal becomes garbage collected
  removeSubscription(sub);
}
var cleanupRegistry = new FinalizationRegistry(cleanupCallback);
var PreactSignalsRxReactivityFactory = exports.PreactSignalsRxReactivityFactory = {
  fromObservable(obs, initialValue) {
    var mySignal = (0, _signalsCore.signal)(initialValue);
    var sigRef = new WeakRef(mySignal);
    var sub = obs.subscribe(value => {
      var sig = PREACT_SIGNAL_STATE.signalBySubscription.get(sub);
      if (sig && sigRef.deref()) {
        sig.value = value;
      } else {
        /**
         * The callback of FinalizationRegistry is not reliable by definition
         * so we have this fallback to still clean unused signals subscriptions
         * when they emit but the signal is already not used anymore.
         * @link https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry#notes_on_cleanup_callbacks
         */
        removeSubscription(sub);
      }
    });
    PREACT_SIGNAL_STATE.aliveSubscription.add(sub);
    PREACT_SIGNAL_STATE.signalBySubscription.set(sub, mySignal);
    PREACT_SIGNAL_STATE.subscribeCount = PREACT_SIGNAL_STATE.subscribeCount + 1;
    cleanupRegistry.register({}, sub);
    return mySignal;
  }
};
//# sourceMappingURL=index.js.map