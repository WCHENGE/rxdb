import { untracked } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';

/**
 * Type-level function (ReactivityLambda) for Angular signals.
 * Use this as the Reactivity type parameter for properly typed signals.
 *
 * @example
 * const db = await createRxDatabase<MyCollections, any, any, AngularSignalReactivityLambda>({
 *     reactivity: createReactivityFactory(injector)
 * });
 * const signal = doc.age$$; // Signal<number>
 */

export function createReactivityFactory(injector) {
  return {
    fromObservable(observable$, initialValue) {
      return untracked(() => toSignal(observable$, {
        initialValue,
        injector
      }));
    }
  };
}
//# sourceMappingURL=index.js.map