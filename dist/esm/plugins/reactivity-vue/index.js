import { shallowRef, triggerRef, onScopeDispose } from 'vue';

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

export var VueRxReactivityFactory = {
  fromObservable(obs, initialValue) {
    var ref = shallowRef(initialValue);
    var sub = obs.subscribe(value => {
      ref.value = value;
      triggerRef(ref);
    });
    onScopeDispose(() => {
      sub.unsubscribe();
    });
    return ref;
  }
};
//# sourceMappingURL=index.js.map