"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.useReplicationStatus = useReplicationStatus;
var _react = require("react");
/**
 * React hook that subscribes to an RxReplicationState and exposes
 * its status as React state. Provides syncing, error, lastSyncedAt,
 * and canceled fields.
 *
 * @param replicationState - The RxReplicationState to observe, or null/undefined.
 * @returns The current replication status.
 */
function useReplicationStatus(replicationState) {
  var [syncing, setSyncing] = (0, _react.useState)(false);
  var [error, setError] = (0, _react.useState)(null);
  var [lastSyncedAt, setLastSyncedAt] = (0, _react.useState)(null);
  var [canceled, setCanceled] = (0, _react.useState)(false);
  (0, _react.useEffect)(() => {
    if (!replicationState) {
      return;
    }
    var subs = [replicationState.active$.subscribe(active => {
      setSyncing(active);
    }), replicationState.error$.subscribe(err => {
      setError(err);
    }), replicationState.canceled$.subscribe(isCanceled => {
      setCanceled(isCanceled);
    }), replicationState.received$.subscribe(() => {
      setLastSyncedAt(Date.now());
    }), replicationState.sent$.subscribe(() => {
      setLastSyncedAt(Date.now());
    })];
    return () => {
      subs.forEach(sub => sub.unsubscribe());
    };
  }, [replicationState]);
  return {
    syncing,
    error,
    lastSyncedAt,
    canceled
  };
}
//# sourceMappingURL=use-replication-status.js.map