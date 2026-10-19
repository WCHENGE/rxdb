import { useState, useEffect } from 'react';
/**
 * React hook that subscribes to an RxReplicationState and exposes
 * its status as React state. Provides syncing, error, lastSyncedAt,
 * and canceled fields.
 *
 * @param replicationState - The RxReplicationState to observe, or null/undefined.
 * @returns The current replication status.
 */
export function useReplicationStatus(replicationState) {
  var [syncing, setSyncing] = useState(false);
  var [error, setError] = useState(null);
  var [lastSyncedAt, setLastSyncedAt] = useState(null);
  var [canceled, setCanceled] = useState(false);
  useEffect(() => {
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