"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxDBLeaderElectionPlugin = exports.OPEN_LEADER_ELECTORS = void 0;
exports.getForDatabase = getForDatabase;
exports.getLeaderElectorByBroadcastChannel = getLeaderElectorByBroadcastChannel;
exports.isLeader = isLeader;
exports.onClose = onClose;
exports.rxdb = exports.prototypes = void 0;
exports.waitForLeadership = waitForLeadership;
var _broadcastChannel = require("broadcast-channel");
var _rxStorageMultiinstance = require("../../rx-storage-multiinstance.js");
var _index = require("../utils/index.js");
/**
 * this plugin adds the leader-election-capabilities to rxdb
 */

var LEADER_ELECTORS_OF_DB = new WeakMap();
var LEADER_ELECTOR_BY_BROADCAST_CHANNEL = new WeakMap();
var OPEN_LEADER_ELECTORS = exports.OPEN_LEADER_ELECTORS = new Set();

/**
 * Returns the leader elector of a broadcast channel.
 * Used to ensure we reuse the same elector for the channel each time.
 */
function getLeaderElectorByBroadcastChannel(broadcastChannel) {
  return (0, _index.getFromMapOrCreate)(LEADER_ELECTOR_BY_BROADCAST_CHANNEL, broadcastChannel, () => (0, _broadcastChannel.createLeaderElection)(broadcastChannel));
}

/**
 * @overwrites RxDatabase().leaderElector for caching
 */
function getForDatabase() {
  var broadcastChannel = (0, _rxStorageMultiinstance.getBroadcastChannelReference)(this.storage.name, this.token, this.name, this);

  /**
   * Clean up the reference on RxDatabase.close()
   */
  var oldClose = this.close.bind(this);
  this.close = async function () {
    /**
     * It is very important that we remove the
     * potential leader elected broadcast channel
     * AFTER the database is closed. Otherwise an
     * instance running in another browser tab can
     * already start working on stuff and interfering
     * with a half-closed database and its collections.
     */
    var ret = await oldClose();
    await (0, _rxStorageMultiinstance.removeBroadcastChannelReference)(this.token, this);
    return ret;
  };
  var elector = getLeaderElectorByBroadcastChannel(broadcastChannel);
  LEADER_ELECTORS_OF_DB.set(this, elector);
  OPEN_LEADER_ELECTORS.add(elector);

  /**
   * Overwrite for caching
   */
  this.leaderElector = () => elector;
  return elector;
}
function isLeader() {
  if (!this.multiInstance) {
    return true;
  }
  return this.leaderElector().isLeader;
}
function waitForLeadership() {
  if (!this.multiInstance) {
    return _index.PROMISE_RESOLVE_TRUE;
  } else {
    return this.leaderElector().awaitLeadership().then(() => true);
  }
}

/**
 * runs when the database gets closed
 * Awaits die() so that the election is finished before
 * the broadcast channel is closed further down in close().
 */
async function onClose(db) {
  var has = LEADER_ELECTORS_OF_DB.get(db);
  if (has) {
    await has.die();
    OPEN_LEADER_ELECTORS.delete(has);
  }
}
var rxdb = exports.rxdb = true;
var prototypes = exports.prototypes = {
  RxDatabase: proto => {
    proto.leaderElector = getForDatabase;
    proto.isLeader = isLeader;
    proto.waitForLeadership = waitForLeadership;
  }
};
var RxDBLeaderElectionPlugin = exports.RxDBLeaderElectionPlugin = {
  name: 'leader-election',
  rxdb,
  prototypes,
  hooks: {
    preCloseRxDatabase: {
      after: onClose
    }
  }
};
//# sourceMappingURL=index.js.map