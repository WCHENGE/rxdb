"use strict";

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard").default;
var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.runReplicationBaseTestSuite = runReplicationBaseTestSuite;
var _assert = _interopRequireDefault(require("assert"));
var _asyncTestUtil = require("async-test-util");
var _index = require("../utils/index.js");
var humansCollection = _interopRequireWildcard(require("./humans-collection.js"));
var schemaObjects = _interopRequireWildcard(require("./schema-objects.js"));
var _testUtil = require("./test-util.js");
/**
 * Base test suite for replication plugins.
 * All replication plugins should run these tests
 * to ensure consistent behavior across different backends.
 *
 * The tests use HumanDocumentType (passportId, firstName, lastName, age)
 * with humansCollection.create() for collection creation.
 */

/**
 * Runs the base test suite for a replication plugin.
 * Call this inside a describe() block in your replication test file.
 *
 * The config callbacks should target a shared server endpoint.
 * Each test calls cleanUpServer() at the start to ensure a clean state.
 *
 * @example
 * ```ts
 * describe('replication-nats.test.ts', () => {
 *     runReplicationBaseTestSuite({
 *         startReplication: (collection) => syncNats(collection, natsName),
 *         syncOnce: (collection) => syncOnceNats(collection, natsName),
 *         getAllServerDocs: () => getAllDocsOfServer(natsName),
 *         cleanUpServer: async () => { ... },
 *         softDeletes: true,
 *         isDeleted: (doc) => doc._deleted,
 *         getPrimaryOfServerDoc: (doc) => doc.passportId,
 *     });
 * });
 * ```
 */
function runReplicationBaseTestSuite(config) {
  var waitTime = config.waitTime || 0;
  describe('base test suite', () => {
    describe('live replication', () => {
      it('push replication to client-server', async () => {
        await config.cleanUpServer();
        var collection = await humansCollection.create(2, undefined, false);
        var replicationState = await Promise.resolve(config.startReplication(collection));
        (0, _testUtil.ensureReplicationHasNoErrors)(replicationState);
        await replicationState.awaitInitialReplication();
        var docsOnServer = await config.getAllServerDocs();
        _assert.default.strictEqual(docsOnServer.length, 2);

        // insert another one
        await collection.insert(schemaObjects.humanData());
        await replicationState.awaitInSync();
        docsOnServer = await config.getAllServerDocs();
        _assert.default.strictEqual(docsOnServer.length, 3);

        // update one
        var doc = await collection.findOne().exec(true);
        await doc.incrementalPatch({
          age: 100
        });
        await replicationState.awaitInSync();
        docsOnServer = await config.getAllServerDocs();
        _assert.default.strictEqual(docsOnServer.length, 3);
        var serverDoc = (0, _index.ensureNotFalsy)(docsOnServer.find(d => config.getPrimaryOfServerDoc(d) === doc.primary));
        _assert.default.strictEqual(serverDoc.age, 100);

        // delete one
        await doc.getLatest().remove();
        await replicationState.awaitInSync();
        docsOnServer = await config.getAllServerDocs();
        if (config.softDeletes) {
          _assert.default.strictEqual(docsOnServer.length, 3);
          _assert.default.ok(docsOnServer.find(d => config.isDeleted(d)));
        } else {
          _assert.default.strictEqual(docsOnServer.length, 2);
        }
        await collection.database.close();
      });
      it('two collections', async () => {
        await config.cleanUpServer();
        var collectionA = await humansCollection.create(0, undefined, false);
        await collectionA.insert(schemaObjects.humanData('1aaa'));
        var collectionB = await humansCollection.create(0, undefined, false);
        await collectionB.insert(schemaObjects.humanData('1bbb'));
        var replicationStateA = await Promise.resolve(config.startReplication(collectionA));
        (0, _testUtil.ensureReplicationHasNoErrors)(replicationStateA);
        await replicationStateA.awaitInitialReplication();
        var replicationStateB = await Promise.resolve(config.startReplication(collectionB));
        (0, _testUtil.ensureReplicationHasNoErrors)(replicationStateB);
        await replicationStateB.awaitInitialReplication();
        if (waitTime) {
          await (0, _asyncTestUtil.wait)(waitTime);
        }
        /**
         * Explicitly resync both sides before checking equality.
         * This makes the test independent of realtime event delivery speed:
         * even if realtime events were slow or missed, the explicit pull
         * from the server ensures both sides have the latest data.
         */
        replicationStateA.reSync();
        replicationStateB.reSync();
        await replicationStateA.awaitInSync();
        await replicationStateB.awaitInSync();
        await (0, _testUtil.awaitCollectionsHaveEqualState)(collectionA, collectionB, 'init sync');

        // insert one
        await collectionA.insert(schemaObjects.humanData('insert-a'));
        await replicationStateA.awaitInSync();
        await replicationStateB.awaitInSync();
        if (waitTime) {
          await (0, _asyncTestUtil.wait)(waitTime);
        }
        replicationStateA.reSync();
        replicationStateB.reSync();
        await replicationStateA.awaitInSync();
        await replicationStateB.awaitInSync();
        await (0, _testUtil.awaitCollectionsHaveEqualState)(collectionA, collectionB, 'after insert');

        // delete one
        await collectionB.findOne().remove();
        await replicationStateB.awaitInSync();
        await replicationStateA.awaitInSync();
        if (waitTime) {
          await (0, _asyncTestUtil.wait)(waitTime);
        }
        replicationStateA.reSync();
        replicationStateB.reSync();
        await replicationStateA.awaitInSync();
        await replicationStateB.awaitInSync();
        await (0, _testUtil.awaitCollectionsHaveEqualState)(collectionA, collectionB, 'after deletion');

        // insert many
        await collectionA.bulkInsert(new Array(10).fill(0).map(() => schemaObjects.humanData(undefined, undefined, 'bulk-insert-A')));
        await replicationStateA.awaitInSync();
        await replicationStateB.awaitInSync();
        if (waitTime) {
          await (0, _asyncTestUtil.wait)(waitTime);
        }
        replicationStateA.reSync();
        replicationStateB.reSync();
        await replicationStateA.awaitInSync();
        await replicationStateB.awaitInSync();
        await (0, _testUtil.awaitCollectionsHaveEqualState)(collectionA, collectionB, 'after insert many');

        // insert at both collections at the same time
        await Promise.all([collectionA.insert(schemaObjects.humanData('insert-parallel-a')), collectionB.insert(schemaObjects.humanData('insert-parallel-b'))]);
        await replicationStateA.awaitInSync();
        await replicationStateB.awaitInSync();
        if (waitTime) {
          await (0, _asyncTestUtil.wait)(waitTime);
        }
        replicationStateA.reSync();
        replicationStateB.reSync();
        await replicationStateA.awaitInSync();
        await replicationStateB.awaitInSync();
        await (0, _testUtil.awaitCollectionsHaveEqualState)(collectionA, collectionB, 'after insert both at same time');
        await collectionA.database.close();
        await collectionB.database.close();
      });
    });
    describe('conflict handling', () => {
      it('should keep the master state as default conflict handler', async () => {
        await config.cleanUpServer();
        var c1 = await humansCollection.create(1);
        var c2 = await humansCollection.create(0);
        await config.syncOnce(c1);
        await config.syncOnce(c2);
        var doc1 = await c1.findOne().exec(true);
        var doc2 = await c2.findOne().exec(true);

        // make update on both sides
        await doc1.incrementalPatch({
          firstName: 'c1'
        });
        await doc2.incrementalPatch({
          firstName: 'c2'
        });
        await config.syncOnce(c2);

        // cause conflict
        await config.syncOnce(c1);

        /**
         * Must have kept the master state c2
         */
        _assert.default.strictEqual(doc1.getLatest().firstName, 'c2');
        await c1.database.close();
        await c2.database.close();
      });
    });
    if (config.syncOnceWithAttachments) {
      describe('attachment replication', () => {
        it('should replicate an inserted attachment', async () => {
          await config.cleanUpServer();
          var c1 = await humansCollection.createAttachments(0);
          var c2 = await humansCollection.createAttachments(0);
          var doc1 = await c1.insert(schemaObjects.humanData('att-insert-base'));
          await doc1.putAttachment({
            id: 'test.txt',
            data: new Blob(['hello replication'], {
              type: 'text/plain'
            }),
            type: 'text/plain'
          });
          await config.syncOnceWithAttachments(c1);
          await config.syncOnceWithAttachments(c2);
          var doc2 = await c2.findOne('att-insert-base').exec(true);
          var att2 = doc2.getAttachment('test.txt');
          _assert.default.ok(att2, 'attachment must exist on second collection');
          _assert.default.strictEqual(await att2.getStringData(), 'hello replication');
          await c1.database.close();
          await c2.database.close();
        });
        it('should replicate an updated attachment', async () => {
          await config.cleanUpServer();
          var c1 = await humansCollection.createAttachments(0);
          var c2 = await humansCollection.createAttachments(0);
          var doc1 = await c1.insert(schemaObjects.humanData('att-update-base'));
          await doc1.putAttachment({
            id: 'file.txt',
            data: new Blob(['v1'], {
              type: 'text/plain'
            }),
            type: 'text/plain'
          });
          await config.syncOnceWithAttachments(c1);
          await config.syncOnceWithAttachments(c2);

          // Update on c1
          var doc1v2 = await c1.findOne('att-update-base').exec(true);
          await doc1v2.putAttachment({
            id: 'file.txt',
            data: new Blob(['v2'], {
              type: 'text/plain'
            }),
            type: 'text/plain'
          });
          await config.syncOnceWithAttachments(c1);
          await config.syncOnceWithAttachments(c2);
          var doc2 = await c2.findOne('att-update-base').exec(true);
          var att2 = doc2.getAttachment('file.txt');
          _assert.default.ok(att2, 'updated attachment must exist');
          _assert.default.strictEqual(await att2.getStringData(), 'v2');
          await c1.database.close();
          await c2.database.close();
        });
        it('should replicate a deleted attachment', async () => {
          await config.cleanUpServer();
          var c1 = await humansCollection.createAttachments(0);
          var c2 = await humansCollection.createAttachments(0);
          var doc1 = await c1.insert(schemaObjects.humanData('att-delete-base'));
          await doc1.putAttachment({
            id: 'remove.txt',
            data: new Blob(['bye'], {
              type: 'text/plain'
            }),
            type: 'text/plain'
          });
          await config.syncOnceWithAttachments(c1);
          await config.syncOnceWithAttachments(c2);

          // Delete on c1
          var doc1v2 = await c1.findOne('att-delete-base').exec(true);
          await doc1v2.getAttachment('remove.txt').remove();
          await config.syncOnceWithAttachments(c1);
          await config.syncOnceWithAttachments(c2);
          var doc2 = await c2.findOne('att-delete-base').exec(true);
          _assert.default.strictEqual(doc2.getAttachment('remove.txt'), null, 'deleted attachment must not exist on second collection');
          await c1.database.close();
          await c2.database.close();
        });
        it('should keep the master attachment state on conflict', async () => {
          await config.cleanUpServer();
          var c1 = await humansCollection.createAttachments(0);
          var c2 = await humansCollection.createAttachments(0);
          var doc1 = await c1.insert(schemaObjects.humanData('att-conflict-base'));
          await doc1.putAttachment({
            id: 'shared.txt',
            data: new Blob(['initial'], {
              type: 'text/plain'
            }),
            type: 'text/plain'
          });

          // Both collections get the initial state
          await config.syncOnceWithAttachments(c1);
          await config.syncOnceWithAttachments(c2);

          // c2 updates the attachment and pushes first (becomes master)
          var doc2 = await c2.findOne('att-conflict-base').exec(true);
          await doc2.putAttachment({
            id: 'shared.txt',
            data: new Blob(['from c2 - master'], {
              type: 'text/plain'
            }),
            type: 'text/plain'
          });
          await config.syncOnceWithAttachments(c2);

          // c1 also updates but pushes later → conflict → master (c2) wins
          var doc1v2 = await c1.findOne('att-conflict-base').exec(true);
          await doc1v2.putAttachment({
            id: 'shared.txt',
            data: new Blob(['from c1 - should lose'], {
              type: 'text/plain'
            }),
            type: 'text/plain'
          });
          await config.syncOnceWithAttachments(c1);

          // Extra rounds to converge: some backends need additional sync
          // cycles for the conflict-resolved attachment state to propagate.
          await config.syncOnceWithAttachments(c2);
          await config.syncOnceWithAttachments(c1);
          var doc1Final = await c1.findOne('att-conflict-base').exec(true);
          var att1Final = doc1Final.getAttachment('shared.txt');
          _assert.default.ok(att1Final, 'attachment must still exist after conflict resolution');
          var content = await att1Final.getStringData();
          _assert.default.strictEqual(content, 'from c2 - master', 'master state (c2) must win the conflict');
          await c1.database.close();
          await c2.database.close();
        });
      });
    }
    if (config.syncOnceWithAttachmentsDisabled) {
      describe('attachment replication disabled', () => {
        it('should not replicate attachment data when attachments are disabled', async () => {
          await config.cleanUpServer();
          var c1 = await humansCollection.createAttachments(0);
          var c2 = await humansCollection.createAttachments(0);
          var doc1 = await c1.insert(schemaObjects.humanData('att-disabled'));
          await doc1.putAttachment({
            id: 'no-sync.txt',
            data: new Blob(['secret'], {
              type: 'text/plain'
            }),
            type: 'text/plain'
          });
          await config.syncOnceWithAttachmentsDisabled(c1);
          await config.syncOnceWithAttachmentsDisabled(c2);
          var doc2 = await c2.findOne('att-disabled').exec(true);
          _assert.default.ok(doc2, 'document should be replicated even with attachments disabled');
          var att2 = doc2.getAttachment('no-sync.txt');
          if (att2) {
            var content = await att2.getStringData().catch(() => '');
            _assert.default.notStrictEqual(content, 'secret', 'attachment binary data must not have been replicated');
          }
          await c1.database.close();
          await c2.database.close();
        });
      });
    }
  });
}
//# sourceMappingURL=replication-base-test-suite.js.map