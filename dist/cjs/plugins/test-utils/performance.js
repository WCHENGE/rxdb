"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.averageOfTimeValues = averageOfTimeValues;
exports.runPerformanceTests = runPerformanceTests;
var _assert = _interopRequireDefault(require("assert"));
var _index = require("../../index.js");
var _asyncTestUtil = require("async-test-util");
var _schemaObjects = require("./schema-objects.js");
var _schemas = require("./schemas.js");
/**
 * Runs a performance benchmark against the given RxStorage.
 * Useful for comparing different RxStorage implementations.
 *
 * @param storage - The RxStorage to benchmark.
 * @param storageDescription - A human-readable description of the storage (used in results).
 * @param config - Optional configuration to override the defaults.
 * @returns An object with averaged timing values for each measured operation.
 */
async function runPerformanceTests(storage, storageDescription, config = {}) {
  var {
    runs = 40,
    collectionsAmount = 4,
    docsAmount = 3000,
    serialDocsAmount = 50,
    parallelQueryAmount = 4,
    insertBatches = 6,
    waitBetweenTests = 100,
    log = true,
    password
  } = config;
  var testBulkFindByIds = config.testBulkFindByIds !== false;
  var testSerialFindById = config.testSerialFindById !== false;
  var testFindByQuery = config.testFindByQuery !== false;
  var testFindByQueryParallel = config.testFindByQueryParallel !== false;
  var testCount = config.testCount !== false;
  var testPropertyAccess = config.testPropertyAccess !== false;
  var totalTimes = {};

  // Generate dbName outside the loop to reuse the exact same MongoDB database.
  // This allows `.remove()` to drop the old collections and the next run to cleanly reuse the same 
  // namespace, avoiding creating thousands of collections on the DB server causing file exhaustion.
  var dbName = 'test-db-performance-' + (0, _index.randomToken)(10);
  var runsDone = 0;
  var _loop = async function () {
    var createDbWithCollections = async function () {
      if (collection) {
        await collection.database.close();
      }
      var db = await (0, _index.createRxDatabase)({
        name: dbName,
        eventReduce: true,
        /**
         * A RxStorage implementation
         * might need a full leader election cycle to be usable.
         * So we disable multiInstance here because it would make no sense
         * to measure the leader election time instead of the database
         * creation time.
        */
        multiInstance: false,
        storage,
        password
      });

      // create collections
      var collectionData = {};
      var collectionNames = [];
      new Array(collectionsAmount).fill(0).forEach((_v, idx) => {
        var name = dbName + '_col_' + idx;
        collectionNames.push(name);
        collectionData[name] = {
          schema,
          statics: {}
        };
      });
      var firstCollectionName = collectionNames[0];
      var collections = await db.addCollections(collectionData);
      /**
       * Many storages have a lazy initialization.
       * So it makes no sense to measure the time of database/collection creation.
       * Instead we do a single insert and measure the time to the first insert.
       */
      await collections[collectionNames[1]].insert((0, _schemaObjects.averageSchemaData)());
      return collections[firstCollectionName];
    };
    if (log) {
      console.log('runsDone: ' + runsDone + ' of ' + runs);
    }
    runsDone++;
    var time = performance.now();
    var updateTime = flag => {
      if (!flag) {
        time = performance.now();
        return;
      }
      var diff = performance.now() - time;
      if (!totalTimes[flag]) {
        totalTimes[flag] = [diff];
      } else {
        totalTimes[flag].push(diff);
      }
      time = performance.now();
    };
    await awaitBetweenTest(waitBetweenTests);
    updateTime();

    // create database
    var schema = (0, _schemas.averageSchema)();
    if (password) {
      schema.encrypted = ['deep', 'list'];
      schema.indexes = schema.indexes.filter(index => {
        if (typeof index === 'string') {
          return !index.startsWith('deep.');
        }
        return !index.some(field => field.startsWith('deep.'));
      });
    }
    var collection;
    collection = await createDbWithCollections();
    updateTime('time-to-first-insert');
    await awaitBetweenTest(waitBetweenTests);

    // insert documents (in batches)
    var docIds = [];
    var docsPerBatch = docsAmount / insertBatches;
    for (var i = 0; i < insertBatches; i++) {
      var docsData = new Array(docsPerBatch).fill(0).map((_v, idx) => {
        var data = (0, _schemaObjects.averageSchemaData)({
          var1: idx % 2 + '',
          var2: idx % parallelQueryAmount
        });
        docIds.push(data.id);
        return data;
      });
      updateTime();
      await collection.bulkInsert(docsData);
      updateTime('insert-documents-' + docsPerBatch);
      await awaitBetweenTest(waitBetweenTests);
    }
    if (testBulkFindByIds) {
      // refresh db to ensure we do not run on caches
      collection = await createDbWithCollections();
      await awaitBetweenTest(waitBetweenTests);

      /**
       * Bulk Find by id
       */
      updateTime();
      var idsResult = await collection.findByIds(docIds).exec();
      updateTime('find-by-ids-' + docsAmount);
      _assert.default.strictEqual(Array.from(idsResult.keys()).length, docsAmount, 'find-by-id amount');
      await awaitBetweenTest(waitBetweenTests);
    }

    /**
     * Serial inserts
     */
    updateTime();
    var c = 0;
    var serialIds = [];
    while (c < serialDocsAmount) {
      c++;
      var data = (0, _schemaObjects.averageSchemaData)({
        var2: 1000
      });
      serialIds.push(data.id);
      await collection.insert(data);
    }
    updateTime('serial-inserts-' + serialDocsAmount);
    if (testSerialFindById || testFindByQuery) {
      // refresh db to ensure we do not run on caches
      collection = await createDbWithCollections();
      await awaitBetweenTest(waitBetweenTests);
    }
    if (testSerialFindById) {
      /**
       * Serial find-by-id
       */
      updateTime();
      for (var id of serialIds) {
        await collection.findByIds([id]).exec();
      }
      updateTime('serial-find-by-id-' + serialDocsAmount);
      await awaitBetweenTest(waitBetweenTests);
    }
    var queryResult;
    if (testFindByQuery) {
      // find by query
      updateTime();
      var query = collection.find({
        selector: {},
        sort: [{
          var2: 'asc'
        }, {
          var1: 'asc'
        }]
      });
      queryResult = await query.exec();
      updateTime('find-by-query');
      _assert.default.strictEqual(queryResult.length, docsAmount + serialDocsAmount, 'find-by-query');
    }
    if (testFindByQueryParallel || testCount) {
      // refresh db to ensure we do not run on caches
      collection = await createDbWithCollections();
      await awaitBetweenTest(waitBetweenTests);
    }
    if (testFindByQueryParallel) {
      // find by multiple queries in parallel
      updateTime();
      var parallelResult = await Promise.all(new Array(parallelQueryAmount).fill(0).map((_v, idx) => {
        var subQuery = collection.find({
          selector: {
            var2: idx
          }
        });
        return subQuery.exec();
      }));
      updateTime('find-by-query-parallel-' + parallelQueryAmount);
      var parallelSum = 0;
      parallelResult.forEach(r => parallelSum = parallelSum + r.length);
      _assert.default.strictEqual(parallelSum, docsAmount, 'parallelSum');
      await awaitBetweenTest(waitBetweenTests);
    }
    if (testCount) {
      // run count query
      updateTime();
      var t = 0;
      while (t < parallelQueryAmount) {
        var countQuery = collection.count({
          selector: {
            var2: {
              $eq: t
            }
          }
        });
        var countQueryResult = await countQuery.exec();
        _assert.default.ok(countQueryResult >= docsAmount / insertBatches - 5, 'count A ' + countQueryResult);
        _assert.default.ok(countQueryResult < docsAmount * 0.8, 'count B ' + countQueryResult);
        t++;
      }
      updateTime('4x-count');
      await awaitBetweenTest(waitBetweenTests);
    }
    if (testPropertyAccess && testFindByQuery && queryResult) {
      // test property access time
      updateTime();
      var sum = 0;
      for (var _i = 0; _i < queryResult.length; _i++) {
        var doc = queryResult[_i];

        // access the same property exactly 2 times
        sum += doc.deep.deeper.deepNr;
        sum += doc.deep.deeper.deepNr;
      }
      updateTime('property-access');
      _assert.default.ok(sum > 10);
    }
    await collection.database.remove();
  };
  while (runsDone < runs) {
    await _loop();
  }
  var result = {
    description: storageDescription,
    collectionsAmount,
    docsAmount
  };
  Object.entries(totalTimes).forEach(([key, times]) => {
    result[key] = roundToTwo(averageOfTimeValues(times, 95));
  });
  if (log) {
    console.log('Performance test for ' + storageDescription);
    console.log(JSON.stringify(result, null, 4));
  }
  return result;
}
function averageOfTimeValues(times,
/**
 * To better account for anomalies
 * during time measurements,
 * we strip the highest x percent.
 */
striphighestXPercent) {
  times = times.sort((a, b) => a - b);
  var stripAmount = Math.floor(times.length * (striphighestXPercent * 0.01));
  var useNumbers = times.slice(0, times.length - stripAmount);
  var total = 0;
  useNumbers.forEach(nr => total = total + nr);
  return total / useNumbers.length;
}
function roundToTwo(num) {
  return Math.round(num * 100) / 100;
}
async function awaitBetweenTest(waitMs) {
  await (0, _index.requestIdlePromise)();
  if (waitMs > 0) {
    await (0, _asyncTestUtil.wait)(waitMs);
  }
  await (0, _index.requestIdlePromise)();
  await (0, _index.requestIdlePromise)();
}
//# sourceMappingURL=performance.js.map