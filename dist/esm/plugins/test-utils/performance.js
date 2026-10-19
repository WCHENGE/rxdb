import assert from 'assert';
import { createRxDatabase, randomToken, requestIdlePromise } from "../../index.js";
import { wait } from 'async-test-util';
import { averageSchemaData } from "./schema-objects.js";
import { averageSchema } from "./schemas.js";
/**
 * Runs a performance benchmark against the given RxStorage.
 * Useful for comparing different RxStorage implementations.
 *
 * @param storage - The RxStorage to benchmark.
 * @param storageDescription - A human-readable description of the storage (used in results).
 * @param config - Optional configuration to override the defaults.
 * @returns An object with averaged timing values for each measured operation.
 */
export async function runPerformanceTests(storage, storageDescription, config = {}) {
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
  var dbName = 'test-db-performance-' + randomToken(10);
  var runsDone = 0;
  var _loop = async function () {
    var createDbWithCollections = async function () {
      if (collection) {
        await collection.database.close();
      }
      var db = await createRxDatabase({
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
      await collections[collectionNames[1]].insert(averageSchemaData());
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
    var schema = averageSchema();
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
        var data = averageSchemaData({
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
      assert.strictEqual(Array.from(idsResult.keys()).length, docsAmount, 'find-by-id amount');
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
      var data = averageSchemaData({
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
      assert.strictEqual(queryResult.length, docsAmount + serialDocsAmount, 'find-by-query');
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
      assert.strictEqual(parallelSum, docsAmount, 'parallelSum');
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
        assert.ok(countQueryResult >= docsAmount / insertBatches - 5, 'count A ' + countQueryResult);
        assert.ok(countQueryResult < docsAmount * 0.8, 'count B ' + countQueryResult);
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
      assert.ok(sum > 10);
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
export function averageOfTimeValues(times,
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
  await requestIdlePromise();
  if (waitMs > 0) {
    await wait(waitMs);
  }
  await requestIdlePromise();
  await requestIdlePromise();
}
//# sourceMappingURL=performance.js.map