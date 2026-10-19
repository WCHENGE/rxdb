"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.getMingoQuery = getMingoQuery;
var _core = require("mingo/core");
var _query = require("mingo/query");
var _pipeline = require("mingo/operators/pipeline");
require("mingo/operators/expression");
require("mingo/operators/projection");
var _query2 = require("mingo/operators/query");
require("mingo/operators/query/evaluation");
require("mingo/operators/query/array");
require("mingo/operators/query/element");
var mingoInitDone = false;
var context;

/**
 * The MongoDB query library is huge and we do not need all the operators.
 * If you add an operator here, make sure that you properly add a test in
 * the file /test/unit/rx-storage-query-correctness.test.ts
 *
 * @link https://github.com/kofrasa/mingo#es6
 */
function getMingoQuery(selector) {
  if (!mingoInitDone) {
    context = _core.Context.init({
      pipeline: {
        $sort: _pipeline.$sort,
        $project: _pipeline.$project
      },
      query: {
        $elemMatch: _query2.$elemMatch,
        $eq: _query2.$eq,
        $nor: _query2.$nor,
        $exists: _query2.$exists,
        $regex: _query2.$regex,
        $and: _query2.$and,
        $gt: _query2.$gt,
        $gte: _query2.$gte,
        $in: _query2.$in,
        $lt: _query2.$lt,
        $lte: _query2.$lte,
        $ne: _query2.$ne,
        $nin: _query2.$nin,
        $mod: _query2.$mod,
        $not: _query2.$not,
        $or: _query2.$or,
        $size: _query2.$size,
        $type: _query2.$type
      }
    });
    mingoInitDone = true;
  }
  return new _query.Query(selector, {
    context
  });
}
//# sourceMappingURL=rx-query-mingo.js.map