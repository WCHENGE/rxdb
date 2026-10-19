import { Context } from 'mingo/core';
import { Query } from 'mingo/query';
import { $project, $sort } from 'mingo/operators/pipeline';
import 'mingo/operators/expression';
import 'mingo/operators/projection';
import { $elemMatch, $eq, $nor, $exists, $regex, $ne, $gte, $lt, $lte, $nin, $in, $gt, $or, $and, $not, $type, $size, $mod } from 'mingo/operators/query';
import 'mingo/operators/query/evaluation';
import 'mingo/operators/query/array';
import 'mingo/operators/query/element';
var mingoInitDone = false;
var context;

/**
 * The MongoDB query library is huge and we do not need all the operators.
 * If you add an operator here, make sure that you properly add a test in
 * the file /test/unit/rx-storage-query-correctness.test.ts
 *
 * @link https://github.com/kofrasa/mingo#es6
 */
export function getMingoQuery(selector) {
  if (!mingoInitDone) {
    context = Context.init({
      pipeline: {
        $sort,
        $project
      },
      query: {
        $elemMatch,
        $eq,
        $nor,
        $exists,
        $regex,
        $and,
        $gt,
        $gte,
        $in,
        $lt,
        $lte,
        $ne,
        $nin,
        $mod,
        $not,
        $or,
        $size,
        $type
      }
    });
    mingoInitDone = true;
  }
  return new Query(selector, {
    context
  });
}
//# sourceMappingURL=rx-query-mingo.js.map