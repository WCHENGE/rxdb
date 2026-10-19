"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxSchema = void 0;
exports.createRxSchema = createRxSchema;
exports.getIndexes = getIndexes;
exports.getPreviousVersions = getPreviousVersions;
exports.isRxSchema = isRxSchema;
exports.toTypedRxJsonSchema = toTypedRxJsonSchema;
var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));
var _defineProperty2 = _interopRequireDefault(require("@babel/runtime/helpers/defineProperty"));
var _index = require("./plugins/utils/index.js");
var _rxError = require("./rx-error.js");
var _hooks = require("./hooks.js");
var _rxSchemaHelper = require("./rx-schema-helper.js");
var _overwritable = require("./overwritable.js");
/**
 * @performance Hashing is expensive and the same schema is often
 * hashed many times, for example when the same schema is used in multiple
 * collections or when a database is closed and created again.
 */
var SCHEMA_HASH_CACHE_MAX_SIZE = 100;
var SCHEMA_HASH_CACHE = new WeakMap();
var RxSchema = exports.RxSchema = /*#__PURE__*/function () {
  function RxSchema(jsonSchema, hashFunction) {
    (0, _defineProperty2.default)(this, "indexes", void 0);
    (0, _defineProperty2.default)(this, "primaryPath", void 0);
    (0, _defineProperty2.default)(this, "finalFields", void 0);
    this.jsonSchema = jsonSchema;
    this.hashFunction = hashFunction;
    this.indexes = getIndexes(this.jsonSchema);

    // primary is always required
    this.primaryPath = (0, _rxSchemaHelper.getPrimaryFieldOfPrimaryKey)(this.jsonSchema.primaryKey);

    /**
     * Many people accidentally put in wrong schema state
     * without the dev-mode plugin, so we need this check here
     * even in non-dev-mode.
     */
    if (!jsonSchema.properties[this.primaryPath].maxLength) {
      throw (0, _rxError.newRxError)('SC39', {
        schema: jsonSchema
      });
    }
    this.finalFields = (0, _rxSchemaHelper.getFinalFields)(this.jsonSchema);
  }
  var _proto = RxSchema.prototype;
  _proto.getJsonSchemaWithoutMeta = function getJsonSchemaWithoutMeta() {
    var jsonSchema = (0, _index.flatClone)(this.jsonSchema);
    jsonSchema.properties = (0, _index.flatClone)(jsonSchema.properties);
    delete jsonSchema.properties._deleted;
    delete jsonSchema.properties._rev;
    delete jsonSchema.properties._meta;
    delete jsonSchema.properties._attachments;
    if (jsonSchema.required) {
      jsonSchema.required = jsonSchema.required.filter(r => !r.startsWith('_'));
    }

    // remove internal meta fields from indexes, consistent with properties and required cleanup
    if (jsonSchema.indexes) {
      jsonSchema.indexes = jsonSchema.indexes.map(index => {
        var arr = (0, _index.isMaybeReadonlyArray)(index) ? [...index] : [index];
        return arr.filter(field => !field.startsWith('_'));
      }).filter(index => index.length > 0);
    }
    return jsonSchema;
  }

  /**
   * @overrides itself on the first call
   */;
  /**
   * checks if a given change on a document is allowed
   * Ensures that:
   * - final fields are not modified
   * @throws {Error} if not valid
   */
  _proto.validateChange = function validateChange(dataBefore, dataAfter) {
    this.finalFields.forEach(fieldName => {
      if (!(0, _index.deepEqual)(dataBefore[fieldName], dataAfter[fieldName])) {
        throw (0, _rxError.newRxError)('DOC9', {
          dataBefore,
          dataAfter,
          fieldName,
          schema: this.jsonSchema
        });
      }
    });
  }

  /**
   * creates the schema-based document-prototype,
   * see RxCollection.getDocumentPrototype()
   */;
  _proto.getDocumentPrototype = function getDocumentPrototype() {
    var proto = {};

    /**
     * On the top level, we know all keys
     * and therefore do not have to create a new Proxy object
     * for each document. Instead we define the getter in the prototype once.
     */
    var pathProperties = (0, _rxSchemaHelper.getSchemaByObjectPath)(this.jsonSchema, '');
    Object.keys(pathProperties).forEach(key => {
      var fullPath = key;

      // getter - value
      proto.__defineGetter__(key, function () {
        if (!this.get || typeof this.get !== 'function') {
          /**
           * When an object gets added to the state of a vuejs-component,
           * it happens that this getter is called with another scope.
           * To prevent errors, we have to return undefined in this case
           */
          return undefined;
        }
        var ret = this.get(fullPath);
        return ret;
      });
      // getter - observable$
      Object.defineProperty(proto, key + '$', {
        get: function () {
          return this.get$(fullPath);
        },
        enumerable: false,
        configurable: false
      });
      // getter - reactivity$$
      Object.defineProperty(proto, key + '$$', {
        get: function () {
          return this.get$$(fullPath);
        },
        enumerable: false,
        configurable: false
      });
      // getter - populate_
      Object.defineProperty(proto, key + '_', {
        get: function () {
          return this.populate(fullPath);
        },
        enumerable: false,
        configurable: false
      });
    });
    (0, _index.overwriteGetterForCaching)(this, 'getDocumentPrototype', () => proto);
    return proto;
  };
  _proto.getPrimaryOfDocumentData = function getPrimaryOfDocumentData(documentData) {
    return (0, _rxSchemaHelper.getComposedPrimaryKeyOfDocumentData)(this.jsonSchema, documentData);
  };
  return (0, _createClass2.default)(RxSchema, [{
    key: "version",
    get: function () {
      return this.jsonSchema.version;
    }
  }, {
    key: "defaultValues",
    get: function () {
      var values = {};
      Object.entries(this.jsonSchema.properties).filter(([, v]) => Object.prototype.hasOwnProperty.call(v, 'default')).forEach(([k, v]) => values[k] = v.default);
      return (0, _index.overwriteGetterForCaching)(this, 'defaultValues', values);
    }
  }, {
    key: "hash",
    get: function () {
      var hashFunction = this.hashFunction;
      var hashByInput = SCHEMA_HASH_CACHE.get(hashFunction);
      if (!hashByInput) {
        hashByInput = new Map();
        SCHEMA_HASH_CACHE.set(hashFunction, hashByInput);
      }
      var input = JSON.stringify(this.jsonSchema);
      var hash = hashByInput.get(input);
      if (!hash) {
        hash = hashFunction(input);
        if (hashByInput.size >= SCHEMA_HASH_CACHE_MAX_SIZE) {
          hashByInput.delete(hashByInput.keys().next().value);
        }
        hashByInput.set(input, hash);
        /**
         * A failed hashing must not be cached
         * so that the next call can try again.
         */
        var cachedHash = hash;
        var cache = hashByInput;
        Promise.resolve(cachedHash).catch(() => {
          if (cache.get(input) === cachedHash) {
            cache.delete(input);
          }
        });
      }
      return (0, _index.overwriteGetterForCaching)(this, 'hash', hash);
    }
  }]);
}();
function getIndexes(jsonSchema) {
  return (jsonSchema.indexes || []).map(index => (0, _index.isMaybeReadonlyArray)(index) ? index : [index]);
}

/**
 * array with previous version-numbers
 */
function getPreviousVersions(schema) {
  var version = schema.version ? schema.version : 0;
  var c = 0;
  return new Array(version).fill(0).map(() => c++);
}
function createRxSchema(jsonSchema, hashFunction, runPreCreateHooks = true) {
  if (runPreCreateHooks) {
    (0, _hooks.runPluginHooks)('preCreateRxSchema', jsonSchema);
  }
  var useJsonSchema = (0, _rxSchemaHelper.fillWithDefaultSettings)(jsonSchema);
  useJsonSchema = (0, _rxSchemaHelper.normalizeRxJsonSchema)(useJsonSchema);
  _overwritable.overwritable.deepFreezeWhenDevMode(useJsonSchema);
  var schema = new RxSchema(useJsonSchema, hashFunction);
  (0, _hooks.runPluginHooks)('createRxSchema', schema);
  return schema;
}
function isRxSchema(obj) {
  return obj instanceof RxSchema;
}

/**
 * Helper function to generate the document type out of the schema via TypeScript.
 * Always use this together with `as const` on the schema literal so that the
 * TypeScript compiler can infer the narrowest possible type.
 *
 * ## AI Agent Guidance — Canonical Schema Definition Pattern
 *
 * ```ts
 * import {
 *   toTypedRxJsonSchema,
 *   ExtractDocumentTypeFromTypedRxJsonSchema,
 *   RxJsonSchema
 * } from 'rxdb';
 *
 * const todoSchemaLiteral = {
 *   title: 'todo schema',
 *   version: 0,
 *   primaryKey: 'id',
 *   type: 'object',
 *   properties: {
 *     id:        { type: 'string', maxLength: 100 },
 *     title:     { type: 'string' },
 *     completed: { type: 'boolean' },
 *     createdAt: { type: 'string', format: 'date-time' },
 *     updatedAt: { type: 'string', format: 'date-time' }
 *   },
 *   required: ['id', 'title', 'completed', 'createdAt', 'updatedAt'],
 *   indexes: ['updatedAt', ['completed', 'updatedAt']]
 * } as const;
 *
 * const schemaTyped = toTypedRxJsonSchema(todoSchemaLiteral);
 * export type TodoDocType = ExtractDocumentTypeFromTypedRxJsonSchema<typeof schemaTyped>;
 * export const todoSchema: RxJsonSchema<TodoDocType> = todoSchemaLiteral;
 * ```
 *
 * @link https://github.com/pubkey/rxdb/discussions/3467
 */
function toTypedRxJsonSchema(schema) {
  return schema;
}
//# sourceMappingURL=rx-schema.js.map