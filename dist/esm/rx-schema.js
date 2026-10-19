import { overwriteGetterForCaching, isMaybeReadonlyArray, deepEqual, flatClone } from "./plugins/utils/index.js";
import { newRxError } from "./rx-error.js";
import { runPluginHooks } from "./hooks.js";
import { fillWithDefaultSettings, getComposedPrimaryKeyOfDocumentData, getFinalFields, getPrimaryFieldOfPrimaryKey, getSchemaByObjectPath, normalizeRxJsonSchema } from "./rx-schema-helper.js";
import { overwritable } from "./overwritable.js";

/**
 * @performance Hashing is expensive and the same schema is often
 * hashed many times, for example when the same schema is used in multiple
 * collections or when a database is closed and created again.
 */
var SCHEMA_HASH_CACHE_MAX_SIZE = 100;
var SCHEMA_HASH_CACHE = new WeakMap();
export class RxSchema {
  indexes;
  primaryPath;
  finalFields;
  constructor(jsonSchema, hashFunction) {
    this.jsonSchema = jsonSchema;
    this.hashFunction = hashFunction;
    this.indexes = getIndexes(this.jsonSchema);

    // primary is always required
    this.primaryPath = getPrimaryFieldOfPrimaryKey(this.jsonSchema.primaryKey);

    /**
     * Many people accidentally put in wrong schema state
     * without the dev-mode plugin, so we need this check here
     * even in non-dev-mode.
     */
    if (!jsonSchema.properties[this.primaryPath].maxLength) {
      throw newRxError('SC39', {
        schema: jsonSchema
      });
    }
    this.finalFields = getFinalFields(this.jsonSchema);
  }
  get version() {
    return this.jsonSchema.version;
  }
  get defaultValues() {
    var values = {};
    Object.entries(this.jsonSchema.properties).filter(([, v]) => Object.prototype.hasOwnProperty.call(v, 'default')).forEach(([k, v]) => values[k] = v.default);
    return overwriteGetterForCaching(this, 'defaultValues', values);
  }
  getJsonSchemaWithoutMeta() {
    var jsonSchema = flatClone(this.jsonSchema);
    jsonSchema.properties = flatClone(jsonSchema.properties);
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
        var arr = isMaybeReadonlyArray(index) ? [...index] : [index];
        return arr.filter(field => !field.startsWith('_'));
      }).filter(index => index.length > 0);
    }
    return jsonSchema;
  }

  /**
   * @overrides itself on the first call
   */
  get hash() {
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
    return overwriteGetterForCaching(this, 'hash', hash);
  }

  /**
   * checks if a given change on a document is allowed
   * Ensures that:
   * - final fields are not modified
   * @throws {Error} if not valid
   */
  validateChange(dataBefore, dataAfter) {
    this.finalFields.forEach(fieldName => {
      if (!deepEqual(dataBefore[fieldName], dataAfter[fieldName])) {
        throw newRxError('DOC9', {
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
   */
  getDocumentPrototype() {
    var proto = {};

    /**
     * On the top level, we know all keys
     * and therefore do not have to create a new Proxy object
     * for each document. Instead we define the getter in the prototype once.
     */
    var pathProperties = getSchemaByObjectPath(this.jsonSchema, '');
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
    overwriteGetterForCaching(this, 'getDocumentPrototype', () => proto);
    return proto;
  }
  getPrimaryOfDocumentData(documentData) {
    return getComposedPrimaryKeyOfDocumentData(this.jsonSchema, documentData);
  }
}
export function getIndexes(jsonSchema) {
  return (jsonSchema.indexes || []).map(index => isMaybeReadonlyArray(index) ? index : [index]);
}

/**
 * array with previous version-numbers
 */
export function getPreviousVersions(schema) {
  var version = schema.version ? schema.version : 0;
  var c = 0;
  return new Array(version).fill(0).map(() => c++);
}
export function createRxSchema(jsonSchema, hashFunction, runPreCreateHooks = true) {
  if (runPreCreateHooks) {
    runPluginHooks('preCreateRxSchema', jsonSchema);
  }
  var useJsonSchema = fillWithDefaultSettings(jsonSchema);
  useJsonSchema = normalizeRxJsonSchema(useJsonSchema);
  overwritable.deepFreezeWhenDevMode(useJsonSchema);
  var schema = new RxSchema(useJsonSchema, hashFunction);
  runPluginHooks('createRxSchema', schema);
  return schema;
}
export function isRxSchema(obj) {
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
export function toTypedRxJsonSchema(schema) {
  return schema;
}
//# sourceMappingURL=rx-schema.js.map