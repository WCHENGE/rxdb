import { newRxError } from "./rx-error.js";
import { ensureNotFalsy, flatClone, getProperty, isMaybeReadonlyArray, REGEX_ALL_DOTS, RX_META_LWT_MINIMUM, sortObject, trimDots } from "./plugins/utils/index.js";
/**
 * Helper function to create a valid RxJsonSchema
 * with a given version.
 */
export function getPseudoSchemaForVersion(version, primaryKey) {
  var pseudoSchema = fillWithDefaultSettings({
    version,
    type: 'object',
    primaryKey: primaryKey,
    properties: {
      [primaryKey]: {
        type: 'string',
        maxLength: 100
      },
      value: {
        type: 'string'
      }
    },
    indexes: [[primaryKey]],
    required: [primaryKey]
  });
  return pseudoSchema;
}

/**
 * Cache for getSchemaByObjectPath results.
 * Uses a WeakMap keyed by the schema object reference
 * so the cache is automatically cleaned up when schemas are garbage collected.
 */
var SCHEMA_PATH_CACHE = new WeakMap();

/**
 * Returns the sub-schema for a given path.
 * Results are cached per schema reference and path
 * to avoid redundant string operations and property lookups.
 */
export function getSchemaByObjectPath(rxJsonSchema, path) {
  var pathCache = SCHEMA_PATH_CACHE.get(rxJsonSchema);
  var pathStr = path;
  if (pathCache) {
    var cached = pathCache.get(pathStr);
    if (cached) {
      return cached;
    }
  } else {
    pathCache = new Map();
    SCHEMA_PATH_CACHE.set(rxJsonSchema, pathCache);
  }
  var usePath = pathStr;
  usePath = usePath.replace(REGEX_ALL_DOTS, '.properties.');
  usePath = 'properties.' + usePath;
  usePath = trimDots(usePath);
  var ret = getProperty(rxJsonSchema, usePath);
  pathCache.set(pathStr, ret);
  return ret;
}
export function fillPrimaryKey(primaryPath, jsonSchema, documentData) {
  // optimization shortcut.
  if (typeof jsonSchema.primaryKey === 'string') {
    return documentData;
  }
  var newPrimary = getComposedPrimaryKeyOfDocumentData(jsonSchema, documentData);
  var existingPrimary = documentData[primaryPath];
  if (existingPrimary && existingPrimary !== newPrimary) {
    throw newRxError('DOC19', {
      args: {
        documentData,
        existingPrimary,
        newPrimary
      },
      schema: jsonSchema
    });
  }
  documentData[primaryPath] = newPrimary;
  return documentData;
}
export function getPrimaryFieldOfPrimaryKey(primaryKey) {
  if (typeof primaryKey === 'string') {
    return primaryKey;
  } else {
    return primaryKey.key;
  }
}
export function getLengthOfPrimaryKey(schema) {
  var primaryPath = getPrimaryFieldOfPrimaryKey(schema.primaryKey);
  var schemaPart = getSchemaByObjectPath(schema, primaryPath);
  return ensureNotFalsy(schemaPart.maxLength);
}

/**
 * Returns the composed primaryKey of a document by its data.
 */
export function getComposedPrimaryKeyOfDocumentData(jsonSchema, documentData) {
  if (typeof jsonSchema.primaryKey === 'string') {
    return documentData[jsonSchema.primaryKey];
  }
  var compositePrimary = jsonSchema.primaryKey;
  return compositePrimary.fields.map(field => {
    var value = getProperty(documentData, field);
    if (typeof value === 'undefined') {
      throw newRxError('DOC18', {
        args: {
          field,
          documentData
        }
      });
    }
    return value;
  }).join(compositePrimary.separator);
}

/**
 * Normalize the RxJsonSchema.
 * We need this to ensure everything is set up properly
 * and we have the same hash on schemas that represent the same value but
 * have different json.
 *
 * - Orders the schemas attributes by alphabetical order
 * - Adds the primaryKey to all indexes that do not contain the primaryKey
 * - We need this for deterministic sort order on all queries, which is required for event-reduce to work.
 *
 * @return RxJsonSchema - ordered and filled
 */
export function normalizeRxJsonSchema(jsonSchema) {
  var normalizedSchema = sortObject(jsonSchema, true);
  return normalizedSchema;
}

/**
 * If the schema does not specify any index,
 * we add this index so we at least can run RxQuery()
 * and only select non-deleted fields.
 */
export function getDefaultIndex(primaryPath) {
  return ['_deleted', primaryPath];
}

/**
 * fills the schema-json with default-settings
 * @return cloned schemaObj
 */
export function fillWithDefaultSettings(schemaObj) {
  schemaObj = flatClone(schemaObj);
  var primaryPath = getPrimaryFieldOfPrimaryKey(schemaObj.primaryKey);
  schemaObj.properties = flatClone(schemaObj.properties);

  // additionalProperties is always false
  schemaObj.additionalProperties = false;

  // fill with key-compression-state ()
  if (!Object.prototype.hasOwnProperty.call(schemaObj, 'keyCompression')) {
    schemaObj.keyCompression = false;
  }

  // indexes must be array
  schemaObj.indexes = schemaObj.indexes ? schemaObj.indexes.slice(0) : [];

  // required must be array
  schemaObj.required = schemaObj.required ? schemaObj.required.slice(0) : [];

  // encrypted must be array
  schemaObj.encrypted = schemaObj.encrypted ? schemaObj.encrypted.slice(0) : [];

  // add _rev
  schemaObj.properties._rev = {
    type: 'string',
    minLength: 1
  };

  // add attachments
  schemaObj.properties._attachments = {
    type: 'object'
  };

  // add deleted flag
  schemaObj.properties._deleted = {
    type: 'boolean'
  };

  // add meta property
  schemaObj.properties._meta = RX_META_SCHEMA;

  /**
   * meta fields are all required
   */
  schemaObj.required = schemaObj.required ? schemaObj.required.slice(0) : [];
  schemaObj.required.push('_deleted');
  schemaObj.required.push('_rev');
  schemaObj.required.push('_meta');
  schemaObj.required.push('_attachments');

  // primaryKey is always required
  schemaObj.required.push(primaryPath);
  schemaObj.required = schemaObj.required.filter(field => !field.includes('.')).filter((elem, pos, arr) => arr.indexOf(elem) === pos); // unique;

  // version is 0 by default
  schemaObj.version = schemaObj.version || 0;
  var useIndexes = schemaObj.indexes.map(index => {
    var arIndex = isMaybeReadonlyArray(index) ? index.slice(0) : [index];
    /**
     * Append primary key to indexes that do not contain the primaryKey.
     * All indexes must have the primaryKey to ensure a deterministic sort order.
     */
    if (!arIndex.includes(primaryPath)) {
      arIndex.push(primaryPath);
    }

    // add _deleted flag to all indexes so we can query only non-deleted fields
    // in RxDB itself
    if (arIndex[0] !== '_deleted') {
      arIndex.unshift('_deleted');
    }
    return arIndex;
  });
  if (useIndexes.length === 0) {
    useIndexes.push(getDefaultIndex(primaryPath));
  }

  // we need this index for the getChangedDocumentsSince() method
  useIndexes.push(['_meta.lwt', primaryPath]);

  // also add the internalIndexes
  if (schemaObj.internalIndexes) {
    schemaObj.internalIndexes.map(idx => {
      useIndexes.push(idx);
    });
  }

  // make indexes unique
  var hasIndex = new Set();
  schemaObj.indexes = useIndexes.filter(index => {
    var indexStr = index.join(',');
    if (hasIndex.has(indexStr)) {
      return false;
    } else {
      hasIndex.add(indexStr);
      return true;
    }
  });
  return schemaObj;
}
export var META_LWT_UNIX_TIME_MAX = 1000000000000000;
export var RX_META_SCHEMA = {
  type: 'object',
  properties: {
    /**
     * The last-write time.
     * Unix time in milliseconds.
     */
    lwt: {
      type: 'number',
      /**
       * We use 1 as minimum so that the value is never falsy.
       */
      minimum: RX_META_LWT_MINIMUM,
      maximum: META_LWT_UNIX_TIME_MAX,
      multipleOf: 0.01
    }
  },
  /**
   * Additional properties are allowed
   * and can be used by plugins to set various flags.
   */
  additionalProperties: true,
  required: ['lwt']
};

/**
 * returns the final-fields of the schema
 * @return field-names of the final-fields
 */
export function getFinalFields(jsonSchema) {
  var ret = Object.keys(jsonSchema.properties).filter(key => jsonSchema.properties[key].final);

  // primary is also final
  var primaryPath = getPrimaryFieldOfPrimaryKey(jsonSchema.primaryKey);
  ret.push(primaryPath);

  // fields of composite primary are final
  if (typeof jsonSchema.primaryKey !== 'string') {
    jsonSchema.primaryKey.fields.forEach(field => ret.push(field));
  }
  return ret;
}

/**
 * fills all unset fields with default-values if set
 * @hotPath
 */
export function fillObjectWithDefaults(rxSchema, obj) {
  var defaultKeys = Object.keys(rxSchema.defaultValues);
  for (var i = 0; i < defaultKeys.length; ++i) {
    var key = defaultKeys[i];
    if (obj[key] === undefined) {
      var val = rxSchema.defaultValues[key];
      if (typeof val === 'object' && val !== null) {
        obj[key] = Array.isArray(val) ? val.slice() : {
          ...val
        };
      } else {
        obj[key] = val;
      }
    }
  }
  return obj;
}
export var DEFAULT_CHECKPOINT_SCHEMA = {
  type: 'object',
  properties: {
    id: {
      type: 'string'
    },
    lwt: {
      type: 'number'
    }
  },
  required: ['id', 'lwt'],
  additionalProperties: false
};
//# sourceMappingURL=rx-schema-helper.js.map