"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RX_ATTACHMENT_RESERVED_NAMES = void 0;
exports.checkOrmDocumentMethods = checkOrmDocumentMethods;
exports.checkOrmMethods = checkOrmMethods;
var _rxError = require("../../rx-error.js");
var _entityProperties = require("./entity-properties.js");
/**
 * Built-in property and method names on RxAttachment instances.
 * Hard-coded here (instead of read from the RxAttachment class)
 * so the dev-mode plugin does not have to import the attachments plugin.
 * ORM attachment-methods that share a name with any of these would
 * silently shadow the built-in method on each attachment instance.
 */
var RX_ATTACHMENT_RESERVED_NAMES = exports.RX_ATTACHMENT_RESERVED_NAMES = ['doc', 'id', 'type', 'length', 'digest', 'remove', 'getData', 'getStringData', 'getDataBase64'];

/**
 * checks if the given static methods are allowed
 * @throws if not allowed
 */
function checkOrmMethods(statics, additionalReservedNames) {
  if (!statics) {
    return;
  }
  Object.entries(statics).forEach(([k, v]) => {
    if (typeof k !== 'string') {
      throw (0, _rxError.newRxTypeError)('COL14', {
        name: k
      });
    }
    if (k.startsWith('_')) {
      throw (0, _rxError.newRxTypeError)('COL15', {
        name: k
      });
    }
    if (typeof v !== 'function') {
      throw (0, _rxError.newRxTypeError)('COL16', {
        name: k,
        type: typeof v
      });
    }
    if ((0, _entityProperties.rxCollectionProperties)().includes(k) || (0, _entityProperties.rxDocumentProperties)().includes(k) || additionalReservedNames && additionalReservedNames.includes(k)) {
      throw (0, _rxError.newRxError)('COL17', {
        name: k
      });
    }
  });
}
function checkOrmDocumentMethods(schema, methods) {
  var topLevelFields = Object.keys(schema.properties);
  if (!methods) {
    return;
  }

  /**
   * Build a set of all schema-generated property names
   * that will be defined on the document prototype.
   * For each field, the schema generates:
   * - field (value getter)
   * - field$ (observable getter)
   * - field$$ (reactivity getter)
   * - field_ (populate getter)
   */
  var schemaGeneratedNames = new Set();
  topLevelFields.forEach(field => {
    var f = field;
    schemaGeneratedNames.add(f);
    schemaGeneratedNames.add(f + '$');
    schemaGeneratedNames.add(f + '$$');
    schemaGeneratedNames.add(f + '_');
  });
  Object.keys(methods).filter(funName => schemaGeneratedNames.has(funName)).forEach(funName => {
    throw (0, _rxError.newRxError)('COL18', {
      funName
    });
  });
}
//# sourceMappingURL=check-orm.js.map