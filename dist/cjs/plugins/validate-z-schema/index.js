"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault").default;
Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ZSchemaClass = void 0;
exports.getValidator = getValidator;
exports.getZSchema = getZSchema;
exports.wrappedValidateZSchemaStorage = void 0;
var _zSchema = _interopRequireDefault(require("z-schema"));
var _pluginHelpers = require("../../plugin-helpers.js");
/**
 * this plugin validates documents before they can be inserted into the RxCollection.
 * It's using z-schema as jsonschema-validator
 * @link https://github.com/zaggino/z-schema
 */

var ZSchemaClass = exports.ZSchemaClass = _zSchema.default;
var zSchema;
function getZSchema() {
  if (!zSchema) {
    zSchema = _zSchema.default.create({
      strictMode: false
    });
  }
  return zSchema;
}
function getValidator(schema) {
  var validator = obj => {
    return getZSchema().validateSafe(obj, schema);
  };
  return docData => {
    var useValidator = validator(docData);
    if (useValidator.valid) {
      return [];
    }
    var errors = useValidator.error?.details || useValidator.err?.details || [];
    if (errors) {
      var formattedZSchemaErrors = errors.map(({
        title,
        description,
        message,
        path
      }) => ({
        title,
        description,
        message,
        path
      }));
      return formattedZSchemaErrors;
    } else {
      return [];
    }
  };
}
var wrappedValidateZSchemaStorage = exports.wrappedValidateZSchemaStorage = (0, _pluginHelpers.wrappedValidateStorageFactory)(getValidator, 'z-schema');
//# sourceMappingURL=index.js.map