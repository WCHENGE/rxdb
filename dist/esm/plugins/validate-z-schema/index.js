/**
 * this plugin validates documents before they can be inserted into the RxCollection.
 * It's using z-schema as jsonschema-validator
 * @link https://github.com/zaggino/z-schema
 */
import ZSchema from 'z-schema';
import { wrappedValidateStorageFactory } from "../../plugin-helpers.js";
export var ZSchemaClass = ZSchema;
var zSchema;
export function getZSchema() {
  if (!zSchema) {
    zSchema = ZSchema.create({
      strictMode: false
    });
  }
  return zSchema;
}
export function getValidator(schema) {
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
export var wrappedValidateZSchemaStorage = wrappedValidateStorageFactory(getValidator, 'z-schema');
//# sourceMappingURL=index.js.map