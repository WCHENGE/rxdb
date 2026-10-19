/**
 * this plugin validates documents before they can be inserted into the RxCollection.
 * It's using z-schema as jsonschema-validator
 * @link https://github.com/zaggino/z-schema
 */
import ZSchema from 'z-schema';
import type { RxJsonSchema } from '../../types/index.js';
export declare const ZSchemaClass: typeof ZSchema;
export declare function getZSchema(): ZSchema;
export declare function getValidator(schema: RxJsonSchema<any>): (docData: any) => any;
export declare const wrappedValidateZSchemaStorage: <Internals, InstanceCreationOptions>(args: {
    storage: import("../../index.js").RxStorage<Internals, InstanceCreationOptions>;
}) => import("../../index.js").RxStorage<Internals, InstanceCreationOptions>;
