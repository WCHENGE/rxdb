import type { RxJsonSchema } from '../../types/index.js';
export declare function getValidator(schema: RxJsonSchema<any>): (docData: any) => any;
export declare const wrappedValidateIsMyJsonValidStorage: <Internals, InstanceCreationOptions>(args: {
    storage: import("../../index.js").RxStorage<Internals, InstanceCreationOptions>;
}) => import("../../index.js").RxStorage<Internals, InstanceCreationOptions>;
