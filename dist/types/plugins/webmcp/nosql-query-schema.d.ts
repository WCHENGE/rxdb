export declare const NOSQL_QUERY_JSON_SCHEMA: {
    $schema: string;
    title: string;
    description: string;
    type: string;
    additionalProperties: boolean;
    required: string[];
    properties: {
        selector: {
            $ref: string;
        };
        sort: {
            $ref: string;
        };
        limit: {
            type: string;
            minimum: number;
            examples: number[];
        };
        skip: {
            type: string;
            minimum: number;
            examples: number[];
        };
    };
    examples: ({
        selector: {
            age: {
                $gt: number;
            };
            status: string;
            $or?: undefined;
            "address.country"?: undefined;
        };
        sort: {
            createdAt: string;
        }[];
        limit: number;
    } | {
        selector: {
            $or: ({
                "company.name": {
                    $regex: string;
                    $options: string;
                };
                tags?: undefined;
            } | {
                tags: {
                    $in: string[];
                };
                "company.name"?: undefined;
            })[];
            "address.country": string;
            age?: undefined;
            status?: undefined;
        };
        limit: number;
        sort?: undefined;
    })[];
    default: {
        selector: {};
        sort: never[];
        limit: number;
        skip: number;
    };
    $defs: {
        sort: {
            description: string;
            type: string;
            items: {
                type: string;
                minProperties: number;
                additionalProperties: boolean;
                patternProperties: {
                    "^(?!\\$).+$": {
                        type: string;
                        enum: string[];
                    };
                };
            };
            examples: ({
                lastName: string;
                createdAt?: undefined;
            } | {
                createdAt: string;
                lastName?: undefined;
            })[][];
        };
        expression: {
            anyOf: {
                $ref: string;
            }[];
        };
        logicalExpression: {
            type: string;
            additionalProperties: boolean;
            properties: {
                $and: {
                    type: string;
                    minItems: number;
                    items: {
                        $ref: string;
                    };
                    examples: ({
                        status: string;
                        age?: undefined;
                    } | {
                        age: {
                            $gte: number;
                        };
                        status?: undefined;
                    })[][];
                };
                $or: {
                    type: string;
                    minItems: number;
                    items: {
                        $ref: string;
                    };
                    examples: {
                        role: string;
                    }[][];
                };
                $nor: {
                    type: string;
                    minItems: number;
                    items: {
                        $ref: string;
                    };
                    examples: ({
                        archived: boolean;
                        deleted?: undefined;
                    } | {
                        deleted: boolean;
                        archived?: undefined;
                    })[][];
                };
                $not: {
                    $ref: string;
                    examples: ({
                        age: {
                            $lt: number;
                        };
                        name?: undefined;
                    } | {
                        name: {
                            $regex: string;
                        };
                        age?: undefined;
                    })[];
                };
            };
            anyOf: {
                required: string[];
            }[];
            examples: ({
                $and: ({
                    status: string;
                    age?: undefined;
                } | {
                    age: {
                        $gte: number;
                    };
                    status?: undefined;
                })[];
                $or?: undefined;
                $nor?: undefined;
                $not?: undefined;
            } | {
                $or: ({
                    tier: string;
                    trial?: undefined;
                } | {
                    trial: boolean;
                    tier?: undefined;
                })[];
                $and?: undefined;
                $nor?: undefined;
                $not?: undefined;
            } | {
                $nor: ({
                    deleted: boolean;
                    blocked?: undefined;
                } | {
                    blocked: boolean;
                    deleted?: undefined;
                })[];
                $and?: undefined;
                $or?: undefined;
                $not?: undefined;
            } | {
                $not: {
                    age: {
                        $gte: number;
                    };
                };
                $and?: undefined;
                $or?: undefined;
                $nor?: undefined;
            })[];
        };
        fieldExpression: {
            type: string;
            minProperties: number;
            additionalProperties: boolean;
            patternProperties: {
                "^(?!\\$).+$": {
                    anyOf: {
                        $ref: string;
                    }[];
                };
            };
            examples: ({
                status: string;
                age?: undefined;
                tags?: undefined;
            } | {
                age: {
                    $gte: number;
                };
                status?: undefined;
                tags?: undefined;
            } | {
                tags: {
                    $in: string[];
                };
                status?: undefined;
                age?: undefined;
            })[];
        };
        fieldOperatorExpression: {
            type: string;
            minProperties: number;
            additionalProperties: boolean;
            properties: {
                $eq: {
                    $ref: string;
                    examples: (string | number | null)[];
                };
                $ne: {
                    $ref: string;
                    examples: (string | number)[];
                };
                $gt: {
                    $ref: string;
                    examples: number[];
                };
                $gte: {
                    $ref: string;
                    examples: number[];
                };
                $lt: {
                    $ref: string;
                    examples: number[];
                };
                $lte: {
                    $ref: string;
                    examples: number[];
                };
                $in: {
                    type: string;
                    items: {
                        $ref: string;
                    };
                    examples: (number[] | string[])[];
                };
                $nin: {
                    type: string;
                    items: {
                        $ref: string;
                    };
                    examples: (number[] | string[])[];
                };
                $exists: {
                    type: string;
                    examples: boolean[];
                };
                $regex: {
                    type: string;
                    examples: string[];
                };
                $options: {
                    type: string;
                    examples: string[];
                };
                $type: {
                    oneOf: ({
                        type: string;
                        items?: undefined;
                    } | {
                        type: string;
                        items: {
                            oneOf: {
                                type: string;
                            }[];
                        };
                    })[];
                    examples: (string | number | string[])[];
                };
                $size: {
                    type: string;
                    minimum: number;
                    examples: number[];
                };
                $mod: {
                    type: string;
                    minItems: number;
                    maxItems: number;
                    items: {
                        type: string;
                    };
                    examples: number[][];
                };
                $elemMatch: {
                    $ref: string;
                    examples: ({
                        qty: {
                            $gte: number;
                        };
                        type?: undefined;
                    } | {
                        type: string;
                        qty?: undefined;
                    })[];
                };
            };
            examples: ({
                $eq: string;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $ne: string;
                $eq?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $gt: number;
                $eq?: undefined;
                $ne?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $gte: number;
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $lt: number;
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $lte: number;
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $in: string[];
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $nin: string[];
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $exists: boolean;
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $regex: string;
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $type: string;
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $size?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $size: number;
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $mod?: undefined;
                $elemMatch?: undefined;
            } | {
                $mod: number[];
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $elemMatch?: undefined;
            } | {
                $elemMatch: {
                    qty: {
                        $gte: number;
                    };
                };
                $eq?: undefined;
                $ne?: undefined;
                $gt?: undefined;
                $gte?: undefined;
                $lt?: undefined;
                $lte?: undefined;
                $in?: undefined;
                $nin?: undefined;
                $exists?: undefined;
                $regex?: undefined;
                $type?: undefined;
                $size?: undefined;
                $mod?: undefined;
            })[];
        };
        literal: {
            oneOf: {
                type: string;
            }[];
        };
    };
};
