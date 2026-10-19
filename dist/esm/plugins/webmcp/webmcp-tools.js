import { Subject } from 'rxjs';
import { newRxError } from "../../rx-error.js";
import { NOSQL_QUERY_JSON_SCHEMA } from "./nosql-query-schema.js";
export var WEBMCP_ERROR_DOCS_HINT = 'Note: If this tool returns an error code, you can find the decoded error message at https://rxdb.info/errors.html';

/**
 * Resolves the WebMCP registry to register the tools at.
 * The specification moved the entrypoint from navigator to document,
 * so both are checked. Passing options.modelContext explicitly is
 * required when the tools must be registered at a registry of
 * another document, like inside an iframe or a devtools panel.
 */
export function getModelContext(options) {
  if (options && options.modelContext) {
    return options.modelContext;
  }
  if (typeof document !== 'undefined' && document.modelContext) {
    return document.modelContext;
  }
  if (typeof navigator !== 'undefined' && navigator.modelContext) {
    return navigator.modelContext;
  }
  return undefined;
}

/**
 * Builds the WebMCP tool definitions for a given target.
 * The returned tools only talk to the target, so the same
 * definitions work for a local RxCollection and for a
 * collection that lives in another process or on another device.
 */
export function getWebMCPTools(target, options, log$, error$) {
  var toolNameSuffix = target.databaseName + "_" + target.collectionName + "_" + target.schemaVersion;
  var schemaString = JSON.stringify(target.jsonSchema);
  var withMiddleware = (toolName, fn) => {
    return async (args, context) => {
      try {
        var result = await fn(args, context);
        if (log$) {
          log$.next({
            collectionName: target.collectionName,
            databaseName: target.databaseName,
            toolName,
            args,
            result
          });
        }
        return result;
      } catch (err) {
        if (error$) {
          error$.next(err);
        }
        if (log$) {
          log$.next({
            collectionName: target.collectionName,
            databaseName: target.databaseName,
            toolName,
            args,
            error: err
          });
        }
        throw err;
      }
    };
  };
  var awaitSyncIfRequired = async () => {
    if (options?.awaitReplicationsInSync !== false) {
      await target.awaitInSync();
    }
  };
  var queryInputSchema = () => ({
    type: 'object',
    $defs: NOSQL_QUERY_JSON_SCHEMA.$defs,
    properties: {
      query: Object.assign({}, NOSQL_QUERY_JSON_SCHEMA, {
        $defs: undefined,
        default: {
          sort: [{
            [target.primaryPath]: 'asc'
          }]
        }
      })
    },
    required: ['query']
  });
  var documentInputSchema = description => ({
    type: 'object',
    properties: {
      document: Object.assign({}, JSON.parse(schemaString), {
        description
      })
    },
    required: ['document']
  });
  var tools = [];
  var queryToolName = "rxdb_query_" + toolNameSuffix;
  tools.push({
    name: queryToolName,
    description: "Query the RxDB collection '" + target.collectionName + "' of database '" + target.databaseName + "'. Allows filtering, sorting, and pagination. Returns an array of matched document objects. The collection has the following JSON schema: " + schemaString + ". " + WEBMCP_ERROR_DOCS_HINT,
    annotations: {
      readOnlyHint: true
    },
    inputSchema: queryInputSchema(),
    execute: withMiddleware(queryToolName, async args => {
      await awaitSyncIfRequired();
      return await target.query(args.query);
    })
  });
  var countToolName = "rxdb_count_" + toolNameSuffix;
  tools.push({
    name: countToolName,
    description: "Counts the documents in the RxDB collection '" + target.collectionName + "' of database '" + target.databaseName + "' matching a given query. The collection has the following JSON schema: " + schemaString + ". " + WEBMCP_ERROR_DOCS_HINT,
    annotations: {
      readOnlyHint: true
    },
    inputSchema: queryInputSchema(),
    execute: withMiddleware(countToolName, async args => {
      await awaitSyncIfRequired();
      var count = await target.count(args.query);
      return {
        count
      };
    })
  });
  var changesToolName = "rxdb_changes_" + toolNameSuffix;
  tools.push({
    name: changesToolName,
    description: "Returns all changes of the RxDB collection '" + target.collectionName + "' of database '" + target.databaseName + "' since a given checkpoint. If no checkpoint is provided, starts from the oldest change. The collection has the following JSON schema: " + schemaString + ". " + WEBMCP_ERROR_DOCS_HINT,
    annotations: {
      readOnlyHint: true
    },
    inputSchema: {
      type: 'object',
      properties: {
        checkpoint: {
          type: 'object',
          description: 'The cursor/checkpoint to start fetching changes from. Leave empty to start from the beginning.'
        },
        limit: {
          type: 'number',
          description: 'Maximum number of changes to return.',
          default: 50
        }
      }
    },
    execute: withMiddleware(changesToolName, async args => {
      await awaitSyncIfRequired();
      var limit = args.limit || 50;
      var changes = await target.changesSince(limit, args.checkpoint);
      return {
        documents: changes.documents.map(doc => {
          var cleaned = Object.assign({}, doc);
          delete cleaned._meta;
          delete cleaned._rev;
          delete cleaned._attachments;
          delete cleaned._deleted;
          return cleaned;
        }),
        checkpoint: changes.checkpoint
      };
    })
  });
  var waitChangesToolName = "rxdb_wait_changes_" + toolNameSuffix;
  tools.push({
    name: waitChangesToolName,
    description: "Waits until a new write event happens to the RxDB collection '" + target.collectionName + "' of database '" + target.databaseName + "'. Returns a promise that resolves when a change occurs. " + WEBMCP_ERROR_DOCS_HINT,
    annotations: {
      readOnlyHint: true
    },
    inputSchema: {
      type: 'object',
      properties: {}
    },
    execute: withMiddleware(waitChangesToolName, async () => {
      await target.awaitChange();
      return {
        success: true,
        message: 'A write event occurred in the collection.'
      };
    })
  });
  if (options?.readOnly === true) {
    return tools;
  }
  var insertToolName = "rxdb_insert_" + toolNameSuffix;
  tools.push({
    name: insertToolName,
    description: "Insert a document into the RxDB collection '" + target.collectionName + "' of database '" + target.databaseName + "'. The collection has the following JSON schema: " + schemaString + ". " + WEBMCP_ERROR_DOCS_HINT,
    inputSchema: documentInputSchema('The document to insert.'),
    execute: withMiddleware(insertToolName, async args => {
      await awaitSyncIfRequired();
      return await target.insert(args.document);
    })
  });
  var upsertToolName = "rxdb_upsert_" + toolNameSuffix;
  tools.push({
    name: upsertToolName,
    description: "Upsert a document into the RxDB collection '" + target.collectionName + "' of database '" + target.databaseName + "'. If a document with the same primary key exists, it will be overwritten. The collection has the following JSON schema: " + schemaString + ". " + WEBMCP_ERROR_DOCS_HINT,
    inputSchema: documentInputSchema('The document to upsert.'),
    execute: withMiddleware(upsertToolName, async args => {
      await awaitSyncIfRequired();
      return await target.upsert(args.document);
    })
  });
  var deleteToolName = "rxdb_delete_" + toolNameSuffix;
  tools.push({
    name: deleteToolName,
    description: "Deletes a document by id from the RxDB collection '" + target.collectionName + "' of database '" + target.databaseName + "'. The collection has the following JSON schema: " + schemaString + ". " + WEBMCP_ERROR_DOCS_HINT,
    inputSchema: {
      type: 'object',
      properties: {
        id: {
          type: 'string',
          description: 'The primary key of the document to delete.'
        }
      },
      required: ['id']
    },
    execute: withMiddleware(deleteToolName, async args => {
      await awaitSyncIfRequired();
      var deletedDoc = await target.remove(args.id);
      if (!deletedDoc) {
        throw newRxError('WMCP1', {
          documentId: args.id
        });
      }
      return deletedDoc;
    })
  });
  return tools;
}

/**
 * Registers the WebMCP tools of a target at the model context
 * and returns a function that unregisters them again.
 */
export function registerWebMCPTarget(target, options) {
  var error$ = new Subject();
  var log$ = new Subject();
  var modelContext = getModelContext(options);
  if (!modelContext) {
    return {
      error$,
      log$,
      unregister: () => {}
    };
  }
  var tools = getWebMCPTools(target, options, log$, error$);

  /**
   * Tools are unregistered by aborting the signal they were registered with.
   * Registries that do not support signals are served by unregisterTool().
   */
  var controller = typeof AbortController !== 'undefined' ? new AbortController() : undefined;
  tools.forEach(tool => {
    var registration = modelContext.registerTool(tool, controller ? {
      signal: controller.signal
    } : undefined);
    /**
     * Spec-conformant registries return a promise that rejects
     * with the abort reason when the tool is unregistered
     * before the registration has finished.
     */
    if (registration && typeof registration.then === 'function') {
      registration.then(undefined, err => {
        if (controller && controller.signal.aborted && err === controller.signal.reason) {
          return;
        }
        error$.next(err);
      });
    }
  });
  var unregister = () => {
    if (controller) {
      controller.abort();
      return;
    }
    tools.forEach(tool => {
      try {
        if (modelContext.unregisterTool) {
          modelContext.unregisterTool(tool.name);
        }
      } catch (err) {}
    });
  };
  target.onClose(unregister);
  return {
    error$,
    log$,
    unregister
  };
}
//# sourceMappingURL=webmcp-tools.js.map