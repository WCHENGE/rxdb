import { Subject } from 'rxjs';
import { getWebMCPTargetFromCollection } from "./webmcp-target.js";
import { registerWebMCPTarget } from "./webmcp-tools.js";
export function registerWebMCPDatabase(options) {
  var database = this;
  var collections = this.collections;
  var error$ = new Subject();
  var log$ = new Subject();
  var registerCollection = collection => {
    var res = collection.registerWebMCP(options);
    res.error$.subscribe(error$);
    res.log$.subscribe(log$);
  };

  // Register existing collections
  for (var [name, collection] of Object.entries(collections)) {
    registerCollection(collection);
  }

  // Store options and subjects on the database instance so the hook can pick them up dynamically
  database._webmcpOptions = options || {};
  database._webmcpError$ = error$;
  database._webmcpLog$ = log$;

  // We should probably tear this down if the database is destroyed... For now it's okay.
  return {
    error$,
    log$
  };
}
export function registerWebMCPCollection(options) {
  var target = getWebMCPTargetFromCollection(this);
  var {
    error$,
    log$
  } = registerWebMCPTarget(target, options);
  return {
    error$,
    log$
  };
}
export var RxDBWebMCPPlugin = {
  name: 'webmcp',
  rxdb: true,
  prototypes: {
    RxDatabase: proto => {
      proto.registerWebMCP = registerWebMCPDatabase;
    },
    RxCollection: proto => {
      proto.registerWebMCP = registerWebMCPCollection;
    }
  },
  hooks: {
    createRxCollection: {
      after: ({
        collection
      }) => {
        var db = collection.database;
        if (db._webmcpOptions) {
          var res = collection.registerWebMCP(db._webmcpOptions);
          if (db._webmcpError$) {
            res.error$.subscribe(db._webmcpError$);
          }
          if (db._webmcpLog$) {
            res.log$.subscribe(db._webmcpLog$);
          }
        }
      }
    }
  }
};
//# sourceMappingURL=webmcp.js.map