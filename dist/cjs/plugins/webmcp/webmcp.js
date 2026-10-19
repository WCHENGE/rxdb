"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.RxDBWebMCPPlugin = void 0;
exports.registerWebMCPCollection = registerWebMCPCollection;
exports.registerWebMCPDatabase = registerWebMCPDatabase;
var _rxjs = require("rxjs");
var _webmcpTarget = require("./webmcp-target.js");
var _webmcpTools = require("./webmcp-tools.js");
function registerWebMCPDatabase(options) {
  var database = this;
  var collections = this.collections;
  var error$ = new _rxjs.Subject();
  var log$ = new _rxjs.Subject();
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
function registerWebMCPCollection(options) {
  var target = (0, _webmcpTarget.getWebMCPTargetFromCollection)(this);
  var {
    error$,
    log$
  } = (0, _webmcpTools.registerWebMCPTarget)(target, options);
  return {
    error$,
    log$
  };
}
var RxDBWebMCPPlugin = exports.RxDBWebMCPPlugin = {
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