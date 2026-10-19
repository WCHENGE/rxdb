"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var _webmcp = require("./webmcp.js");
Object.keys(_webmcp).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _webmcp[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _webmcp[key];
    }
  });
});
var _webmcpTarget = require("./webmcp-target.js");
Object.keys(_webmcpTarget).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _webmcpTarget[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _webmcpTarget[key];
    }
  });
});
var _webmcpTools = require("./webmcp-tools.js");
Object.keys(_webmcpTools).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _webmcpTools[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _webmcpTools[key];
    }
  });
});
var _nosqlQuerySchema = require("./nosql-query-schema.js");
Object.keys(_nosqlQuerySchema).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _nosqlQuerySchema[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function () {
      return _nosqlQuerySchema[key];
    }
  });
});
//# sourceMappingURL=index.js.map