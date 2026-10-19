"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.findDoubleNewline = findDoubleNewline;
exports.parseBatchResponse = parseBatchResponse;
/**
 * Parses a multipart/mixed response body from Google Drive Batch API.
 * Returns an array of objects containing { status, headers, body }.
 */
function parseBatchResponse(body) {
  var boundary = detectBoundary(body);
  if (boundary.startsWith('"') && boundary.endsWith('"')) {
    boundary = boundary.substring(1, boundary.length - 1);
  }
  var parts = body.split("--" + boundary);
  var results = [];
  for (var part of parts) {
    var trimmedPart = part.trim();
    if (!trimmedPart || trimmedPart === '--') continue;
    var outerBodyStart = findDoubleNewline(trimmedPart);
    if (outerBodyStart === -1) continue;
    var outerBody = trimmedPart.substring(outerBodyStart).trim();
    var lines = outerBody.split(/\r?\n/);
    var statusLine = lines[0];
    var statusCode = parseInt(statusLine.split(' ')[1], 10);

    // Parse inner headers
    var innerHeaders = {};
    for (var i = 1; i < lines.length; i++) {
      var line = lines[i];
      if (line.trim() === '') {
        break;
      }
      var [key, ...val] = line.split(':');
      if (key) innerHeaders[key.toLowerCase().trim()] = val.join(':').trim();
    }
    var innerBodyStart = findDoubleNewline(outerBody);
    var parsedBody = null;
    if (innerBodyStart !== -1) {
      var contentBody = outerBody.substring(innerBodyStart).trim();
      if (contentBody) {
        try {
          parsedBody = JSON.parse(contentBody);
        } catch {
          parsedBody = contentBody;
        }
      }
    }
    results.push({
      status: statusCode,
      headers: innerHeaders,
      body: parsedBody
    });
  }
  return results;
}
function findDoubleNewline(str) {
  var crlf = str.indexOf('\r\n\r\n');
  if (crlf !== -1) return crlf + 4;
  var lf = str.indexOf('\n\n');
  if (lf !== -1) return lf + 2;
  return -1;
}
function detectBoundary(content) {
  var newlineIndex = content.indexOf('\n');
  var firstLine = newlineIndex === -1 ? content : content.slice(0, newlineIndex);
  if (firstLine.startsWith('--')) {
    return firstLine.slice(2).trim();
  }
  throw new Error('Could not detect boundary from response body: ' + content.slice(0, 100));
}
//# sourceMappingURL=multipart.js.map