import { newRxError } from "../../rx-error.js";

/**
 * Parses the full revision.
 * Do NOT use this if you only need the revision height,
 * then use getHeightOfRevision() instead which is faster.
 */
export function parseRevision(revision) {
  var dashIndex = revision.indexOf('-');
  if (dashIndex === -1) {
    throw new Error('malformatted revision: ' + revision);
  }
  var height;
  if (dashIndex === 1) {
    height = revision.charCodeAt(0) - 48;
  } else {
    height = 0;
    for (var i = 0; i < dashIndex; i++) {
      height = height * 10 + (revision.charCodeAt(i) - 48);
    }
  }
  return {
    height,
    hash: revision.substring(dashIndex + 1)
  };
}

/**
 * @hotPath Performance is very important here
 * because we need to parse the revision height very often.
 * Uses indexOf + charCodeAt for maximum performance.
 * Single-digit heights (most common) use a fast path
 * that avoids parseInt entirely.
 */
export function getHeightOfRevision(revision) {
  var dashIndex = revision.indexOf('-');
  if (dashIndex === -1) {
    throw newRxError('SNH', {
      args: {
        revision
      }
    });
  }
  // Fast path for single-digit revision heights (most common case)
  if (dashIndex === 1) {
    return revision.charCodeAt(0) - 48;
  }
  // Manual number parsing for multi-digit heights (avoids parseInt + substring)
  var num = 0;
  for (var i = 0; i < dashIndex; i++) {
    num = num * 10 + (revision.charCodeAt(i) - 48);
  }
  return num;
}

/**
 * Creates the next write revision for a given document.
 */
export function createRevision(databaseInstanceToken, previousDocData) {
  var newRevisionHeight = !previousDocData ? 1 : getHeightOfRevision(previousDocData._rev) + 1;
  return newRevisionHeight + '-' + databaseInstanceToken;
}
//# sourceMappingURL=utils-revision.js.map