/**
 * Parses a multipart/mixed response body from Google Drive Batch API.
 * Returns an array of objects containing { status, headers, body }.
 */
export declare function parseBatchResponse(body: string): {
    status: number;
    headers: Record<string, string>;
    body: unknown;
}[];
export declare function findDoubleNewline(str: string): number;
