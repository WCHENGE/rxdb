/**
 * A minimal websocket client that reconnects with an
 * exponential backoff when the connection is lost.
 * Messages that are sent while the client is not connected,
 * are queued and flushed as soon as the connection is open again.
 *
 * This replaces the unmaintained 'reconnecting-websocket' npm module
 * which was the only dependency of the websocket replication.
 */
export type ReconnectingWebSocketOptions = {
    /**
     * The WebSocket implementation to use.
     * (optional, default: the global WebSocket)
     */
    WebSocket?: any;
    /**
     * Time in milliseconds to wait before the first reconnect attempt.
     * (optional, [default=1000])
     */
    minReconnectionDelay?: number;
    /**
     * Maximum time in milliseconds to wait between two reconnect attempts.
     * (optional, [default=10000])
     */
    maxReconnectionDelay?: number;
    /**
     * Factor the reconnection delay grows with on each failed attempt.
     * (optional, [default=1.3])
     */
    reconnectionDelayGrowFactor?: number;
    /**
     * Time in milliseconds a connection attempt has to open the connection.
     * When it takes longer, the attempt is aborted and retried.
     * (optional, [default=4000])
     */
    connectionTimeout?: number;
    /**
     * Time in milliseconds a connection has to stay open
     * before the retry counter is reset.
     * (optional, [default=5000])
     */
    minUptime?: number;
    /**
     * How many times the client tries to reconnect before it gives up.
     * (optional, [default=Infinity])
     */
    maxRetries?: number;
};
export declare class ReconnectingWebSocket {
    readonly url: string;
    readonly protocols: string | string[];
    readonly options: ReconnectingWebSocketOptions;
    onopen: ((event: any) => void) | null;
    onclose: ((event: any) => void) | null;
    onerror: ((event: any) => void) | null;
    onmessage: ((event: any) => void) | null;
    private socket;
    /**
     * Amount of connection attempts that were started
     * since the last working connection.
     * Is -1 before the first attempt so that the
     * initial connect does not have to wait.
     */
    private retryCount;
    private shouldReconnect;
    private isConnecting;
    private messageQueue;
    private connectionTimeoutId;
    private uptimeTimeoutId;
    private reconnectTimeoutId;
    constructor(url: string, protocols?: string | string[], options?: ReconnectingWebSocketOptions);
    get readyState(): number;
    /**
     * Sends the data to the server.
     * When the client is not connected, the data is queued
     * and sent as soon as the connection is open again.
     */
    send(data: any): void;
    /**
     * Closes the connection and stops reconnecting.
     */
    close(code?: number, reason?: string): void;
    private getOption;
    private getNextDelay;
    private connect;
    private handleOpen;
    private handleError;
    private handleClose;
    private removeHandlers;
    private clearTimeouts;
}
