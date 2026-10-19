import type { WebMCPOptions, WebMCPLogEvent, WebMCPTarget, WebMCPTool } from '../../types/plugins/webmcp.js';
import { Subject } from 'rxjs';
export declare const WEBMCP_ERROR_DOCS_HINT = "Note: If this tool returns an error code, you can find the decoded error message at https://rxdb.info/errors.html";
/**
 * Resolves the WebMCP registry to register the tools at.
 * The specification moved the entrypoint from navigator to document,
 * so both are checked. Passing options.modelContext explicitly is
 * required when the tools must be registered at a registry of
 * another document, like inside an iframe or a devtools panel.
 */
export declare function getModelContext(options?: WebMCPOptions): any;
/**
 * Builds the WebMCP tool definitions for a given target.
 * The returned tools only talk to the target, so the same
 * definitions work for a local RxCollection and for a
 * collection that lives in another process or on another device.
 */
export declare function getWebMCPTools(target: WebMCPTarget, options?: WebMCPOptions, log$?: Subject<WebMCPLogEvent>, error$?: Subject<Error>): WebMCPTool[];
/**
 * Registers the WebMCP tools of a target at the model context
 * and returns a function that unregisters them again.
 */
export declare function registerWebMCPTarget(target: WebMCPTarget, options?: WebMCPOptions): {
    error$: Subject<Error>;
    log$: Subject<WebMCPLogEvent>;
    unregister: () => void;
};
