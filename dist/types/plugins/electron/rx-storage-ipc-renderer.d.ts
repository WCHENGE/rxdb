import { RxStorageRemote, RxStorageRemoteSettings } from '../storage-remote/index.js';
export type RxStorageIpcRendererSettings = {
    /**
     * Set the same key on both sides
     * to ensure that messages do not get mixed
     * up when you use more than one storage.
     */
    key: string;
    ipcRenderer: any;
    mode: RxStorageRemoteSettings['mode'];
};
export type RxStorageIpcRenderer = RxStorageRemote;
export declare function getRxStorageIpcRenderer(settings: RxStorageIpcRendererSettings): RxStorageIpcRenderer;
