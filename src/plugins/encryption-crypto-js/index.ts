/**
 * this plugin adds the encryption-capabilities to rxdb
 *
 * It uses AES-256-GCM (authenticated encryption) via @noble/ciphers.
 * The module path keeps the historical `encryption-crypto-js` name so existing
 * imports stay valid; the crypto-js dependency itself is gone.
 */
import { gcm } from '@noble/ciphers/aes.js';
import { concatBytes, bytesToUtf8, utf8ToBytes } from '@noble/ciphers/utils.js';
import { sha256 } from '@noble/hashes/sha2.js';

import { wrapRxStorageInstance } from '../../plugin-helpers.ts';
import { newRxError, newRxTypeError } from '../../rx-error.ts';
import { hasEncryption } from '../../rx-storage-helper.ts';
import type {
    InternalStoreDocType,
    RxAttachmentWriteData,
    RxDocumentData,
    RxDocumentWriteData,
    RxJsonSchema,
    RxStorage,
    RxStorageInstanceCreationParams
} from '../../types/index.d.ts';
import {
    arrayBufferToBase64,
    base64ToArrayBuffer,
    clone,
    ensureNotFalsy,
    flatClone,
    getProperty,
    setProperty
} from '../../plugins/utils/index.ts';

export const MINIMUM_PASSWORD_LENGTH: 8 = 8;


/** 96-bit nonce, the size recommended for AES-GCM */
const NONCE_BYTES = 12;

function deriveKey(password: string): Uint8Array {
    // 32-byte AES-256 key. Password is hashed so any string length works.
    return sha256(utf8ToBytes(password));
}

function bytesToBase64(bytes: Uint8Array): string {
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
}

function base64ToBytes(text: string): Uint8Array {
    const binary = atob(text);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
}

/**
 * Encrypts to `base64(nonce ‖ ciphertext ‖ tag)`.
 *
 * GCM is authenticated: a tampered or truncated payload makes decryption throw
 * instead of silently returning garbage.
 */
export function encryptString(value: string, password: string): string {
    const nonce = crypto.getRandomValues(new Uint8Array(NONCE_BYTES));
    const ciphertext = gcm(deriveKey(password), nonce).encrypt(utf8ToBytes(value));
    return bytesToBase64(concatBytes(nonce, ciphertext));
}

export function decryptString(cipherText: string, password: any): string {
    /**
     * Trying to decrypt non-strings
     * will cause no errors and will be hard to debug.
     * So instead we do this check here.
     */
    if (typeof cipherText !== 'string') {
        throw newRxError('SNH', {
            args: {
                cipherText
            }
        });
    }

    const payload = base64ToBytes(cipherText);
    const plaintext = gcm(deriveKey(password), payload.slice(0, NONCE_BYTES))
        .decrypt(payload.slice(NONCE_BYTES));
    return bytesToUtf8(plaintext);
}

export type InternalStorePasswordDocType = InternalStoreDocType<{
    hash: string;
}>;

export function wrappedKeyEncryptionCryptoJsStorage<Internals, InstanceCreationOptions>(
    args: {
        storage: RxStorage<Internals, InstanceCreationOptions>;
    }
): RxStorage<Internals, InstanceCreationOptions> {
    return Object.assign(
        {},
        args.storage,
        {
            async createStorageInstance<RxDocType>(
                params: RxStorageInstanceCreationParams<RxDocType, any>
            ) {
                if (typeof params.password !== 'undefined') {
                    validatePassword(params.password as any);
                }

                if (!hasEncryption(params.schema)) {
                    const retInstance = await args.storage.createStorageInstance(params);
                    return retInstance;
                }

                if (!params.password) {
                    throw newRxError('EN3', {
                        database: params.databaseName,
                        collection: params.collectionName,
                        schema: params.schema
                    });
                }
                const password = params.password;

                const schemaWithoutEncrypted: RxJsonSchema<RxDocumentData<RxDocType>> = clone(params.schema);
                delete schemaWithoutEncrypted.encrypted;
                if (schemaWithoutEncrypted.attachments) {
                    schemaWithoutEncrypted.attachments.encrypted = false;
                }

                /**
                 * Encrypted data is always stored as string
                 * so we have to replace the schema definition of
                 * encrypted fields with just {type: 'string'}.
                 * All type-specific keywords (properties, required,
                 * items, maxLength, enum etc.) must be removed because
                 * they do not apply to the encrypted ciphertext string.
                 */
                const encryptedPaths = ensureNotFalsy(params.schema.encrypted);
                encryptedPaths.forEach(key => {
                    const pathParts = key.split('.');
                    if (pathParts.length === 1) {
                        (schemaWithoutEncrypted as any).properties[key] = { type: 'string' };
                    } else {
                        // Navigate nested schema structure: properties.a.properties.b.properties.c
                        let currentSchemaLevel: any = schemaWithoutEncrypted;
                        for (let i = 0; i < pathParts.length - 1; i++) {
                            currentSchemaLevel = currentSchemaLevel.properties[pathParts[i]];
                        }
                        currentSchemaLevel.properties[pathParts[pathParts.length - 1]] = { type: 'string' };
                    }
                });

                const instance = await args.storage.createStorageInstance(
                    Object.assign(
                        {},
                        params,
                        {
                            schema: schemaWithoutEncrypted
                        }
                    )
                );

                async function modifyToStorage(docData: RxDocumentWriteData<RxDocType>) {
                    docData = cloneWithoutAttachments(docData);
                    encryptedPaths
                        .forEach(path => {
                            const value = getProperty(docData, path);
                            if (typeof value === 'undefined') {
                                return;
                            }

                            const stringValue = JSON.stringify(value);
                            const encrypted = encryptString(stringValue, password);
                            setProperty(docData, path, encrypted);
                        });

                    // handle attachments
                    if (
                        params.schema.attachments &&
                        params.schema.attachments.encrypted
                    ) {
                        const newAttachments: typeof docData._attachments = {};
                        await Promise.all(
                            Object.entries(docData._attachments).map(async ([id, attachment]) => {
                                const useAttachment: RxAttachmentWriteData = flatClone(attachment) as any;
                                if (useAttachment.data) {
                                    const ab = await useAttachment.data.arrayBuffer();
                                    const base64 = arrayBufferToBase64(ab);
                                    const encrypted = encryptString(base64, password);
                                    useAttachment.data = new Blob([encrypted], { type: useAttachment.type });
                                }
                                newAttachments[id] = useAttachment;
                            })
                        );
                        docData._attachments = newAttachments;
                    }
                    return docData;
                }
                function modifyFromStorage(docData: RxDocumentData<any>): Promise<RxDocumentData<RxDocType>> {
                    docData = cloneWithoutAttachments(docData);
                    encryptedPaths
                        .forEach(path => {
                            const value = getProperty(docData, path);
                            if (typeof value === 'undefined') {
                                return;
                            }
                            const decrypted = decryptString(value, password);
                            const decryptedParsed = JSON.parse(decrypted);
                            setProperty(docData, path, decryptedParsed);
                        });
                    return docData;
                }

                async function modifyAttachmentFromStorage(attachmentData: Blob): Promise<Blob> {
                    if (
                        params.schema.attachments &&
                        params.schema.attachments.encrypted
                    ) {
                        const encryptedText = await attachmentData.text();
                        const decryptedBase64 = decryptString(encryptedText, password);
                        const ab = base64ToArrayBuffer(decryptedBase64);
                        return new Blob([ab], attachmentData.type ? { type: attachmentData.type } : undefined);
                    } else {
                        return attachmentData;
                    }
                }

                return wrapRxStorageInstance(
                    params.schema,
                    instance,
                    modifyToStorage,
                    modifyFromStorage,
                    modifyAttachmentFromStorage
                );
            }
        }
    );
}





function cloneWithoutAttachments<T>(data: RxDocumentWriteData<T>): RxDocumentData<T> {
    const attachments = data._attachments;
    data = flatClone(data);
    delete (data as any)._attachments;
    data = clone(data);
    data._attachments = attachments;
    return data as any;
}

function validatePassword(password: string) {
    if (typeof password !== 'string') {
        throw newRxTypeError('EN1', {
            passwordType: typeof password
        });
    }
    if (password.length < MINIMUM_PASSWORD_LENGTH) {
        throw newRxError('EN2', {
            minPassLength: MINIMUM_PASSWORD_LENGTH,
            passwordLength: password.length
        });
    }
}
