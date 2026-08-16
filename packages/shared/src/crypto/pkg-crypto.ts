import { Result } from '../result/result';

export interface EncryptedPackage {
  algorithm: 'AES-256-GCM';
  iv: string; // Hex
  authTag: string; // Hex
  encryptedData: string; // Hex
  signature: string; // Hex (HMAC-SHA256)
}

function bufferToHex(buffer: ArrayBuffer | Uint8Array): string {
  const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

function hexToBuffer(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substr(i * 2, 2), 16);
  }
  return bytes;
}

export class PackageCrypto {
  /**
   * Encrypts opcode JSON payload using AES-256-GCM & generates HMAC signature using WebCrypto.
   * Works seamlessly in Browser WebViews, Tauri, and Node.js.
   */
  public static async encryptAndSign(
    payloadJson: string,
    secretKeyHex: string
  ): Promise<Result<EncryptedPackage, Error>> {
    try {
      const webCrypto = globalThis.crypto;
      if (!webCrypto || !webCrypto.subtle) {
        throw new Error('WebCrypto API unavailable');
      }

      const rawKey = hexToBuffer(secretKeyHex.padEnd(64, '0').slice(0, 64));

      // Import key for AES-GCM
      const aesKey = await webCrypto.subtle.importKey(
        'raw',
        rawKey.buffer as ArrayBuffer,
        { name: 'AES-GCM' },
        false,
        ['encrypt']
      );

      const iv = webCrypto.getRandomValues(new Uint8Array(12));
      const encoder = new TextEncoder();
      const encodedPayload = encoder.encode(payloadJson);

      const encryptedBuffer = await webCrypto.subtle.encrypt(
        { name: 'AES-GCM', iv: iv.buffer as ArrayBuffer },
        aesKey,
        encodedPayload.buffer as ArrayBuffer
      );

      // AES-GCM output in WebCrypto includes tag at the end (last 16 bytes)
      const encryptedBytes = new Uint8Array(encryptedBuffer);
      const ciphertext = encryptedBytes.slice(0, encryptedBytes.length - 16);
      const authTag = encryptedBytes.slice(encryptedBytes.length - 16);

      // Generate signature via HMAC-SHA256
      const hmacKey = await webCrypto.subtle.importKey(
        'raw',
        rawKey.buffer as ArrayBuffer,
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign']
      );

      const signatureBuffer = await webCrypto.subtle.sign('HMAC', hmacKey, ciphertext.buffer as ArrayBuffer);

      return Result.ok({
        algorithm: 'AES-256-GCM',
        iv: bufferToHex(iv),
        authTag: bufferToHex(authTag),
        encryptedData: bufferToHex(ciphertext),
        signature: bufferToHex(signatureBuffer),
      });
    } catch (err: unknown) {
      return Result.err(err instanceof Error ? err : new Error(String(err)));
    }
  }

  /**
   * Verifies HMAC signature and decrypts AES-256-GCM package in memory only.
   */
  public static async verifyAndDecrypt(
    pkg: EncryptedPackage,
    secretKeyHex: string
  ): Promise<Result<string, Error>> {
    try {
      const webCrypto = globalThis.crypto;
      if (!webCrypto || !webCrypto.subtle) {
        throw new Error('WebCrypto API unavailable');
      }

      const rawKey = hexToBuffer(secretKeyHex.padEnd(64, '0').slice(0, 64));
      const ciphertext = hexToBuffer(pkg.encryptedData);
      const authTag = hexToBuffer(pkg.authTag);

      // 1. Verify HMAC Signature
      const hmacKey = await webCrypto.subtle.importKey(
        'raw',
        rawKey.buffer as ArrayBuffer,
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['verify']
      );

      const expectedSignature = hexToBuffer(pkg.signature);
      const isValid = await webCrypto.subtle.verify(
        'HMAC',
        hmacKey,
        expectedSignature.buffer as ArrayBuffer,
        ciphertext.buffer as ArrayBuffer
      );

      if (!isValid) {
        return Result.err(new Error('Invalid signature: Package has been tampered with or key is invalid'));
      }

      // 2. Decrypt AES-GCM
      const aesKey = await webCrypto.subtle.importKey(
        'raw',
        rawKey.buffer as ArrayBuffer,
        { name: 'AES-GCM' },
        false,
        ['decrypt']
      );

      // Combine ciphertext + authTag for WebCrypto format
      const combinedBuffer = new Uint8Array(ciphertext.length + authTag.length);
      combinedBuffer.set(ciphertext, 0);
      combinedBuffer.set(authTag, ciphertext.length);

      const iv = hexToBuffer(pkg.iv);
      const decryptedBuffer = await webCrypto.subtle.decrypt(
        { name: 'AES-GCM', iv: iv.buffer as ArrayBuffer },
        aesKey,
        combinedBuffer.buffer as ArrayBuffer
      );

      const decoder = new TextDecoder();
      return Result.ok(decoder.decode(decryptedBuffer));
    } catch (err: unknown) {
      return Result.err(err instanceof Error ? err : new Error(String(err)));
    }
  }
}
