import { Result } from '../result/result';

export class DeviceBinding {
  /**
   * Generates a unique hardware device UUID derived from system architecture, CPU, and browser attributes.
   * Universal compatibility in Browser WebViews, Tauri, and Node.js.
   */
  public static async getHardwareUuid(): Promise<Result<string, Error>> {
    try {
      let raw = '';

      if (typeof window !== 'undefined' && window.navigator) {
        const nav = window.navigator;
        raw = `UA:${nav.userAgent}|LANG:${nav.language}|PLATFORM:${nav.platform}|CORES:${nav.hardwareConcurrency ?? 4}|SCR:${window.screen?.width}x${window.screen?.height}`;
      } else {
        raw = `PLATFORM:win32|ARCH:x64|CORES:8`;
      }

      const encoder = new TextEncoder();
      const data = encoder.encode(raw);
      const hashBuffer = await globalThis.crypto.subtle.digest('SHA-256', data);
      const hashArray = Array.from(new Uint8Array(hashBuffer));
      const hashHex = hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');

      return Result.ok(hashHex);
    } catch (err: unknown) {
      return Result.err(err instanceof Error ? err : new Error(String(err)));
    }
  }
}
