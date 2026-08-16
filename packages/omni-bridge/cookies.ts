import type { RpcClient } from './rpc';

// ── Bridge cookie shape (CDP / Playwright-style) ─────────────────

export interface Cookie {
  name: string;
  value: string;
  domain?: string;
  path?: string;
  url?: string;
  httpOnly?: boolean;
  secure?: boolean;
  /** 'Strict' | 'Lax' | 'None'. Bridge accepts any string and forwards verbatim. */
  sameSite?: string;
  /** Unix seconds. -1 (or omit) = session cookie. */
  expires?: number;
}

export interface OriginStorage {
  origin: string;
  localStorage: Array<{ name: string; value: string }>;
}

export interface StorageState {
  cookies: Cookie[];
  origins: OriginStorage[];
}

// ── Multi-format import helpers ──────────────────────────────────

/** Recognised cookie source formats. */
export const CookieFormat = Object.freeze({
  /** Playwright/Puppeteer + bridge native — already correct shape. */
  Bridge: 'bridge',
  /** EditThisCookie / Cookie-Editor / extension export — uses `expirationDate` and lowercase sameSite. */
  Extension: 'extension',
  /** Netscape cookies.txt format (one cookie per line). */
  Netscape: 'netscape',
  /** Browser `document.cookie` style: `k=v; k2=v2`. No metadata. */
  Header: 'header',
} as const);
export type CookieFormat = (typeof CookieFormat)[keyof typeof CookieFormat];

interface ExtensionCookie {
  name: string;
  value: string;
  domain: string;
  path: string;
  expirationDate?: number;
  httpOnly?: boolean;
  hostOnly?: boolean;
  secure?: boolean;
  session?: boolean;
  /** 'no_restriction' | 'lax' | 'strict' | 'unspecified' (extension format). */
  sameSite?: 'no_restriction' | 'lax' | 'strict' | 'unspecified';
  storeId?: string;
}

/** Title-case the extension's `sameSite` to bridge form. */
function normaliseSameSite(s?: string): string | undefined {
  if (!s) return undefined;
  const lower = s.toLowerCase();
  if (lower === 'no_restriction' || lower === 'none') return 'None';
  if (lower === 'lax') return 'Lax';
  if (lower === 'strict') return 'Strict';
  if (lower === 'unspecified') return undefined;
  // Already title-case ('Lax', 'Strict', 'None') — pass through.
  return s;
}

function parseExtensionCookie(c: ExtensionCookie): Cookie {
  return {
    name: c.name,
    value: c.value,
    domain: c.domain,
    path: c.path ?? '/',
    httpOnly: c.httpOnly,
    secure: c.secure,
    sameSite: normaliseSameSite(c.sameSite),
    expires: c.expirationDate,
  };
}

/** Parse a Netscape cookies.txt line: `domain<TAB>flag<TAB>path<TAB>secure<TAB>expiry<TAB>name<TAB>value`. */
function parseNetscapeLine(line: string): Cookie | null {
  if (!line || line.startsWith('#')) return null;
  const parts = line.split('\t');
  if (parts.length < 7) return null;
  const [domain, , path, secure, expiry, name, value] = parts;
  return {
    name,
    value,
    domain,
    path: path || '/',
    secure: secure.toUpperCase() === 'TRUE',
    expires: parseInt(expiry, 10) || -1,
  };
}

function parseHeaderCookies(text: string, defaults?: { domain?: string; path?: string }): Cookie[] {
  return text.split(';').map(pair => pair.trim()).filter(Boolean).map(pair => {
    const eq = pair.indexOf('=');
    if (eq < 0) return null;
    return {
      name: pair.slice(0, eq).trim(),
      value: pair.slice(eq + 1).trim(),
      domain: defaults?.domain,
      path: defaults?.path ?? '/',
    } as Cookie;
  }).filter((c): c is Cookie => !!c);
}

/**
 * Parse cookies from any common format into bridge-native `Cookie[]`.
 *
 * Supported inputs:
 * - Already an array of bridge `Cookie` objects (passes through).
 * - JSON string of an array (Playwright/Puppeteer/extension export).
 * - Single cookie object (wrapped to an array).
 * - `{ cookies: [...] }` wrapper (e.g. Playwright `storageState` partial).
 * - Raw Netscape cookies.txt (multiline, tab-delimited).
 * - HTTP `Cookie:` header / `document.cookie` (`k=v; k2=v2`).
 *
 * Pass `defaults` to fill in `domain` / `path` for the header format (which
 * doesn't carry that metadata).
 *
 * Returns `[]` on unparseable input — never throws.
 */
export function parseCookies(
  input: string | unknown,
  opts?: { format?: CookieFormat; defaults?: { domain?: string; path?: string } },
): Cookie[] {
  try {
    if (Array.isArray(input)) {
      return input.map(c => normaliseCookie(c as Cookie | ExtensionCookie));
    }

    if (input && typeof input === 'object') {
      const obj = input as { cookies?: unknown[]; name?: string; value?: string };
      if (Array.isArray(obj.cookies)) {
        return obj.cookies.map(c => normaliseCookie(c as Cookie | ExtensionCookie));
      }
      if (obj.name && obj.value !== undefined) {
        return [normaliseCookie(input as Cookie | ExtensionCookie)];
      }
      return [];
    }

    if (typeof input !== 'string') return [];
    const trimmed = input.trim();
    if (!trimmed) return [];

    const fmt = opts?.format;

    // Explicit format
    if (fmt === CookieFormat.Netscape) {
      return trimmed.split(/\r?\n/).map(parseNetscapeLine).filter((c): c is Cookie => !!c);
    }
    if (fmt === CookieFormat.Header) {
      return parseHeaderCookies(trimmed, opts?.defaults);
    }

    // Auto-detect
    if (trimmed.startsWith('[') || trimmed.startsWith('{')) {
      return parseCookies(JSON.parse(trimmed), opts);
    }
    if (trimmed.includes('\t') && /^[^\t]+\t/.test(trimmed)) {
      return trimmed.split(/\r?\n/).map(parseNetscapeLine).filter((c): c is Cookie => !!c);
    }
    if (trimmed.includes('=')) {
      return parseHeaderCookies(trimmed, opts?.defaults);
    }
    return [];
  } catch {
    return [];
  }
}

function normaliseCookie(raw: Cookie | ExtensionCookie): Cookie {
  // Extension format → bridge format.
  if ('expirationDate' in raw || (raw.sameSite && /^(no_restriction|lax|strict|unspecified)$/i.test(raw.sameSite))) {
    return parseExtensionCookie(raw as ExtensionCookie);
  }
  // Already bridge-shaped — strip nullish path/domain to keep payload clean.
  const c = raw as Cookie;
  return {
    name: c.name,
    value: c.value,
    domain: c.domain,
    path: c.path ?? '/',
    url: c.url,
    httpOnly: c.httpOnly,
    secure: c.secure,
    sameSite: normaliseSameSite(c.sameSite),
    expires: c.expires,
  };
}

/** Convert a bridge cookie array back to plain Netscape `cookies.txt` format. */
export function toNetscape(cookies: Cookie[]): string {
  const lines = ['# Netscape HTTP Cookie File'];
  for (const c of cookies) {
    if (!c.domain) continue;
    const flag = c.domain.startsWith('.') ? 'TRUE' : 'FALSE';
    const expiry = c.expires && c.expires > 0 ? Math.floor(c.expires) : 0;
    lines.push([
      c.domain,
      flag,
      c.path ?? '/',
      c.secure ? 'TRUE' : 'FALSE',
      String(expiry),
      c.name,
      c.value,
    ].join('\t'));
  }
  return lines.join('\n') + '\n';
}

/** Alias of `parseCookies` — convenience name when the intent is "send to bridge". */
export const toBridgeCookies = parseCookies;

// ── Cookies service ──────────────────────────────────────────────

/** Cookie + storage management for the browser session. */
export class Cookies {
  constructor(private rpc: RpcClient) {}

  async list(urls?: string[]) {
    return this.rpc.call<Cookie[]>('context.cookies', { urls });
  }

  /**
   * Add cookies to the browser. Accepts native `Cookie[]` (recommended) or
   * any source format via `parseCookies()` first:
   *
   * ```ts
   * import { parseCookies } from '@omnilogin/sdk';
   *
   * await session.cookies.add(parseCookies(stringFromExportFile, {
   *   defaults: { domain: '.example.com' }
   * }));
   * ```
   */
  async add(cookies: Cookie[]) {
    return this.rpc.call('context.addCookies', { cookies });
  }

  async clear() {
    return this.rpc.call('context.clearCookies');
  }

  async getStorageState() {
    return this.rpc.call<StorageState>('context.storageState');
  }

  async setStorageState(state: { cookies?: Cookie[]; origins?: OriginStorage[] }) {
    return this.rpc.call('context.setStorageState', state);
  }

  /** Convenience: export current cookies as Netscape `cookies.txt`. */
  async exportNetscape(urls?: string[]): Promise<string> {
    const all = await this.list(urls);
    return toNetscape(all);
  }
}
