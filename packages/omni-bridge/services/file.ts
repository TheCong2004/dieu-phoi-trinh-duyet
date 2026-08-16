import type { RpcClient } from '../rpc';

export interface FileExportOptions {
  path: string;
  format?: 'json' | 'csv' | 'text';
  onConflict?: 'overwrite' | 'append';
  addBOM?: boolean;
  /** When format='csv' + onConflict='append': merge rows by header (re-aligns mismatched columns). */
  mergeCsv?: boolean;
}

export interface FileDownloadOptions {
  path?: string;
  proxy?: boolean;
}

export interface FileReadLinesOptions {
  /** 'all' (default): array of non-empty lines.
   *  'first': first line as a string.
   *  'random': a random line as a string.
   *  'firstRowAsKey': parse as CSV-like → array of objects keyed by the first row. */
  mode?: 'all' | 'first' | 'random' | 'firstRowAsKey';
  /** Delimiter for firstRowAsKey mode. Default: ','. */
  delimiter?: string;
  /** Trim each line. Default: true. */
  trim?: boolean;
  /** When true, deletes the file after a successful read. */
  deleteAfterRead?: boolean;
}

export interface FileSaveElementAssetsOptions {
  /** Attribute to read for each asset URL. Default: 'src'. */
  attribute?: string;
  /** Pull asset URL from descendants too (otherwise just the locator's element). */
  multiple?: boolean;
  /** Conflict mode for filename collisions. Default: 'uniquify'. */
  onConflict?: 'overwrite' | 'uniquify';
  /** Route HTTP fetches via the profile's proxy. */
  proxy?: boolean;
}

export interface FileSaveElementAssetsResult {
  saved: Array<{ path: string; url: string }>;
}

export class FileService {
  constructor(private rpc: RpcClient) {}

  read(path: string) {
    return this.rpc.call<string>('file.read', { path });
  }

  write(path: string, content: string) {
    return this.rpc.call('file.write', { path, content });
  }

  /**
   * Read a text file as lines. Useful for input lists (one URL/email per line),
   * CSV-like data, or random-pick scenarios.
   */
  readLines(path: string, opts?: FileReadLinesOptions) {
    return this.rpc.call<string | string[] | Record<string, string>[]>(
      'file.readLines',
      { path, ...opts },
    );
  }

  export(data: unknown, opts: FileExportOptions) {
    return this.rpc.call('file.export', { data, ...opts });
  }

  download(url: string, opts?: FileDownloadOptions) {
    return this.rpc.call<string>('file.download', { url, ...opts });
  }

  /**
   * Walk an element (and optionally descendants) and download every value of
   * `attribute` (default 'src') into `folder`. Supports `data:` URLs and
   * `http(s)://...`. Filenames are inferred and sanitised.
   *
   * NOTE: Locator chain matches the `Locator` `selectorChain` API. To pass a
   * locator object, use `loc.getSelectorChain()`.
   */
  saveElementAssets(
    selectorChain: string[],
    folder: string,
    opts?: FileSaveElementAssetsOptions,
  ): Promise<FileSaveElementAssetsResult> {
    return this.rpc.call('file.saveElementAssets', { selectorChain, folder, ...opts });
  }
}
