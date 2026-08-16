import type { RpcClient } from '../rpc';

export interface SpreadsheetReadOptions {
  /** A1-notation range, e.g. 'Sheet1!A1:D100'. Defaults to first sheet, full data. */
  range?: string;
  /** When true, treat first row as keys → returns array of objects. */
  firstRowAsKey?: boolean;
}

export interface SpreadsheetWriteOptions {
  /** A1-notation range, e.g. 'Sheet1!A1'. Defaults to first sheet, row 1. */
  range?: string;
  /** When true, append rows after existing data. Otherwise replace from `range` (or full sheet). */
  append?: boolean;
}

/**
 * Local spreadsheet I/O backed by ExcelJS.
 *
 * - `.xlsx` / `.xlsm` files are read/written via the XLSX engine.
 * - `.csv` files are read/written via the CSV engine.
 *
 * File extension determines which engine is used; mixing is not supported in
 * a single call.
 */
export class SpreadsheetService {
  constructor(private rpc: RpcClient) {}

  /** Returns 2D array of cell values, or array of objects when firstRowAsKey is true. */
  read(path: string, opts?: SpreadsheetReadOptions): Promise<unknown[][] | Record<string, unknown>[]> {
    return this.rpc.call('spreadsheet.read', { path, ...opts });
  }

  write(path: string, values: unknown[][], opts?: SpreadsheetWriteOptions): Promise<void> {
    return this.rpc.call('spreadsheet.write', { path, values, ...opts });
  }

  clear(path: string, range: string): Promise<void> {
    return this.rpc.call('spreadsheet.clear', { path, range });
  }
}
