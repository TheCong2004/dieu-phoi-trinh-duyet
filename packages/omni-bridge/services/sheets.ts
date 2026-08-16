import type { RpcClient } from '../rpc';

export interface SheetsGetOptions {
  /** Convert rows to objects keyed by the first row. */
  firstRowAsKey?: boolean;
  /** When set + firstRowAsKey: index the result by this column (object map). */
  primaryKey?: string;
  /** When set + primaryKey: group rows sharing the same key into arrays. */
  valueIsArray?: boolean;
  /** How values are returned. Default: FORMATTED_VALUE. */
  valueRenderOption?: 'FORMATTED_VALUE' | 'UNFORMATTED_VALUE' | 'FORMULA';
  /** How dates are returned. Default: SERIAL_NUMBER. */
  dateTimeRenderOption?: 'SERIAL_NUMBER' | 'FORMATTED_STRING';
}

export interface SheetsUpdateOptions {
  valueInputOption?: 'RAW' | 'USER_ENTERED';
}

export interface SheetsAppendOptions extends SheetsUpdateOptions {
  /** INSERT_ROWS (default) inserts new rows; OVERWRITE writes over existing data. */
  insertDataOption?: 'INSERT_ROWS' | 'OVERWRITE';
}

export interface SheetsCreateResult {
  spreadsheetId: string;
  /** Public URL to the new spreadsheet. */
  url: string;
}

export interface SheetsAddSheetResult {
  sheetId: number;
  title: string;
}

export class SheetsService {
  constructor(private rpc: RpcClient) {}

  get(spreadsheetId: string, range: string, opts?: SheetsGetOptions) {
    return this.rpc.call('sheets.get', { spreadsheetId, range, ...opts });
  }

  update(spreadsheetId: string, range: string, values: unknown[][], opts?: SheetsUpdateOptions) {
    return this.rpc.call('sheets.update', { spreadsheetId, range, values, ...opts });
  }

  append(spreadsheetId: string, range: string, values: unknown[][], opts?: SheetsAppendOptions) {
    return this.rpc.call('sheets.append', { spreadsheetId, range, values, ...opts });
  }

  clear(spreadsheetId: string, range: string) {
    return this.rpc.call('sheets.clear', { spreadsheetId, range });
  }

  /** Create a new spreadsheet. Returns its id + URL. */
  create(title: string): Promise<SheetsCreateResult> {
    return this.rpc.call('sheets.create', { title });
  }

  /** Add a tab/sheet to an existing spreadsheet. */
  addSheet(spreadsheetId: string, title: string): Promise<SheetsAddSheetResult> {
    return this.rpc.call('sheets.addSheet', { spreadsheetId, title });
  }
}
