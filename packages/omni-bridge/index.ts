// Bridge exports — runtime classes
export { Session } from './session';
export { Page } from './page';
export { Locator } from './locator';
export { Keyboard } from './keyboard';
export { Mouse } from './mouse';
export { Tabs } from './tabs';
export { Cookies } from './cookies';
export { PageBrowser } from './page-browser';
export {
  Services,
  SpreadsheetService,
  ClipboardService,
  NotificationService,
  TotpService,
  SheetsService,
  FileService,
  EmailService,
  ProfileInfoService,
  ExtensionService,
} from './services/index';

// Bridge errors (low-level RpcClient stays internal)
export { RpcError } from './rpc';

// Service-level types
export type {
  HttpMethod,
  HttpRequestOptions,
  HttpResponse,
  TelegramSendOptions,
  AiOptions,
  AiVisionImage,
  AiImageOptions,
  AiImageResult,
  ImageSearchOptions,
  ImageMatch,
  // Sheets
  SheetsGetOptions,
  SheetsUpdateOptions,
  SheetsAppendOptions,
  SheetsCreateResult,
  SheetsAddSheetResult,
  // File
  FileExportOptions,
  FileDownloadOptions,
  FileReadLinesOptions,
  FileSaveElementAssetsOptions,
  FileSaveElementAssetsResult,
  // Email
  EmailReadOptions,
  EmailMessage,
  // Profile
  BridgeProfileInfo,
  InlineProxy,
  ProfileCloneOptions,
  ProfileCloneResult,
  ProfileSwitchProxyOptions,
  // New services
  SpreadsheetReadOptions,
  SpreadsheetWriteOptions,
  ClipboardReadOptions,
  ClipboardWriteOptions,
  NotificationShowOptions,
  TotpResult,
} from './services/index';

// Sub-class option types
export type { KeyboardTypeOptions } from './keyboard';
export type { ClickOptions, MouseButton } from './mouse';
export type { GetByRoleOptions } from './locator';
export type { TabInfo, NewPageOptions } from './tabs';
export type { Cookie, OriginStorage, StorageState } from './cookies';
export { CookieFormat, parseCookies, toBridgeCookies, toNetscape } from './cookies';
