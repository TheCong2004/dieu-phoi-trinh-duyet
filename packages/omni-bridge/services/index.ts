import type { RpcClient } from '../rpc';
import { SheetsService } from './sheets';
import { FileService } from './file';
import { EmailService } from './email';
import { ProfileInfoService } from './profile-info';
import { ExtensionService } from './extension';
import { SpreadsheetService } from './spreadsheet';
import { ClipboardService } from './clipboard';
import { NotificationService } from './notification';
import { TotpService } from './totp';

export type HttpMethod = 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH' | 'HEAD' | 'OPTIONS';

export interface HttpRequestOptions {
  headers?: Record<string, string>;
  body?: unknown;
  proxy?: boolean;
  timeout?: number;
  responseType?: string;
  cache?: boolean;
  jsonPath?: string;
}

export interface HttpResponse {
  status: number;
  headers: Record<string, string>;
  data: unknown;
}

export interface TelegramSendOptions {
  botToken: string;
  chatId: string;
  parseMode?: 'HTML' | 'Markdown' | 'MarkdownV2';
  silent?: boolean;
  disablePreview?: boolean;
  proxy?: boolean;
  /** Reply to an existing message (maps to reply_parameters.message_id). */
  replyToMessageId?: number;
  /** When parseMode='HTML', escape `< > &` in the message before sending. */
  escapeHtml?: boolean;
}

export interface AiVisionImage {
  /** Local file path. */
  path?: string;
  /** Public URL — bridge fetches it. */
  url?: string;
  /** Pre-encoded base64 (no `data:` prefix needed). */
  base64?: string;
  /** Override; otherwise inferred from extension/URL. */
  mimeType?: string;
}

export interface AiOptions {
  provider: 'openai' | 'claude' | 'gemini' | 'deepseek' | 'grok' | 'openrouter' | 'custom';
  model?: string;
  apiKey: string;
  systemPrompt?: string;
  temperature?: number;
  maxTokens?: number;
  endpoint?: string;
  headers?: Record<string, string>;
  /** Vision input (Claude / Gemini / Grok / OpenAI multimodal models). */
  images?: AiVisionImage[];
  /** Gemini API version. Default: 'v1beta'. */
  apiVersion?: 'v1' | 'v1beta';
}

export interface AiImageOptions {
  /** Default: 'openai'. */
  provider?: 'openai' | 'grok' | 'custom';
  /** OpenAI: 'dall-e-3' (default), 'dall-e-2', 'gpt-image-1'. */
  model?: string;
  apiKey: string;
  /** OpenAI sizes: '1024x1024' (default), '1024x1792', '1792x1024'. */
  size?: string;
  /** 'url' (default) or 'b64_json'. */
  responseFormat?: 'url' | 'b64_json';
  /** Number of images. Default: 1. */
  n?: number;
  /** Custom endpoint (custom provider only). */
  endpoint?: string;
  headers?: Record<string, string>;
}

export interface AiImageResult {
  images: Array<{ url?: string; b64?: string }>;
}

export interface ImageSearchOptions {
  fullScreen?: boolean;
  multiple?: boolean;
  timeout?: number;
  /** Min match score (0-1). Lower than the matcher's built-in 0.9 returns more matches. */
  confidence?: number;
  /** Capture multiple frames per attempt — useful for animated targets. */
  gifCapture?: boolean;
  /** Restrict matches to a viewport region. */
  region?: { x: number; y: number; width: number; height: number };
}

export interface ImageMatch {
  left: number;
  top: number;
  right: number;
  bottom: number;
  /** Match confidence score (0-1). */
  score: number;
  center: { x: number; y: number };
}

/**
 * Host-side services exposed via the OmniBridge WebSocket.
 *
 * Multi-method services live as namespaces (`services.sheets.get(...)`); single
 * operations are callable directly on the facade (`services.http(...)`).
 *
 * In the AI App Deno runtime this instance is exposed as the global `omni`,
 * so user scripts call `omni.sheets.get(...)`, `omni.file.write(...)`, etc.
 */
export class Services {
  readonly sheets: SheetsService;
  readonly email: EmailService;
  readonly file: FileService;
  readonly profile: ProfileInfoService;
  readonly extension: ExtensionService;
  readonly spreadsheet: SpreadsheetService;
  readonly clipboard: ClipboardService;
  readonly notification: NotificationService;
  readonly totp: TotpService;

  constructor(private rpc: RpcClient) {
    this.sheets = new SheetsService(rpc);
    this.email = new EmailService(rpc);
    this.file = new FileService(rpc);
    this.profile = new ProfileInfoService(rpc);
    this.extension = new ExtensionService(rpc);
    this.spreadsheet = new SpreadsheetService(rpc);
    this.clipboard = new ClipboardService(rpc);
    this.notification = new NotificationService(rpc);
    this.totp = new TotpService(rpc);
  }

  http(method: HttpMethod, url: string, opts?: HttpRequestOptions) {
    return this.rpc.call<HttpResponse>('http', { method, url, ...opts });
  }

  telegram(message: string, opts: TelegramSendOptions) {
    return this.rpc.call('telegram', { message, ...opts });
  }

  ai(prompt: string, opts: AiOptions) {
    return this.rpc.call<string>('ai', { prompt, ...opts });
  }

  /** Generate an image from a text prompt. Separate from `ai()` because input + output shapes differ. */
  aiImage(prompt: string, opts: AiImageOptions) {
    return this.rpc.call<AiImageResult>('ai.image', { prompt, ...opts });
  }

  imageSearch(template: string, opts?: ImageSearchOptions) {
    return this.rpc.call<ImageMatch[]>('imageSearch', { template, ...opts });
  }
}

export type {
  SheetsGetOptions,
  SheetsUpdateOptions,
  SheetsAppendOptions,
  SheetsCreateResult,
  SheetsAddSheetResult,
} from './sheets';
export { SheetsService } from './sheets';

export type {
  FileExportOptions,
  FileDownloadOptions,
  FileReadLinesOptions,
  FileSaveElementAssetsOptions,
  FileSaveElementAssetsResult,
} from './file';
export { FileService } from './file';

export type { EmailReadOptions, EmailMessage } from './email';
export { EmailService } from './email';

export type {
  BridgeProfileInfo,
  InlineProxy,
  ProfileCloneOptions,
  ProfileCloneResult,
  ProfileSwitchProxyOptions,
} from './profile-info';
export { ProfileInfoService } from './profile-info';

export { ExtensionService } from './extension';

export type {
  SpreadsheetReadOptions,
  SpreadsheetWriteOptions,
} from './spreadsheet';
export { SpreadsheetService } from './spreadsheet';

export type {
  ClipboardReadOptions,
  ClipboardWriteOptions,
} from './clipboard';
export { ClipboardService } from './clipboard';

export type { NotificationShowOptions } from './notification';
export { NotificationService } from './notification';

export type { TotpResult } from './totp';
export { TotpService } from './totp';
