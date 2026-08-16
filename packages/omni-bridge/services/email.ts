import type { RpcClient } from '../rpc';

export interface EmailReadOptions {
  host: string;
  port: number;
  user: string;
  /** Required when not using OAuth. */
  password?: string;
  /** XOAUTH2 access token (any provider). Bypasses password. */
  accessToken?: string;
  /** Outlook OAuth — exchanged server-side for an access token. */
  clientId?: string;
  refreshToken?: string;
  tls?: boolean;
  /** When true, sets `tls.rejectUnauthorized: false` + `minVersion: TLSv1`. */
  tlsInsecure?: boolean;
  folder?: string;
  since?: string;
  from?: string;
  to?: string;
  subject?: string;
  /** Substring search inside the message body. */
  body?: string;
  /** Only return unread messages. */
  unreadOnly?: boolean;
  /** Mark fetched messages as \\Seen after reading. */
  markAsRead?: boolean;
  /** When set, regex applied to body — `matches` populated on each message and the union on [0]. */
  bodyRegex?: string;
  /** Flags for bodyRegex (e.g. "gi"). Default: "g". */
  bodyRegexFlags?: string;
  limit?: number;
}

export interface EmailMessage {
  from: string;
  to: string;
  subject: string;
  date: string;
  text: string;
  html: string;
  /** Regex matches against body (when bodyRegex was set). */
  matches?: string[];
}

export class EmailService {
  constructor(private rpc: RpcClient) {}

  /**
   * Fetch up to `opts.limit` messages from a mailbox.
   *
   * Authentication:
   * - **basic**: `{ user, password }`
   * - **OAuth (Outlook)**: `{ user, clientId, refreshToken }` — bridge exchanges
   *   the refresh token for an access token via Microsoft.
   * - **OAuth (generic)**: `{ user, accessToken }` — XOAUTH2.
   *
   * For Outlook, host is automatically rewritten to `outlook.office365.com`
   * when an `imap-mail.outlook.com` host is supplied.
   */
  read(opts: EmailReadOptions) {
    return this.rpc.call<EmailMessage[]>('email.read', { ...opts });
  }
}
