import { RpcClient } from './rpc';
import { Page } from './page';
import { Tabs } from './tabs';
import { Cookies } from './cookies';
import { Services } from './services/index';

/**
 * One connected automation session for one running browser profile.
 *
 * A Session wraps the WebSocket bridge protocol and exposes:
 *
 * - `page`     — the active tab (Playwright-style automation surface)
 * - `tabs`     — open / close / list / activate tabs
 * - `cookies`  — read & write cookies + storage state
 * - `services` — host-side services (sheets, file, telegram, ai, ...)
 *
 * Created automatically by `OmniLogin.open(profileId)`. Construct manually
 * only when connecting to a bridge launched outside the SDK.
 */
export class Session {
  private rpc: RpcClient;
  private _page: Page | null = null;

  readonly tabs: Tabs;
  readonly cookies: Cookies;
  readonly services: Services;

  constructor(bridgeUrl: string) {
    this.rpc = new RpcClient(bridgeUrl);
    this.tabs = new Tabs(this.rpc);
    this.cookies = new Cookies(this.rpc);
    this.services = new Services(this.rpc);
  }

  async connect() {
    await this.rpc.connect();
  }

  async disconnect() {
    await this.rpc.disconnect();
  }

  get connected(): boolean {
    return this.rpc.connected;
  }

  /** The active tab. Created lazily on first access. */
  get page(): Page {
    if (!this._page) this._page = new Page(this.rpc);
    return this._page;
  }
}
