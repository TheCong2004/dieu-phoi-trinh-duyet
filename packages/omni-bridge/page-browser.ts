import type { RpcClient } from './rpc';
import { Tabs, type NewPageOptions } from './tabs';
import { Cookies } from './cookies';

/**
 * Browser-level operations exposed via `page.browser` for backward compatibility
 * with AI App scripts that use the Playwright-style names `newPage()`, `pages()`,
 * `context().cookies()`, etc.
 *
 * New SDK code should prefer `session.tabs` and `session.cookies` directly.
 */
export class PageBrowser {
  private _tabs: Tabs;
  private _cookies: Cookies;

  constructor(rpc: RpcClient) {
    this._tabs = new Tabs(rpc);
    this._cookies = new Cookies(rpc);
  }

  /** Old `context()` shape — delegates to `Cookies` with legacy method names. */
  context() {
    const c = this._cookies;
    return {
      cookies: c.list.bind(c),
      addCookies: c.add.bind(c),
      clearCookies: c.clear.bind(c),
      storageState: c.getStorageState.bind(c),
      setStorageState: c.setStorageState.bind(c),
    };
  }

  newPage(url?: string, options?: NewPageOptions) { return this._tabs.open(url, options); }
  closePage(targetId?: string)                    { return this._tabs.close(targetId); }
  pages()                                         { return this._tabs.list(); }
  bringToFront(targetId: string)                  { return this._tabs.activate(targetId); }
  nextPage()                                      { return this._tabs.next(); }
  previousPage()                                  { return this._tabs.previous(); }
}
