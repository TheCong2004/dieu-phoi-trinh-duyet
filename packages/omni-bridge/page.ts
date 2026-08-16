import type { RpcClient } from './rpc';
import { Locator, roleSelector, type GetByRoleOptions } from './locator';
import { Keyboard } from './keyboard';
import { Mouse } from './mouse';
import { PageBrowser } from './page-browser';

export class Page {
  readonly keyboard: Keyboard;
  readonly mouse: Mouse;
  /**
   * Backward-compatible browser-level operations (newPage, pages, context.cookies, etc.)
   * for AI App scripts. New SDK code should prefer `session.tabs` / `session.cookies`.
   */
  readonly browser: PageBrowser;

  constructor(private _rpc: RpcClient) {
    this.keyboard = new Keyboard(_rpc);
    this.mouse = new Mouse(_rpc);
    this.browser = new PageBrowser(_rpc);
  }

  get rpc(): RpcClient { return this._rpc; }

  // Navigation
  async goto(url: string, options?: { waitUntil?: 'load' | 'domcontentloaded' | 'networkidle'; timeout?: number }) {
    return this._rpc.call('page.goto', { url, options });
  }
  async goBack(options?: { timeout?: number }) { return this._rpc.call('page.goBack', { options }); }
  async goForward(options?: { timeout?: number }) { return this._rpc.call('page.goForward', { options }); }
  async reload(options?: { waitUntil?: string; timeout?: number }) { return this._rpc.call('page.reload', { options }); }
  async url() { return this._rpc.call<string>('page.url'); }
  async title() { return this._rpc.call<string>('page.title'); }
  async content() { return this._rpc.call<string>('page.content'); }

  // Evaluate
  async evaluate(expression: string | ((...args: unknown[]) => unknown), ...args: unknown[]) {
    const expr = typeof expression === 'function' ? expression.toString() : expression;
    return this._rpc.call('page.evaluate', { expression: expr, args });
  }
  async evaluateHandle(expression: string) {
    return this._rpc.call<string>('page.evaluateHandle', { expression });
  }

  // Wait
  async waitForTimeout(timeout: number) {
    return this._rpc.call('page.waitForTimeout', { timeout });
  }
  async waitForLoadState(state?: 'load' | 'domcontentloaded' | 'networkidle') {
    return this._rpc.call('page.waitForLoadState', { state });
  }
  async waitForURL(url: string, options?: { timeout?: number }) {
    return this._rpc.call('page.waitForURL', { url, options });
  }
  async waitForRequest(url: string, options?: { timeout?: number }) {
    return this._rpc.call<{ url: string; method: string; resourceType?: string; requestId: string }>('page.waitForRequest', { url, options });
  }
  async waitForResponse(url: string, options?: { timeout?: number }) {
    return this._rpc.call<{ url: string; status: number; requestId: string }>('page.waitForResponse', { url, options });
  }
  async waitForFunction(expression: string | ((...args: unknown[]) => unknown), options?: { timeout?: number; polling?: number }) {
    const expr = typeof expression === 'function' ? expression.toString() : expression;
    return this._rpc.call('page.waitForFunction', { expression: expr, options });
  }

  // Screenshot
  async screenshot(options?: { fullPage?: boolean; clip?: { x: number; y: number; width: number; height: number }; type?: 'png' | 'jpeg'; quality?: number; path?: string }) {
    return this._rpc.call<string>('page.screenshot', { options });
  }

  // Locator factories
  locator(selector: string): Locator {
    return new Locator(this._rpc, [selector]);
  }
  getByLabel(text: string): Locator { return new Locator(this._rpc, [`label=${text}`]); }
  getByPlaceholder(text: string): Locator { return new Locator(this._rpc, [`placeholder=${text}`]); }
  getByRole(role: string, options?: GetByRoleOptions): Locator {
    return new Locator(this._rpc, [roleSelector(role, options)]);
  }
  getByText(text: string): Locator { return new Locator(this._rpc, [`text=${text}`]); }
  getByTestId(testId: string): Locator { return new Locator(this._rpc, [`testid=${testId}`]); }
  getByAltText(text: string): Locator { return new Locator(this._rpc, [`css=[alt="${text}"]`]); }
  getByTitle(text: string): Locator { return new Locator(this._rpc, [`css=[title="${text}"]`]); }

  // Events
  on(event: string, handler: (...args: unknown[]) => void) {
    this._rpc.on(`event.${event}`, handler);
    this._rpc.call('page.on', { event }).catch(() => {});
  }

  // Dialog
  async acceptDialog(promptText?: string) {
    return this._rpc.call('dialog.accept', { promptText });
  }
  async dismissDialog() {
    return this._rpc.call('dialog.dismiss');
  }

  // Network control
  async setExtraHTTPHeaders(headers: Record<string, string>) {
    return this._rpc.call('page.setExtraHTTPHeaders', { headers });
  }
  async route(url: string, action: { action: 'abort' | 'continue' | 'fulfill'; status?: number; headers?: Record<string, string>; body?: string; method?: string; postData?: string; errorReason?: string }) {
    return this._rpc.call('page.route', { url, action: action.action, overrides: action });
  }
  async unroute(url?: string) {
    return this._rpc.call('page.unroute', { url });
  }
  async unrouteAll() {
    return this._rpc.call('page.unrouteAll');
  }

  // Frames
  async frame(nameOrUrl: string) { return this._rpc.call<string | null>('page.frame', { nameOrUrl }); }
  async frames() { return this._rpc.call<Array<{ name: string; url: string; frameId: string }>>('page.frames'); }

  // Snapshots — for AI-friendly page understanding
  async accessibilitySnapshot(options?: { depth?: number }) {
    return this._rpc.call<{ tree: string; nodeCount: number }>('agent.getAccessibilityTree', options);
  }
  async markdownSnapshot(options?: { maxLength?: number }) {
    return this._rpc.call<{ content: string; truncated: boolean }>('agent.getMarkdown', options);
  }
}
