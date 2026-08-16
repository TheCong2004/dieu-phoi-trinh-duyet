import type { RpcClient } from './rpc';

export interface GetByRoleOptions {
  checked?: boolean;
  disabled?: boolean;
  exact?: boolean;
  expanded?: boolean;
  includeHidden?: boolean;
  level?: number;
  name?: string | RegExp;
  pressed?: boolean;
  selected?: boolean;
}

function escapeRoleName(value: string | RegExp, exact: boolean): string {
  if (value instanceof RegExp) return value.toString().replace(/>>/g, '\\>\\>');
  return `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"${exact ? 's' : 'i'}`;
}

export function roleSelector(role: string, options: GetByRoleOptions = {}): string {
  const attributes: string[] = [];
  if (options.checked !== undefined) attributes.push(`[checked=${options.checked}]`);
  if (options.disabled !== undefined) attributes.push(`[disabled=${options.disabled}]`);
  if (options.expanded !== undefined) attributes.push(`[expanded=${options.expanded}]`);
  if (options.includeHidden !== undefined)
    attributes.push(`[include-hidden=${options.includeHidden}]`);
  if (options.level !== undefined) attributes.push(`[level=${options.level}]`);
  if (options.name !== undefined)
    attributes.push(`[name=${escapeRoleName(options.name, options.exact === true)}]`);
  if (options.pressed !== undefined) attributes.push(`[pressed=${options.pressed}]`);
  if (options.selected !== undefined) attributes.push(`[selected=${options.selected}]`);

  // Preserve the lightweight bridge query when no advanced ARIA option is used.
  if (attributes.length === 0) return `role=${role}`;
  return `internal:role=${role}${attributes.join('')}`;
}

export class Locator {
  constructor(
    private rpc: RpcClient,
    private selectorChain: string[],
  ) {}

  /** Read-only access to the selector chain — used by peer locators (e.g. dragTo). */
  getSelectorChain(): readonly string[] {
    return this.selectorChain;
  }

  locator(selector: string): Locator {
    return new Locator(this.rpc, [...this.selectorChain, selector]);
  }

  getByLabel(text: string): Locator {
    return this.locator(`label=${text}`);
  }
  getByPlaceholder(text: string): Locator {
    return this.locator(`placeholder=${text}`);
  }
  getByRole(role: string, options?: GetByRoleOptions): Locator {
    return this.locator(roleSelector(role, options));
  }
  getByText(text: string): Locator {
    return this.locator(`text=${text}`);
  }
  getByTestId(testId: string): Locator {
    return this.locator(`testid=${testId}`);
  }
  getByAltText(text: string): Locator {
    return this.locator(`css=[alt="${text}"]`);
  }
  getByTitle(text: string): Locator {
    return this.locator(`css=[title="${text}"]`);
  }

  first(): Locator {
    return new Locator(this.rpc, [...this.selectorChain, ':first']);
  }

  last(): Locator {
    return new Locator(this.rpc, [...this.selectorChain, ':last']);
  }

  nth(n: number): Locator {
    return new Locator(this.rpc, [...this.selectorChain, `:nth(${n})`]);
  }

  async click(options?: {
    humanize?: boolean;
    button?: 'left' | 'right' | 'middle';
    /** 1 (click) | 2 (double) | 3 (triple). Default: 1. */
    clickCount?: number;
    /** Modifier keys held during the click. */
    modifiers?: Array<'Alt' | 'Control' | 'Meta' | 'Shift'>;
    /** Ctrl/Cmd+click — open the link in a new background tab. */
    openInNewTab?: boolean;
    /** Hold time between mousedown and mouseup (ms). */
    delay?: number;
  }) {
    return this.rpc.call('locator.click', {
      selectorChain: this.selectorChain,
      options: { humanize: true, ...options },
    });
  }

  async fill(value: string, options?: { humanize?: boolean; clearBefore?: boolean }) {
    return this.rpc.call('locator.fill', {
      selectorChain: this.selectorChain,
      value,
      options: { humanize: true, ...options },
    });
  }

  async textContent() {
    return this.rpc.call<string | null>('locator.textContent', {
      selectorChain: this.selectorChain,
    });
  }

  async innerText() {
    return this.rpc.call<string>('locator.innerText', { selectorChain: this.selectorChain });
  }

  async innerHTML() {
    return this.rpc.call<string>('locator.innerHTML', { selectorChain: this.selectorChain });
  }

  async getAttribute(name: string) {
    return this.rpc.call<string | null>('locator.getAttribute', {
      selectorChain: this.selectorChain,
      name,
    });
  }

  async inputValue() {
    return this.rpc.call<string>('locator.inputValue', { selectorChain: this.selectorChain });
  }

  async isVisible() {
    return this.rpc.call<boolean>('locator.isVisible', { selectorChain: this.selectorChain });
  }

  async isEnabled() {
    return this.rpc.call<boolean>('locator.isEnabled', { selectorChain: this.selectorChain });
  }

  async isChecked() {
    return this.rpc.call<boolean>('locator.isChecked', { selectorChain: this.selectorChain });
  }

  async isEditable() {
    return this.rpc.call<boolean>('locator.isEditable', { selectorChain: this.selectorChain });
  }

  async count() {
    return this.rpc.call<number>('locator.count', { selectorChain: this.selectorChain });
  }

  async waitFor(options?: {
    state?: 'attached' | 'visible' | 'hidden' | 'detached';
    timeout?: number;
  }) {
    return this.rpc.call('locator.waitFor', { selectorChain: this.selectorChain, options });
  }

  async hover(options?: { humanize?: boolean }) {
    return this.rpc.call('locator.hover', {
      selectorChain: this.selectorChain,
      options: { humanize: true, ...options },
    });
  }

  async dblclick(options?: { humanize?: boolean }) {
    return this.rpc.call('locator.dblclick', {
      selectorChain: this.selectorChain,
      options: { humanize: true, ...options },
    });
  }

  async selectOption(values: string | string[], options?: { by?: 'value' | 'text' | 'position' }) {
    return this.rpc.call<string[]>('locator.selectOption', {
      selectorChain: this.selectorChain,
      values,
      options,
    });
  }

  async check(options?: { humanize?: boolean }) {
    return this.rpc.call('locator.check', {
      selectorChain: this.selectorChain,
      options: { humanize: true, ...options },
    });
  }

  async uncheck(options?: { humanize?: boolean }) {
    return this.rpc.call('locator.uncheck', {
      selectorChain: this.selectorChain,
      options: { humanize: true, ...options },
    });
  }

  async setChecked(checked: boolean) {
    return this.rpc.call('locator.setChecked', { selectorChain: this.selectorChain, checked });
  }

  async clear() {
    return this.rpc.call('locator.clear', { selectorChain: this.selectorChain });
  }

  async press(key: string) {
    return this.rpc.call('locator.press', { selectorChain: this.selectorChain, key });
  }

  async pressSequentially(text: string, options?: { delay?: number; humanize?: boolean }) {
    return this.rpc.call('locator.pressSequentially', {
      selectorChain: this.selectorChain,
      text,
      options: { humanize: true, ...options },
    });
  }

  async focus() {
    return this.rpc.call('locator.focus', { selectorChain: this.selectorChain });
  }

  async blur() {
    return this.rpc.call('locator.blur', { selectorChain: this.selectorChain });
  }

  async scrollIntoViewIfNeeded() {
    return this.rpc.call('locator.scrollIntoViewIfNeeded', { selectorChain: this.selectorChain });
  }

  async dispatchEvent(type: string, eventInit?: Record<string, unknown>) {
    return this.rpc.call('locator.dispatchEvent', {
      selectorChain: this.selectorChain,
      type,
      eventInit,
    });
  }

  /**
   * Upload files. Target may be an `<input type="file">` directly OR any element
   * (button, drag-zone, custom widget) that opens a native file chooser on click.
   *
   * Each entry accepts:
   *   - Absolute local path
   *   - `http(s)://...` URL (downloaded to a temp file)
   *   - `data:<mime>;base64,...` (decoded to a temp file)
   *   - `<filename>|http(s)://...` or `<filename>|data:...` (downloaded/decoded as <filename>)
   *
   * Pass `[]` to clear an `<input type="file">`.
   */
  async setInputFiles(files: string[]) {
    return this.rpc.call('locator.setInputFiles', { selectorChain: this.selectorChain, files });
  }

  /** Drag this locator onto another locator with bridge-managed natural hold/drag/drop timing. */
  async dragTo(target: Locator, options?: { delay?: number }) {
    return this.rpc.call('locator.dragTo', {
      selectorChain: this.selectorChain,
      targetSelectorChain: target.getSelectorChain(),
      options,
    });
  }

  async boundingBox() {
    return this.rpc.call<{ x: number; y: number; width: number; height: number } | null>(
      'locator.boundingBox',
      { selectorChain: this.selectorChain },
    );
  }

  async evaluate(expression: string | ((...args: unknown[]) => unknown), ...args: unknown[]) {
    const expr = typeof expression === 'function' ? expression.toString() : expression;
    return this.rpc.call('locator.evaluate', {
      selectorChain: this.selectorChain,
      expression: expr,
      args,
    });
  }

  async evaluateAll(expression: string | ((...args: unknown[]) => unknown), ...args: unknown[]) {
    const expr = typeof expression === 'function' ? expression.toString() : expression;
    return this.rpc.call('locator.evaluateAll', {
      selectorChain: this.selectorChain,
      expression: expr,
      args,
    });
  }

  async screenshot(options?: { type?: 'png' | 'jpeg'; quality?: number; path?: string }) {
    return this.rpc.call<string>('locator.screenshot', {
      selectorChain: this.selectorChain,
      options,
    });
  }

  async allInnerTexts() {
    return this.rpc.call<string[]>('locator.allInnerTexts', { selectorChain: this.selectorChain });
  }

  async allTextContents() {
    return this.rpc.call<(string | null)[]>('locator.allTextContents', {
      selectorChain: this.selectorChain,
    });
  }

  filter(options: { hasText?: string; hasNotText?: string }): Locator {
    const chain = [...this.selectorChain];
    const last = chain[chain.length - 1];
    let suffix = '';
    if (options.hasText) suffix += `:has-text("${options.hasText}")`;
    if (options.hasNotText) suffix += `:not(:has-text("${options.hasNotText}"))`;
    chain[chain.length - 1] = last + suffix;
    return new Locator(this.rpc, chain);
  }

  async all(): Promise<Locator[]> {
    const n = await this.count();
    const result: Locator[] = [];
    for (let i = 0; i < n; i++) {
      result.push(this.nth(i));
    }
    return result;
  }
}
