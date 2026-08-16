import type { RpcClient } from './rpc';

export interface KeyboardTypeOptions {
  delay?: number;
  /** When true (default), simulate human-like timing between keystrokes. */
  humanize?: boolean;
}

export class Keyboard {
  constructor(private rpc: RpcClient) {}

  async type(text: string, options?: KeyboardTypeOptions) {
    return this.rpc.call('page.keyboard.type', { text, options });
  }

  /** Press a key with bridge-managed natural timing. */
  async press(key: string) {
    return this.rpc.call('page.keyboard.press', { key });
  }

  /** Hold a key down; bridge injects a small natural delay before the transition. */
  async down(key: string) {
    return this.rpc.call('page.keyboard.down', { key });
  }

  /** Release a held key; bridge injects a small natural delay before the transition. */
  async up(key: string) {
    return this.rpc.call('page.keyboard.up', { key });
  }

  /** Insert text directly with no key events and no humanized timing. */
  async insertText(text: string) {
    return this.rpc.call('page.keyboard.insertText', { text });
  }
}
