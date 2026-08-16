import type { RpcClient } from './rpc';

export type MouseButton = 'left' | 'right' | 'middle';

export interface ClickOptions {
  button?: MouseButton;
  clickCount?: number;
  /** When true (default), simulate human-like cursor motion and natural click timing. */
  humanize?: boolean;
}

export class Mouse {
  constructor(private rpc: RpcClient) {}

  async click(x: number, y: number, options?: ClickOptions) {
    return this.rpc.call('page.mouse.click', { x, y, options });
  }

  async dblclick(x: number, y: number, options?: Omit<ClickOptions, 'clickCount'>) {
    return this.rpc.call('page.mouse.click', { x, y, options: { clickCount: 2, ...options } });
  }

  async move(x: number, y: number, options?: { steps?: number }) {
    return this.rpc.call('page.mouse.move', { x, y, options });
  }

  async wheel(deltaX?: number, deltaY?: number, options?: { humanize?: boolean }) {
    return this.rpc.call('page.mouse.wheel', { deltaX, deltaY, options });
  }

  /** Press a mouse button; bridge injects a small natural delay before the transition. */
  async down(options?: { button?: MouseButton }) {
    return this.rpc.call('page.mouse.down', { options });
  }

  /** Release a mouse button; bridge injects a small natural delay before the transition. */
  async up(options?: { button?: MouseButton }) {
    return this.rpc.call('page.mouse.up', { options });
  }
}
