import type { RpcClient } from './rpc';

export interface TabInfo {
  targetId: string;
  url: string;
  title: string;
}

export interface NewPageOptions {
  active?: boolean;
  userAgent?: string;
  bypassCSP?: boolean;
}

/**
 * Tab management for the connected browser.
 *
 * Cookies & storage live on `Cookies`; per-tab automation lives on `Page`.
 */
export class Tabs {
  constructor(private rpc: RpcClient) {}

  async open(url?: string, options?: NewPageOptions) {
    return this.rpc.call<{ targetId: string }>('tab.new', { url, ...options });
  }

  async close(targetId?: string) {
    return this.rpc.call('tab.close', { targetId });
  }

  async list() {
    return this.rpc.call<{ pages: TabInfo[] }>('tab.list');
  }

  async activate(targetId: string) {
    return this.rpc.call('tab.activate', { targetId });
  }

  async next() {
    return this.rpc.call<{ targetId: string } | void>('tab.next');
  }

  async previous() {
    return this.rpc.call<{ targetId: string } | void>('tab.previous');
  }
}
