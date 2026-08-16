import type { RpcClient } from '../rpc';

export interface NotificationShowOptions {
  body?: string;
  /** Absolute path to an icon file (PNG/ICO/etc.). Validated to be inside user home. */
  icon?: string;
  silent?: boolean;
}

/**
 * Desktop notifications via Electron's Notification API. Relayed to the
 * main process — only available while the OmniLogin desktop is running.
 */
export class NotificationService {
  constructor(private rpc: RpcClient) {}

  show(title: string, opts?: NotificationShowOptions): Promise<void> {
    return this.rpc.call('notification.show', { title, ...opts });
  }
}
