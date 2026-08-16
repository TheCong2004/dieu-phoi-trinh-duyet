import type { RpcClient } from '../rpc';

export interface ClipboardReadOptions {
  format?: 'text' | 'html';
}

export interface ClipboardWriteOptions {
  format?: 'text' | 'html';
}

/**
 * System clipboard access. Relayed from the bridge process to the desktop's
 * main process — only available while the OmniLogin desktop is running.
 */
export class ClipboardService {
  constructor(private rpc: RpcClient) {}

  read(opts?: ClipboardReadOptions): Promise<string> {
    return this.rpc.call('clipboard.read', { ...opts });
  }

  write(text: string, opts?: ClipboardWriteOptions): Promise<void> {
    return this.rpc.call('clipboard.write', { text, ...opts });
  }
}
