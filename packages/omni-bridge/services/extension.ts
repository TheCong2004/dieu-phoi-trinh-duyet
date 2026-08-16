import type { RpcClient } from '../rpc';

export class ExtensionService {
  constructor(private rpc: RpcClient) {}

  trigger(extensionId: string) {
    return this.rpc.call('extension.trigger', { extensionId });
  }
}
