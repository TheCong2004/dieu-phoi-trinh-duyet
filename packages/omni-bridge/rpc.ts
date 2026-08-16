export class RpcError extends Error {
  code: number;
  data?: unknown;

  constructor(code: number, message: string, data?: unknown) {
    super(message);
    this.name = 'RpcError';
    this.code = code;
    this.data = data;
  }
}

interface PendingRequest {
  resolve: (value: any) => void;
  reject: (reason: any) => void;
  timer: ReturnType<typeof setTimeout>;
}

let _WS: typeof WebSocket | undefined;

async function getWebSocket(): Promise<typeof WebSocket> {
  if (_WS) return _WS;
  if (typeof globalThis.WebSocket !== 'undefined') {
    return (_WS = globalThis.WebSocket);
  }

  // Fallback for runtimes without a native WebSocket, such as Node 18-20.
  try {
    const wsModule = await import('ws');
    const ws = ((wsModule as any).WebSocket ?? wsModule.default ?? wsModule) as typeof WebSocket;

    return (_WS = ws);
  } catch {
    throw new Error('WebSocket not available. Install the ws package: npm install ws');
  }
}

export class RpcClient {
  private ws: WebSocket | null = null;
  private requestId = 0;
  private pending = new Map<number, PendingRequest>();
  private eventHandlers = new Map<string, Set<(...args: unknown[]) => void>>();
  private remoteCdpSubscriptions = new Set<string>();
  private remoteCdpSubscriptionsInFlight = new Map<string, Promise<void>>();
  private defaultTimeout: number;

  constructor(
    private url: string,
    options?: { timeout?: number },
  ) {
    this.defaultTimeout = options?.timeout ?? 30000;
  }

  async connect(): Promise<void> {
    const WS = await getWebSocket();
    return new Promise((resolve, reject) => {
      this.ws = new WS(this.url) as WebSocket;
      this.ws.onopen = () => {
        void this.resubscribeRemoteCdpEvents();
        resolve();
      };
      this.ws.onerror = (e: any) => reject(new Error(e.message ?? 'WebSocket error'));
      this.ws.onclose = () => this.cleanup();
      this.ws.onmessage = (e: any) =>
        this.handleMessage(typeof e.data === 'string' ? e.data : String(e.data));
    });
  }

  async call<T = unknown>(
    method: string,
    params?: Record<string, unknown> | unknown[],
    timeout?: number,
  ): Promise<T> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket not connected');
    }

    const id = ++this.requestId;
    const msg = JSON.stringify({ jsonrpc: '2.0', id, method, params });

    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new RpcError(-32001, `Timeout: ${method}`));
      }, timeout ?? this.defaultTimeout);

      this.pending.set(id, { resolve, reject, timer });
      this.ws!.send(msg);
    });
  }

  notify(method: string, params?: Record<string, unknown>) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ jsonrpc: '2.0', method, params }));
    }
  }

  on(event: string, handler: (...args: unknown[]) => void) {
    let handlers = this.eventHandlers.get(event);
    if (!handlers) {
      handlers = new Set();
      this.eventHandlers.set(event, handlers);
    }

    const isNewHandler = !handlers.has(handler);
    handlers.add(handler);

    if (isNewHandler && this.isCdpEvent(event)) {
      void this.ensureRemoteCdpSubscribed(event);
    }
  }

  off(event: string, handler: (...args: unknown[]) => void) {
    const handlers = this.eventHandlers.get(event);
    if (!handlers || !handlers.delete(handler)) return;

    if (handlers.size === 0) {
      this.eventHandlers.delete(event);
      if (this.isCdpEvent(event)) {
        void this.unsubscribeRemoteCdpEvent(event);
      }
    }
  }

  async disconnect(): Promise<void> {
    this.cleanup();
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  get connected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  private handleMessage(data: string) {
    let msg: any;
    try {
      msg = JSON.parse(data);
    } catch {
      return;
    }

    // Response to a request
    if (msg.id !== undefined && msg.id !== null) {
      const pending = this.pending.get(msg.id);
      if (pending) {
        this.pending.delete(msg.id);
        clearTimeout(pending.timer);
        if (msg.error) {
          pending.reject(new RpcError(msg.error.code, msg.error.message, msg.error.data));
        } else {
          pending.resolve(msg.result);
        }
      }
      return;
    }

    // Server notification
    if (msg.method) {
      const handlers = this.eventHandlers.get(msg.method);
      if (handlers) {
        for (const h of handlers) {
          try {
            h(msg.params);
          } catch {
            /* ignore */
          }
        }
      }
    }
  }

  private cleanup() {
    for (const [, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(new Error('Connection closed'));
    }
    this.pending.clear();
    this.remoteCdpSubscriptions.clear();
    this.remoteCdpSubscriptionsInFlight.clear();
  }

  private isCdpEvent(event: string): boolean {
    return /^[A-Z][A-Za-z0-9]*\.[A-Za-z0-9]+$/.test(event);
  }

  private hasHandlers(event: string): boolean {
    return (this.eventHandlers.get(event)?.size ?? 0) > 0;
  }

  private async resubscribeRemoteCdpEvents(): Promise<void> {
    for (const event of this.eventHandlers.keys()) {
      if (this.isCdpEvent(event) && this.hasHandlers(event)) {
        await this.ensureRemoteCdpSubscribed(event);
      }
    }
  }

  private async ensureRemoteCdpSubscribed(event: string): Promise<void> {
    if (!this.connected || !this.hasHandlers(event) || this.remoteCdpSubscriptions.has(event)) {
      return;
    }

    const inFlight = this.remoteCdpSubscriptionsInFlight.get(event);
    if (inFlight) {
      await inFlight;
      return;
    }

    const operation = this.call('cdp.subscribe', { event })
      .then(() => {
        if (this.connected && this.hasHandlers(event)) {
          this.remoteCdpSubscriptions.add(event);
        }
      })
      .catch(() => {
        // Best-effort auto-subscribe for bridge notifications.
      })
      .finally(() => {
        this.remoteCdpSubscriptionsInFlight.delete(event);
        if (!this.hasHandlers(event)) {
          void this.unsubscribeRemoteCdpEvent(event);
        }
      });

    this.remoteCdpSubscriptionsInFlight.set(event, operation);
    await operation;
  }

  private async unsubscribeRemoteCdpEvent(event: string): Promise<void> {
    const inFlight = this.remoteCdpSubscriptionsInFlight.get(event);
    if (inFlight) {
      await inFlight;
    }

    if (this.hasHandlers(event)) return;

    if (!this.remoteCdpSubscriptions.has(event)) {
      return;
    }

    if (!this.connected) {
      this.remoteCdpSubscriptions.delete(event);
      return;
    }

    await this.call('cdp.unsubscribe', { event }).catch(() => {
      // Best-effort cleanup. The bridge will also drop subscriptions on session end.
    });

    if (!this.hasHandlers(event)) {
      this.remoteCdpSubscriptions.delete(event);
    }
  }
}
