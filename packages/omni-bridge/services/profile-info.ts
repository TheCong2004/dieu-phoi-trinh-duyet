import type { RpcClient } from '../rpc';

export interface BridgeProfileInfo {
  id: number;
  name: string;
  proxy?: unknown;
  tags?: string[];
  group?: string;
}

export interface InlineProxy {
  proxy_type: 'HTTPS' | 'HTTP' | 'Socks5';
  host: string;
  port: number;
  user_name?: string;
  password?: string;
}

export interface ProfileCloneOptions {
  /** New profile name (A-Za-z0-9 -_ space). Defaults to "{current} (clone)" host-side. */
  name?: string;
  /** Reuse the source profile's proxy. Default: true. Ignored when proxyId/embeddedProxy are set. */
  copyProxy?: boolean;
  /** Override: assign a saved proxy by id. */
  proxyId?: number;
  /** Override: use an inline embedded proxy. */
  embeddedProxy?: InlineProxy;
  /** When true, deletes the source profile after cloning. */
  deleteOriginal?: boolean;
  /** When deleteOriginal=true, also wipe the on-disk profile data. */
  removeProfileData?: boolean;
}

export interface ProfileCloneResult {
  id: number;
  name: string;
}

export interface ProfileSwitchProxyOptions {
  /** Switch to a saved proxy by id. */
  proxyId?: number | null;
  /** Or set an inline embedded proxy. */
  embeddedProxy?: InlineProxy;
  /** Or disable the proxy entirely. */
  disabled?: boolean;
}

/**
 * Bridge-side profile service — operates on the currently running profile.
 *
 * `info()` and `setTags()` mutate session state; `clone()` and `switchProxy()`
 * are relayed to the desktop's REST API and update the persistent profile.
 *
 * Distinct from `OmniLogin.profiles.*` (REST catalog management).
 */
export class ProfileInfoService {
  constructor(private rpc: RpcClient) {}

  info() {
    return this.rpc.call<BridgeProfileInfo>('profile.info');
  }

  setTags(tags: string[]) {
    return this.rpc.call('profile.setTags', { tags });
  }

  /**
   * Clone the currently running profile. Returns the new profile's id + name.
   *
   * The clone is created via the host's REST `POST /profiles/:id/clone`, so
   * the desktop's HTTP server must be enabled (it always is when a bridge is
   * running — the bridge launched through that same desktop).
   */
  clone(opts?: ProfileCloneOptions): Promise<ProfileCloneResult> {
    return this.rpc.call('profile.clone', { ...opts });
  }

  /**
   * Change the proxy of the currently running profile.
   *
   * The browser keeps using its current proxy until a new tab is opened or
   * the profile is restarted — proxy switching is a profile-config update,
   * not a live network swap.
   */
  switchProxy(opts: ProfileSwitchProxyOptions): Promise<void> {
    return this.rpc.call('profile.switchProxy', { ...opts });
  }
}
