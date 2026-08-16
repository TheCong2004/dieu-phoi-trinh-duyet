import type { RpcClient } from '../rpc';

export interface TotpResult {
  /** 6-digit (default) one-time code. */
  code: string;
  /** Seconds until this code expires. */
  timeRemaining: number;
}

/**
 * Generate TOTP codes (RFC 6238) from a Base32-encoded shared secret.
 *
 * Spaces in the secret are stripped automatically — convenient for the
 * common 4-char-grouped display format ("ABCD EFGH IJKL MNOP").
 *
 * The bridge uses `otplib`'s `authenticator` and an internally-tracked time
 * offset to handle clock drift, so codes generated here match Google
 * Authenticator / Authy / 1Password.
 */
export class TotpService {
  constructor(private rpc: RpcClient) {}

  /** Generate a code now. */
  generate(secret: string): Promise<TotpResult> {
    return this.rpc.call('totp.generate', { secret });
  }

  /** Convenience: just the code string. */
  async code(secret: string): Promise<string> {
    const r = await this.generate(secret);
    return r.code;
  }
}
