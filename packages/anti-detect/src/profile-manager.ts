import { Session } from '@neodonut/omni-bridge';
import { Result } from '@neodonut/shared';
import { FingerprintConfig, FingerprintGenerator } from './fingerprint-generator';

export interface ActiveProfileSession {
  profileId: string;
  cdpEndpointUrl: string;
  cdpPort: number;
  session: Session;
}

export class ProfileManager {
  private readonly activeProfiles = new Map<string, ActiveProfileSession>();

  /**
   * Connects to a running NeoDonut Chrome browser instance via CDP and binds an OmniBridge Session.
   */
  public async connectProfile(
    profileId: string,
    cdpPort: number,
    _fingerprint?: FingerprintConfig
  ): Promise<Result<ActiveProfileSession, Error>> {
    try {
      const cdpEndpointUrl = `ws://127.0.0.1:${cdpPort}/devtools/browser`;
      const session = new Session(cdpEndpointUrl);
      await session.connect();

      const activeSession: ActiveProfileSession = {
        profileId,
        cdpEndpointUrl,
        cdpPort,
        session,
      };

      this.activeProfiles.set(profileId, activeSession);
      return Result.ok(activeSession);
    } catch (err: unknown) {
      return Result.err(err instanceof Error ? err : new Error(String(err)));
    }
  }

  public getActiveSession(profileId: string): Result<ActiveProfileSession, Error> {
    const session = this.activeProfiles.get(profileId);
    if (!session) {
      return Result.err(new Error(`Profile ${profileId} is not active or connected`));
    }
    return Result.ok(session);
  }

  public async disconnectProfile(profileId: string): Promise<Result<void, Error>> {
    try {
      const active = this.activeProfiles.get(profileId);
      if (active) {
        await active.session.disconnect();
        this.activeProfiles.delete(profileId);
      }
      return Result.ok(undefined);
    } catch (err: unknown) {
      return Result.err(err instanceof Error ? err : new Error(String(err)));
    }
  }
}
