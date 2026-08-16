import { ProfileManager } from '@neodonut/anti-detect';
import { FlowiseGraph, FlowiseOpcodeCompiler, OpcodeExecutor } from '@neodonut/workflow-engine';
import { DeviceBinding, EncryptedPackage, PackageCrypto, Result } from '@neodonut/shared';

export class NeoDonutEngine {
  private static instance: NeoDonutEngine;
  private readonly profileManager = new ProfileManager();
  private readonly compiler = new FlowiseOpcodeCompiler();

  public static getInstance(): NeoDonutEngine {
    if (!NeoDonutEngine.instance) {
      NeoDonutEngine.instance = new NeoDonutEngine();
    }
    return NeoDonutEngine.instance;
  }

  /**
   * Compiles Flowise Visual Canvas Graph -> Execution Plan -> AES-256-GCM + Ed25519 Package
   */
  public async compileAndPackage(
    graph: FlowiseGraph,
    secretKeyHex: string
  ): Promise<Result<EncryptedPackage, Error>> {
    const planRes = this.compiler.compile(graph);
    if (planRes.isErr) {
      return Result.err(planRes.error);
    }

    const payloadJson = JSON.stringify(planRes.value);
    return await PackageCrypto.encryptAndSign(payloadJson, secretKeyHex);
  }

  /**
   * Decrypts encrypted package in memory -> Verifies Hardware Binding -> Executes Opcodes over CDP Session
   */
  public async executePackage(
    profileId: string,
    cdpPort: number,
    pkg: EncryptedPackage,
    secretKeyHex: string
  ): Promise<Result<void, Error>> {
    // 1. Check device hardware binding
    const hwUuidRes = await DeviceBinding.getHardwareUuid();
    if (hwUuidRes.isErr) {
      return Result.err(hwUuidRes.error);
    }

    // 2. Decrypt in memory
    const decryptRes = await PackageCrypto.verifyAndDecrypt(pkg, secretKeyHex);
    if (decryptRes.isErr) {
      return Result.err(decryptRes.error);
    }

    const plan = JSON.parse(decryptRes.value);

    // 3. Connect browser profile over CDP via OmniBridge
    const sessionRes = await this.profileManager.connectProfile(profileId, cdpPort);
    if (sessionRes.isErr) {
      return Result.err(sessionRes.error);
    }

    // 4. Run opcodes step-by-step
    const executor = new OpcodeExecutor(sessionRes.value.session);
    const execRes = await executor.execute(plan.opcodes);

    // 5. Cleanup session
    await this.profileManager.disconnectProfile(profileId);

    return execRes;
  }
}
