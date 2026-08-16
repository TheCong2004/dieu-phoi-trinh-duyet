import { OpcodeInstruction, OpcodeType, Result } from '@neodonut/shared';
import { Session } from '@neodonut/omni-bridge';

export class OpcodeExecutor {
  constructor(private readonly session: Session) {}

  public async execute(opcodes: OpcodeInstruction[]): Promise<Result<void, Error>> {
    for (const step of opcodes) {
      const res = await this.executeStep(step);
      if (res.isErr) {
        return res;
      }
    }
    return Result.ok(undefined);
  }

  public async executeStep(step: OpcodeInstruction): Promise<Result<void, Error>> {
    try {
      switch (step.type) {
        case OpcodeType.PAGE_GOTO: {
          const url = String(step.payload.url);
          const page = this.session.page;
          await page.goto(url);
          return Result.ok(undefined);
        }
        case OpcodeType.CLICK: {
          const selector = String(step.payload.selector);
          const page = this.session.page;
          const loc = page.locator(selector);
          await loc.click();
          return Result.ok(undefined);
        }
        case OpcodeType.TYPE: {
          const selector = String(step.payload.selector);
          const text = String(step.payload.text);
          const page = this.session.page;
          const loc = page.locator(selector);
          await loc.fill(text);
          return Result.ok(undefined);
        }
        case OpcodeType.WAIT: {
          const durationMs = Number(step.payload.durationMs ?? 1000);
          await new Promise((resolve) => setTimeout(resolve, durationMs));
          return Result.ok(undefined);
        }
        default:
          return Result.err(new Error(`Unsupported opcode type: ${step.type}`));
      }
    } catch (err: unknown) {
      return Result.err(err instanceof Error ? err : new Error(String(err)));
    }
  }
}
