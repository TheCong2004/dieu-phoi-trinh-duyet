import { z } from 'zod';

export enum OpcodeType {
  PROFILE_START = 'PROFILE_START',
  PROFILE_STOP = 'PROFILE_STOP',
  PAGE_GOTO = 'PAGE_GOTO',
  CLICK = 'CLICK',
  TYPE = 'TYPE',
  WAIT = 'WAIT',
  READ_TEXT = 'READ_TEXT',
  SCREENSHOT = 'SCREENSHOT',
  LOOP = 'LOOP',
  IF = 'IF',
  EXPORT = 'EXPORT',
}

export const PageGotoPayloadSchema = z.object({
  url: z.string().url(),
  timeoutMs: z.number().optional().default(30000),
});

export const ClickPayloadSchema = z.object({
  selector: z.string(),
  button: z.enum(['left', 'right', 'middle']).optional().default('left'),
});

export const TypePayloadSchema = z.object({
  selector: z.string(),
  text: z.string(),
  delayMs: z.number().optional().default(50),
});

export const WaitPayloadSchema = z.object({
  durationMs: z.number().min(0),
});

export const OpcodeInstructionSchema = z.object({
  id: z.string().uuid(),
  type: z.nativeEnum(OpcodeType),
  payload: z.record(z.unknown()),
});

export type OpcodeInstruction = z.infer<typeof OpcodeInstructionSchema>;

export interface ExecutionPlan {
  id: string;
  version: string;
  opcodes: OpcodeInstruction[];
  createdAt: string;
}
