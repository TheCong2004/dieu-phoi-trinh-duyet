import { ExecutionPlan, OpcodeInstruction, OpcodeType, Result } from '@neodonut/shared';

export interface FlowiseNodeData {
  id: string;
  type: string;
  inputs?: Record<string, unknown>;
}

export interface FlowiseGraph {
  nodes: FlowiseNodeData[];
}

function generateUUID(): string {
  if (typeof globalThis !== 'undefined' && globalThis.crypto?.randomUUID) {
    return globalThis.crypto.randomUUID();
  }
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export class FlowiseOpcodeCompiler {
  /**
   * Compiles Flowise visual canvas JSON graph into deterministic Opcode ExecutionPlan.
   * Universal compatibility across Browser WebViews, Tauri, and Node.js.
   */
  public compile(graph: FlowiseGraph): Result<ExecutionPlan, Error> {
    try {
      const opcodes: OpcodeInstruction[] = [];

      for (const node of graph.nodes) {
        const opcode = this.mapNodeToOpcode(node);
        if (opcode.isOk) {
          opcodes.push(opcode.value);
        }
      }

      const plan: ExecutionPlan = {
        id: generateUUID(),
        version: '1.0.0',
        opcodes,
        createdAt: new Date().toISOString(),
      };

      return Result.ok(plan);
    } catch (err: unknown) {
      return Result.err(err instanceof Error ? err : new Error(String(err)));
    }
  }

  private mapNodeToOpcode(node: FlowiseNodeData): Result<OpcodeInstruction, Error> {
    const inputs = node.inputs ?? {};

    switch (node.type) {
      case 'pageGotoNode':
      case 'GOTO':
        return Result.ok({
          id: generateUUID(),
          type: OpcodeType.PAGE_GOTO,
          payload: { url: String(inputs.url ?? 'https://google.com') },
        });

      case 'clickNode':
      case 'CLICK':
        return Result.ok({
          id: generateUUID(),
          type: OpcodeType.CLICK,
          payload: { selector: String(inputs.selector ?? 'button') },
        });

      case 'typeNode':
      case 'TYPE':
        return Result.ok({
          id: generateUUID(),
          type: OpcodeType.TYPE,
          payload: { selector: String(inputs.selector ?? 'input'), text: String(inputs.text ?? '') },
        });

      case 'waitNode':
      case 'WAIT':
        return Result.ok({
          id: generateUUID(),
          type: OpcodeType.WAIT,
          payload: { durationMs: Number(inputs.durationMs ?? 1000) },
        });

      default:
        return Result.err(new Error(`Unknown canvas node type: ${node.type}`));
    }
  }
}
