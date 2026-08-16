export interface CapCutPolotRunOptions {
  mode?: string;
  input?: string;
  args?: string[];
  onStdout?: (data: string) => void;
  onStderr?: (data: string) => void;
}

/**
 * Launch CapCutPolot Sidecar binary bundled with DonutBrowser in headless background mode.
 */
export async function launchCapCutPolot(options: CapCutPolotRunOptions = {}) {
  if (typeof window === "undefined" || !("__TAURI_INTERNALS__" in window)) {
    throw new Error("CapCutPolot Sidecar chỉ hoạt động trong môi trường Desktop App (Tauri).");
  }

  const cliArgs: string[] = [];

  if (options.mode) {
    cliArgs.push("--mode", options.mode);
  }
  if (options.input) {
    cliArgs.push("--input", options.input);
  }
  if (options.args && options.args.length > 0) {
    cliArgs.push(...options.args);
  }

  try {
    const { Command } = await import("@tauri-apps/plugin-shell");
    const sidecarCommand = Command.sidecar("binaries/capcutpolot", cliArgs, {
      env: {
        ARTCRAFT_HEADLESS: "1",
        HEADLESS: "1",
        CAPCUT_MATE_AUTO_START: "1",
      },
    });

    if (options.onStdout) {
      sidecarCommand.stdout.on("data", (line: string) => {
        options.onStdout?.(line);
      });
    }

    if (options.onStderr) {
      sidecarCommand.stderr.on("data", (line: string) => {
        options.onStderr?.(line);
      });
    }

    const child = await sidecarCommand.spawn();
    console.log(`[CapCutPolot Sidecar] Started in headless mode with PID: ${child.pid}`);
    return child;
  } catch (error) {
    console.error("[CapCutPolot Sidecar] Failed to launch:", error);
    throw error;
  }
}
