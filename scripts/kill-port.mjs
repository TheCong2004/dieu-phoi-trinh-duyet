import { execSync } from 'node:child_process';

const PORTS_TO_KILL = [12341];

for (const port of PORTS_TO_KILL) {
  try {
    if (process.platform === 'win32') {
      try {
        const output = execSync('netstat -ano', { stdio: ['pipe', 'pipe', 'pipe'] }).toString();
        const lines = output.split('\n');
        const pids = new Set();
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed.includes('LISTENING')) continue;
          const parts = trimmed.split(/\s+/);
          const localAddr = parts[1] || '';
          const pid = parts[parts.length - 1];
          if ((localAddr.endsWith(`:${port}`) || localAddr.endsWith(`:${port}\r`)) && pid && /^\d+$/.test(pid) && pid !== '0') {
            if (Number.parseInt(pid, 10) !== process.pid) {
              pids.add(pid);
            }
          }
        }
        for (const pid of pids) {
          try {
            execSync(`taskkill /F /PID ${pid} >NUL 2>&1`, { shell: true });
            console.log(`[kill-port] Terminated process ${pid} on port ${port}`);
          } catch {
            // Ignore if already exited
          }
        }
      } catch {
        // Ignore netstat error when no listening port found
      }
    } else {
      try {
        execSync(`lsof -ti:${port} | xargs kill -9 2>/dev/null || true`);
      } catch {
        // Ignore
      }
    }
  } catch {
    // Ignore all errors
  }
}

process.exit(0);
