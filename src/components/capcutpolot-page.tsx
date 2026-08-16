"use client";

import { useEffect, useState, useRef } from "react";
import { launchCapCutPolot } from "@/lib/capcutpolot-service";
import { Button } from "./ui/button";
import { Badge } from "./ui/badge";
import { LuVideo, LuTerminal, LuRefreshCw, LuSquare, LuPlay, LuShieldCheck } from "react-icons/lu";

interface LogEntry {
  id: string;
  timestamp: string;
  type: "stdout" | "stderr" | "system";
  text: string;
}

export function CapCutPolotPage() {
  const [activeView, setActiveView] = useState<"app" | "terminal">("app");
  const [appUrl, setAppUrl] = useState<string>("http://127.0.0.1:30000");
  const [iframeKey, setIframeKey] = useState<number>(0);
  const [isRunning, setIsRunning] = useState(false);
  const [pid, setPid] = useState<number | null>(null);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [childProcess, setChildProcess] = useState<any>(null);
  const autoStartRef = useRef(false);

  const addLog = (text: string, type: "stdout" | "stderr" | "system" = "stdout") => {
    setLogs((prev) => [
      ...prev,
      {
        id: Math.random().toString(36).substring(7),
        timestamp: new Date().toLocaleTimeString(),
        type,
        text,
      },
    ]);
  };

  const startSidecar = async () => {
    if (isRunning) return;
    setIsRunning(true);
    addLog("Đang kích hoạt CapCutPolot Engine ngầm...", "system");

    try {
      const child = await launchCapCutPolot({
        mode: "auto",
        onStdout: (line) => addLog(line, "stdout"),
        onStderr: (line) => addLog(line, "stderr"),
      });

      setChildProcess(child);
      setPid(child.pid);
      addLog(`✓ CapCutPolot Engine sẵn sàng (PID: ${child.pid})`, "system");
    } catch (err: unknown) {
      addLog(`Lỗi khởi chạy: ${err instanceof Error ? err.message : String(err)}`, "stderr");
      setIsRunning(false);
    }
  };

  useEffect(() => {
    if (!autoStartRef.current) {
      autoStartRef.current = true;
      void startSidecar();
    }
  }, []);

  const stopSidecar = async () => {
    if (childProcess) {
      try {
        await childProcess.kill();
        addLog(`Đã dừng CapCutPolot Engine (PID: ${pid})`, "system");
      } catch (err) {
        addLog(`Lỗi dừng tiến trình: ${err}`, "stderr");
      }
    }
    setIsRunning(false);
    setPid(null);
    setChildProcess(null);
  };

  return (
    <div className="flex h-full w-full flex-col bg-background overflow-hidden">
      {/* Top Bar Navigation */}
      <div className="flex h-11 shrink-0 items-center justify-between border-b border-border bg-card px-4">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <LuVideo className="size-4 text-purple-500" />
            <span className="text-sm font-semibold tracking-tight">CapCutPolot Agent</span>
          </div>

          <div className="h-4 w-px bg-border" />

          <Badge variant="outline" className="flex items-center gap-1.5 text-xs font-normal border-purple-500/30 bg-purple-500/5 text-purple-600 dark:text-purple-400">
            <span className={`size-1.5 rounded-full ${isRunning ? "bg-emerald-500 animate-pulse" : "bg-zinc-400"}`} />
            {isRunning ? `Online (PID ${pid})` : "Đang khởi tạo..."}
          </Badge>

          <Badge variant="secondary" className="flex items-center gap-1 text-xs font-normal text-muted-foreground">
            <LuShieldCheck className="size-3 text-emerald-500" />
            CDP + Playwright Bridge
          </Badge>
        </div>

        <div className="flex items-center gap-2">
          {/* View Toggle */}
          <div className="flex items-center gap-1 rounded-md bg-muted p-0.5">
            <Button
              size="sm"
              variant={activeView === "app" ? "secondary" : "ghost"}
              onClick={() => setActiveView("app")}
              className={`h-6 text-xs px-2.5 font-medium ${activeView === "app" ? "bg-background shadow-xs text-foreground" : "text-muted-foreground"}`}
            >
              Giao diện App
            </Button>
            <Button
              size="sm"
              variant={activeView === "terminal" ? "secondary" : "ghost"}
              onClick={() => setActiveView("terminal")}
              className={`h-6 text-xs px-2.5 font-medium ${activeView === "terminal" ? "bg-background shadow-xs text-foreground" : "text-muted-foreground"}`}
            >
              Log System ({logs.length})
            </Button>
          </div>

          <Button
            size="icon"
            variant="ghost"
            className="size-7"
            onClick={() => setIframeKey((k) => k + 1)}
            title="Tải lại Giao diện"
          >
            <LuRefreshCw className="size-3.5" />
          </Button>

          {isRunning ? (
            <Button size="sm" variant="ghost" onClick={stopSidecar} className="h-7 text-xs text-red-500 hover:text-red-600 hover:bg-red-500/10 gap-1 font-normal">
              <LuSquare className="size-3" />
              Dừng Engine
            </Button>
          ) : (
            <Button size="sm" onClick={startSidecar} className="h-7 text-xs bg-purple-600 hover:bg-purple-700 text-white gap-1 font-medium">
              <LuPlay className="size-3" />
              Bật Engine
            </Button>
          )}
        </div>
      </div>

      {/* Main Content Area */}
      <div className="flex flex-1 flex-col overflow-hidden relative">
        {activeView === "app" ? (
          <iframe
            key={iframeKey}
            src={appUrl}
            className="w-full h-full border-0 bg-background"
            title="CapCutPolot Native App View"
          />
        ) : (
          <div className="flex flex-1 flex-col bg-zinc-950 text-zinc-100 overflow-hidden font-mono text-xs">
            <div className="flex items-center justify-between border-b border-zinc-800 bg-zinc-900/80 px-4 py-2">
              <div className="flex items-center gap-2 text-zinc-400">
                <LuTerminal className="size-4 text-purple-400" />
                <span className="font-semibold text-zinc-200">CapCutPolot Sidecar Real-time Console</span>
              </div>
              <Button size="sm" variant="ghost" onClick={() => setLogs([])} className="h-6 text-[11px] text-zinc-400 hover:text-zinc-100">
                Xóa Log
              </Button>
            </div>

            <div className="flex-1 overflow-y-auto p-4 space-y-1.5">
              {logs.length === 0 ? (
                <div className="text-zinc-600 italic">Chưa có dữ liệu log...</div>
              ) : (
                logs.map((log) => (
                  <div key={log.id} className="flex gap-2 leading-relaxed hover:bg-zinc-900/50 rounded px-1">
                    <span className="text-zinc-500 shrink-0">[{log.timestamp}]</span>
                    <span
                      className={
                        log.type === "stderr"
                          ? "text-red-400 font-semibold"
                          : log.type === "system"
                          ? "text-purple-400 font-semibold"
                          : "text-emerald-300"
                      }
                    >
                      {log.text}
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
