"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { invoke } from "@tauri-apps/api/core";

type LocalProfile = {
  id: string;
  name: string;
  browser: string;
  is_running: boolean;
  process_id?: number | null;
};

type LocalPage = {
  targetId: string;
  url: string;
  title: string;
  purpose: string;
  managed: boolean;
  state: string;
};

type PagesResponse = {
  browserPid: number;
  remoteDebuggingPort: number;
  launchGeneration: number;
  pages: LocalPage[];
};

const API = "http://127.0.0.1:10108";

async function readJson<T>(path: string, init?: RequestInit, token?: string): Promise<T> {
  const headers = new Headers(init?.headers);
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const response = await fetch(`${API}${path}`, { ...init, headers, cache: "no-store" });
  const value = await response.json();
  if (!response.ok) {
    throw new Error(value?.error?.code || "LOCAL_BROWSER_REQUEST_FAILED");
  }
  return value as T;
}

export function LocalBrowserManager() {
  const [profiles, setProfiles] = useState<LocalProfile[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [pages, setPages] = useState<PagesResponse | null>(null);
  const [newUrl, setNewUrl] = useState("https://grok.com/imagine");
  const [error, setError] = useState<string | null>(null);
  const [apiToken, setApiToken] = useState<string | undefined>();
  const selected = useMemo(() => profiles.find((p) => p.id === selectedId), [profiles, selectedId]);

  const refresh = useCallback(async () => {
    try {
      const value = await readJson<{ profiles: LocalProfile[] }>("/v1/local/browser/profiles", undefined, apiToken);
      setProfiles(value.profiles);
      setSelectedId((current) => current || value.profiles[0]?.id || "");
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "LOCAL_BROWSER_MANAGER_OFFLINE");
    }
  }, [apiToken]);

  const refreshPages = useCallback(async () => {
    if (!selectedId) return;
    try {
      setPages(await readJson<PagesResponse>(`/v1/local/browser/profiles/${encodeURIComponent(selectedId)}/pages`, undefined, apiToken));
      setError(null);
    } catch (cause) {
      setPages(null);
      setError(cause instanceof Error ? cause.message : "LOCAL_BROWSER_PAGES_UNAVAILABLE");
    }
  }, [selectedId, apiToken]);

  useEffect(() => {
    void invoke<{ api_token?: string }>("get_app_settings")
      .then((settings) => setApiToken(settings.api_token))
      .catch(() => setApiToken(undefined));
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 3000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  useEffect(() => {
    void refreshPages();
    const timer = window.setInterval(() => void refreshPages(), 2000);
    return () => window.clearInterval(timer);
  }, [refreshPages]);

  const run = async () => {
    if (!selectedId) return;
    await readJson(`/v1/local/browser/profiles/${encodeURIComponent(selectedId)}/run`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: "https://grok.com/imagine", headless: false, cold_start_only: false, browser_engine: "chromium" }),
    }, apiToken);
    await refresh();
    await refreshPages();
  };

  const stop = async () => {
    if (!selectedId) return;
    await readJson(`/v1/local/browser/profiles/${encodeURIComponent(selectedId)}/stop`, { method: "POST" }, apiToken);
    setPages(null);
    await refresh();
  };

  const createPage = async () => {
    if (!selectedId || !newUrl.trim()) return;
    await readJson(`/v1/local/browser/profiles/${encodeURIComponent(selectedId)}/pages`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: newUrl.trim(), purpose: "USER" }),
    }, apiToken);
    await refreshPages();
  };

  const closePage = async (targetId: string) => {
    if (!selectedId) return;
    await readJson(`/v1/local/browser/profiles/${encodeURIComponent(selectedId)}/pages/${encodeURIComponent(targetId)}`, { method: "DELETE" }, apiToken);
    await refreshPages();
  };

  return (
    <section className="mb-2 rounded-lg border border-border bg-card/60 p-3" aria-label="Local Browser Manager">
      <div className="flex flex-wrap items-center gap-2">
        <div className="mr-auto">
          <h2 className="text-sm font-semibold">Local Browser Manager</h2>
          <p className="text-xs text-muted-foreground">Donut Desktop · profile → CFT → managed pages</p>
        </div>
        <button type="button" className="rounded border px-2 py-1 text-xs" onClick={() => void refresh()}>Refresh</button>
        <select className="rounded border bg-background px-2 py-1 text-xs" value={selectedId} onChange={(event) => setSelectedId(event.target.value)} aria-label="Profile">
          <option value="">Select profile</option>
          {profiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name} · Chrome for Testing</option>)}
        </select>
        <button type="button" className="rounded bg-primary px-2 py-1 text-xs text-primary-foreground disabled:opacity-50" disabled={!selected || selected.is_running} onClick={() => void run()}>Start CFT</button>
        <button type="button" className="rounded border px-2 py-1 text-xs disabled:opacity-50" disabled={!selected?.is_running} onClick={() => void stop()}>Stop</button>
      </div>
      {selected && <div className="mt-2 flex flex-wrap gap-3 text-xs text-muted-foreground"><span>PID: {pages?.browserPid || selected.process_id || "—"}</span><span>CDP: {pages?.remoteDebuggingPort || "—"}</span><span>Generation: {pages?.launchGeneration || "—"}</span><span>Pages: {pages?.pages.length ?? 0}</span></div>}
      <div className="mt-2 flex gap-2"><input className="min-w-0 flex-1 rounded border bg-background px-2 py-1 text-xs" value={newUrl} onChange={(event) => setNewUrl(event.target.value)} aria-label="New page URL" /><button type="button" className="rounded border px-2 py-1 text-xs disabled:opacity-50" disabled={!selected?.is_running} onClick={() => void createPage()}>New managed page</button></div>
      <div className="mt-2 space-y-1">{pages?.pages.map((page) => <div key={page.targetId} className="flex items-center gap-2 rounded border border-border/60 px-2 py-1 text-xs"><span className={page.managed ? "text-emerald-500" : "text-muted-foreground"}>{page.managed ? "MANAGED" : "USER"}</span><span className="min-w-0 flex-1 truncate" title={page.url}>{page.title || page.url || "(blank)"}</span><span className="text-muted-foreground">{page.purpose}</span><button type="button" className="rounded border px-1.5 py-0.5 disabled:opacity-40" disabled={!page.managed} onClick={() => void closePage(page.targetId)}>Close</button></div>)}</div>
      {error && <p className="mt-2 text-xs text-destructive">{error}</p>}
    </section>
  );
}
