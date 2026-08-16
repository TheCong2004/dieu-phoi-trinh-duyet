"use client";

import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  LuPlay,
  LuRefreshCw,
  LuRotateCw,
  LuSquare,
  LuTrash2,
} from "react-icons/lu";
import { toast } from "sonner";
import { AnimatedSwitch } from "@/components/ui/animated-switch";
import {
  AnimatedTabs,
  AnimatedTabsContent,
  AnimatedTabsList,
  AnimatedTabsTrigger,
} from "@/components/ui/animated-tabs";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { translateBackendError } from "@/lib/backend-errors";
import type {
  PoolSelectionStrategy,
  VpnConfig,
  VpnLease,
  VpnPool,
  VpnPoolRuntime,
  VpnProviderAccount,
  VpnProviderCountry,
  VpnProviderKind,
} from "@/types";

interface Props {
  vpnConfigs: VpnConfig[];
}

export function VpnPoolManagement({ vpnConfigs }: Props) {
  const { t } = useTranslation();
  const [accounts, setAccounts] = useState<VpnProviderAccount[]>([]);
  const [pools, setPools] = useState<VpnPool[]>([]);
  const [runtimes, setRuntimes] = useState<VpnPoolRuntime[]>([]);
  const [leases, setLeases] = useState<VpnLease[]>([]);
  const [busy, setBusy] = useState(false);
  const [provider, setProvider] = useState<VpnProviderKind>("nordvpn");
  const [label, setLabel] = useState("");
  const [username, setUsername] = useState("");
  const [secret, setSecret] = useState("");
  const [importAccount, setImportAccount] = useState("");
  const [importCountry, setImportCountry] = useState("");
  const [importCount, setImportCount] = useState(1);
  const [importSummary, setImportSummary] = useState("");
  const [countries, setCountries] = useState<VpnProviderCountry[]>([]);
  const [poolName, setPoolName] = useState("");
  const [editingPoolId, setEditingPoolId] = useState<string | null>(null);
  const [selectedConfigs, setSelectedConfigs] = useState<string[]>([]);
  const [rotationEnabled, setRotationEnabled] = useState(false);
  const [rotationInterval, setRotationInterval] = useState(600);
  const [poolProviderFilter, setPoolProviderFilter] = useState<
    VpnProviderKind[]
  >([]);
  const [poolCountry, setPoolCountry] = useState("");
  const [poolStrategy, setPoolStrategy] = useState<PoolSelectionStrategy>(
    "least_recently_used",
  );
  const [leasePool, setLeasePool] = useState("");
  const [leaseTtl, setLeaseTtl] = useState(0);
  const [now, setNow] = useState(() => Date.now());

  const load = useCallback(async () => {
    const [nextAccounts, nextPools, nextRuntimes, nextLeases] =
      await Promise.all([
        invoke<VpnProviderAccount[]>("list_vpn_provider_accounts"),
        invoke<VpnPool[]>("list_vpn_pools"),
        invoke<VpnPoolRuntime[]>("list_vpn_pool_runtimes"),
        invoke<VpnLease[]>("list_vpn_leases"),
      ]);
    setAccounts(nextAccounts);
    setPools(nextPools);
    setRuntimes(nextRuntimes);
    setLeases(nextLeases);
  }, []);

  useEffect(() => {
    void load();
    const events = [
      "vpn-provider-accounts-updated",
      "vpn-pools-updated",
      "vpn-pool-runtime-updated",
      "vpn-leases-updated",
    ];
    const unlisteners = events.map((event) => listen(event, () => void load()));
    return () => {
      for (const unlisten of unlisteners) void unlisten.then((fn) => fn());
    };
  }, [load]);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  const run = async (operation: () => Promise<unknown>, success: string) => {
    setBusy(true);
    try {
      await operation();
      toast.success(success);
      await load();
    } catch (error) {
      toast.error(translateBackendError(t, error));
    } finally {
      setBusy(false);
    }
  };

  const activeRuntime = useMemo(
    () => new Map(runtimes.map((runtime) => [runtime.pool_id, runtime])),
    [runtimes],
  );

  const addAccount = () =>
    run(async () => {
      if (provider === "nordvpn")
        await invoke("add_nordvpn_account", { label, token: secret });
      else
        await invoke("add_pia_account", { label, username, password: secret });
      setLabel("");
      setUsername("");
      setSecret("");
    }, t("vpnPool.accounts.added"));

  const importConfigs = () =>
    run(async () => {
      const result = await invoke<{ configs: VpnConfig[]; failed: number }>(
        "import_vpn_provider_configs",
        {
          provider,
          request: {
            account_ids: [importAccount],
            country: importCountry || null,
            country_id:
              countries.find((country) => country.code === importCountry)?.id ??
              null,
            count: importCount,
          },
        },
      );
      setImportSummary(
        t("vpnPool.import.summary", {
          imported: result.configs.length,
          failed: result.failed,
        }),
      );
    }, t("vpnPool.import.completed"));

  const savePool = () =>
    run(
      async () => {
        const request = {
          name: poolName,
          provider_filter: poolProviderFilter,
          country: poolCountry || null,
          config_ids: selectedConfigs,
          rotation_enabled: rotationEnabled,
          rotation_interval_sec: rotationEnabled ? rotationInterval : null,
          rotation_mode: "safe",
          strategy: poolStrategy,
          enabled: true,
        };
        if (editingPoolId)
          await invoke("update_vpn_pool", { poolId: editingPoolId, request });
        else await invoke("create_vpn_pool", { request });
        setPoolName("");
        setSelectedConfigs([]);
        setEditingPoolId(null);
      },
      t(editingPoolId ? "vpnPool.pools.updated" : "vpnPool.pools.created"),
    );

  const editPool = (pool: VpnPool) => {
    setEditingPoolId(pool.id);
    setPoolName(pool.name);
    setSelectedConfigs(pool.config_ids);
    setRotationEnabled(pool.rotation_enabled);
    setRotationInterval(pool.rotation_interval_sec ?? 600);
    setPoolProviderFilter(pool.provider_filter);
    setPoolCountry(pool.country ?? "");
    setPoolStrategy(pool.strategy);
  };

  useEffect(() => {
    if (!importAccount) {
      setCountries([]);
      return;
    }
    void invoke<VpnProviderCountry[]>("list_vpn_provider_countries", {
      provider,
    })
      .then(setCountries)
      .catch((error) => toast.error(translateBackendError(t, error)));
  }, [importAccount, provider, t]);

  return (
    <AnimatedTabs defaultValue="pools" className="flex min-h-0 flex-1 flex-col">
      <AnimatedTabsList>
        <AnimatedTabsTrigger value="pools">
          {t("vpnPool.tabs.pools")}
        </AnimatedTabsTrigger>
        <AnimatedTabsTrigger value="accounts">
          {t("vpnPool.tabs.accounts")}
        </AnimatedTabsTrigger>
        <AnimatedTabsTrigger value="leases">
          {t("vpnPool.tabs.leases")}
        </AnimatedTabsTrigger>
      </AnimatedTabsList>

      <AnimatedTabsContent
        value="pools"
        className="space-y-4 overflow-auto pt-4"
      >
        <section className="space-y-3 rounded-lg border border-border p-4">
          <h3 className="font-medium">
            {t(
              editingPoolId
                ? "vpnPool.pools.editTitle"
                : "vpnPool.pools.createTitle",
            )}
          </h3>
          <Input
            value={poolName}
            onChange={(event) => setPoolName(event.target.value)}
            placeholder={t("vpnPool.pools.namePlaceholder")}
          />
          <div className="grid gap-2 sm:grid-cols-2">
            {vpnConfigs.map((config) => (
              <Label
                key={config.id}
                className="flex items-center gap-2 rounded-md border border-border p-2"
              >
                <Checkbox
                  checked={selectedConfigs.includes(config.id)}
                  onCheckedChange={(checked) =>
                    setSelectedConfigs((current) =>
                      checked
                        ? [...current, config.id]
                        : current.filter((id) => id !== config.id),
                    )
                  }
                />
                <span className="truncate">{config.name}</span>
              </Label>
            ))}
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex flex-wrap gap-4">
              {(["nordvpn", "piavpn"] as VpnProviderKind[]).map((kind) => (
                <Label key={kind} className="flex items-center gap-2">
                  <Checkbox
                    checked={poolProviderFilter.includes(kind)}
                    onCheckedChange={(checked) =>
                      setPoolProviderFilter((current) =>
                        checked
                          ? [...current, kind]
                          : current.filter((value) => value !== kind),
                      )
                    }
                  />
                  {kind === "nordvpn" ? "NordVPN" : "PIA"}
                </Label>
              ))}
            </div>
            <Input
              className="w-40"
              value={poolCountry}
              onChange={(event) => setPoolCountry(event.target.value)}
              placeholder={t("vpnPool.pools.countryPlaceholder")}
            />
            <select
              className="h-9 rounded-md border border-border bg-background px-3 text-sm"
              value={poolStrategy}
              onChange={(event) =>
                setPoolStrategy(event.target.value as PoolSelectionStrategy)
              }
            >
              <option value="least_recently_used">
                {t("vpnPool.pools.strategyLru")}
              </option>
              <option value="round_robin">
                {t("vpnPool.pools.strategyRoundRobin")}
              </option>
            </select>
            <Label className="flex items-center gap-2">
              <AnimatedSwitch
                checked={rotationEnabled}
                onCheckedChange={setRotationEnabled}
              />
              {t("vpnPool.pools.rotation")}
            </Label>
            {rotationEnabled && (
              <Input
                className="w-28"
                type="number"
                min={30}
                max={86400}
                value={rotationInterval}
                onChange={(event) =>
                  setRotationInterval(Number(event.target.value))
                }
                aria-label={t("vpnPool.pools.rotationInterval")}
              />
            )}
          </div>
          <Button
            disabled={busy || !poolName.trim() || selectedConfigs.length === 0}
            onClick={() => void savePool()}
          >
            {t(editingPoolId ? "vpnPool.pools.update" : "vpnPool.pools.create")}
          </Button>
          {editingPoolId && (
            <Button variant="ghost" onClick={() => setEditingPoolId(null)}>
              {t("common.buttons.cancel")}
            </Button>
          )}
        </section>
        <div className="space-y-2">
          {pools.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              {t("vpnPool.pools.empty")}
            </p>
          ) : (
            pools.map((pool) => {
              const runtime = activeRuntime.get(pool.id);
              const hasActiveLease = leases.some(
                (lease) =>
                  lease.pool_id === pool.id &&
                  ["provisioning", "active", "releasing"].includes(
                    lease.status,
                  ),
              );
              return (
                <div
                  key={pool.id}
                  className="flex flex-wrap items-center gap-3 rounded-lg border border-border p-3"
                >
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className="truncate font-medium">{pool.name}</span>
                      <Badge variant="outline">
                        {t(`vpnPool.status.${runtime?.status ?? "stopped"}`)}
                      </Badge>
                    </div>
                    <p className="text-xs text-muted-foreground">
                      {runtime?.exit_ip ?? t("vpnPool.common.notAvailable")} ·{" "}
                      {runtime?.exit_country ??
                        t("vpnPool.common.notAvailable")}{" "}
                      · {pool.config_ids.length} {t("vpnPool.pools.configs")}
                    </p>
                    {runtime?.health.latency_ms != null && (
                      <p className="text-xs text-muted-foreground">
                        {t("vpnPool.pools.latency", {
                          latency: runtime.health.latency_ms,
                        })}
                        {runtime.next_rotation_at
                          ? ` · ${t("vpnPool.pools.nextRotation", {
                              seconds: Math.max(
                                0,
                                runtime.next_rotation_at -
                                  Math.floor(now / 1000),
                              ),
                            })}`
                          : ""}
                      </p>
                    )}
                    {runtime?.last_error_code && (
                      <p className="text-xs text-destructive">
                        {translateBackendError(
                          t,
                          JSON.stringify({ code: runtime.last_error_code }),
                        )}
                      </p>
                    )}
                  </div>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={busy || hasActiveLease}
                    onClick={() => editPool(pool)}
                  >
                    {t("common.buttons.edit")}
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={busy || hasActiveLease}
                    onClick={() =>
                      void run(
                        () => invoke("start_vpn_pool", { poolId: pool.id }),
                        t("vpnPool.pools.started"),
                      )
                    }
                  >
                    <LuPlay />
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={busy || hasActiveLease}
                    onClick={() =>
                      void run(
                        () => invoke("rotate_vpn_pool", { poolId: pool.id }),
                        t("vpnPool.pools.rotated"),
                      )
                    }
                  >
                    <LuRotateCw />
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={busy || hasActiveLease}
                    onClick={() =>
                      void run(
                        () => invoke("stop_vpn_pool", { poolId: pool.id }),
                        t("vpnPool.pools.stopped"),
                      )
                    }
                  >
                    <LuSquare />
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={busy || hasActiveLease}
                    onClick={() =>
                      void run(
                        () => invoke("delete_vpn_pool", { poolId: pool.id }),
                        t("vpnPool.pools.deleted"),
                      )
                    }
                  >
                    <LuTrash2 />
                  </Button>
                </div>
              );
            })
          )}
        </div>
      </AnimatedTabsContent>

      <AnimatedTabsContent
        value="accounts"
        className="space-y-4 overflow-auto pt-4"
      >
        <section className="space-y-3 rounded-lg border border-border p-4">
          <h3 className="font-medium">{t("vpnPool.accounts.addTitle")}</h3>
          <select
            className="h-9 rounded-md border border-border bg-background px-3 text-sm"
            value={provider}
            onChange={(event) =>
              setProvider(event.target.value as VpnProviderKind)
            }
          >
            <option value="nordvpn">NordVPN</option>
            <option value="piavpn">PIA</option>
          </select>
          <Input
            value={label}
            onChange={(event) => setLabel(event.target.value)}
            placeholder={t("vpnPool.accounts.labelPlaceholder")}
          />
          {provider === "piavpn" && (
            <Input
              value={username}
              onChange={(event) => setUsername(event.target.value)}
              placeholder={t("vpnPool.accounts.usernamePlaceholder")}
            />
          )}
          <Input
            type="password"
            value={secret}
            onChange={(event) => setSecret(event.target.value)}
            placeholder={
              provider === "nordvpn"
                ? t("vpnPool.accounts.tokenPlaceholder")
                : t("vpnPool.accounts.passwordPlaceholder")
            }
          />
          <Button
            disabled={busy || !label.trim() || !secret}
            onClick={() => void addAccount()}
          >
            {t("vpnPool.accounts.add")}
          </Button>
        </section>
        <div className="space-y-2">
          {accounts.map((account) => (
            <div
              key={account.id}
              className="flex items-center gap-3 rounded-lg border border-border p-3"
            >
              <div className="min-w-0 flex-1">
                <p className="truncate font-medium">{account.label}</p>
                <p className="text-xs text-muted-foreground">
                  {account.provider} · {t(`vpnPool.status.${account.status}`)} ·{" "}
                  {t("vpnPool.accounts.cap", { count: account.connection_cap })}
                </p>
              </div>
              <Button
                size="sm"
                variant="ghost"
                disabled={busy}
                onClick={() =>
                  void run(
                    () =>
                      invoke("validate_vpn_provider_account", {
                        accountId: account.id,
                      }),
                    t("vpnPool.accounts.validated"),
                  )
                }
              >
                <LuRefreshCw />
              </Button>
              <Button
                size="sm"
                variant="ghost"
                disabled={busy}
                onClick={() =>
                  void run(
                    () =>
                      invoke("delete_vpn_provider_account", {
                        accountId: account.id,
                      }),
                    t("vpnPool.accounts.deleted"),
                  )
                }
              >
                <LuTrash2 />
              </Button>
            </div>
          ))}
        </div>
        <section className="space-y-3 rounded-lg border border-border p-4">
          <h3 className="font-medium">{t("vpnPool.import.title")}</h3>
          <select
            className="h-9 w-full rounded-md border border-border bg-background px-3 text-sm"
            value={importAccount}
            onChange={(event) => {
              const id = event.target.value;
              setImportAccount(id);
              const account = accounts.find((item) => item.id === id);
              if (account) setProvider(account.provider);
            }}
          >
            <option value="">{t("vpnPool.import.selectAccount")}</option>
            {accounts.map((account) => (
              <option key={account.id} value={account.id}>
                {account.label}
              </option>
            ))}
          </select>
          <select
            className="h-9 w-full rounded-md border border-border bg-background px-3 text-sm"
            value={importCountry}
            onChange={(event) => setImportCountry(event.target.value)}
          >
            <option value="">{t("vpnPool.import.selectCountry")}</option>
            {countries.map((country) => (
              <option
                key={`${country.id ?? country.code}`}
                value={country.code}
              >
                {country.name} ({country.server_count})
              </option>
            ))}
          </select>
          <Input
            type="number"
            min={1}
            max={500}
            value={importCount}
            onChange={(event) => setImportCount(Number(event.target.value))}
            aria-label={t("vpnPool.import.count")}
          />
          <Button
            disabled={busy || !importAccount}
            onClick={() => void importConfigs()}
          >
            {t("vpnPool.import.action")}
          </Button>
          {importSummary && (
            <p className="text-sm text-muted-foreground">{importSummary}</p>
          )}
        </section>
      </AnimatedTabsContent>

      <AnimatedTabsContent
        value="leases"
        className="space-y-4 overflow-auto pt-4"
      >
        <section className="flex flex-wrap gap-2 rounded-lg border border-border p-4">
          <select
            className="h-9 min-w-48 rounded-md border border-border bg-background px-3 text-sm"
            value={leasePool}
            onChange={(event) => setLeasePool(event.target.value)}
          >
            <option value="">{t("vpnPool.leases.anyPool")}</option>
            {pools.map((pool) => (
              <option key={pool.id} value={pool.id}>
                {pool.name}
              </option>
            ))}
          </select>
          <Input
            className="w-32"
            type="number"
            min={0}
            max={86400}
            value={leaseTtl}
            onChange={(event) => setLeaseTtl(Number(event.target.value))}
            aria-label={t("vpnPool.leases.ttl")}
          />
          <Button
            disabled={busy}
            onClick={() =>
              void run(
                () =>
                  invoke("acquire_vpn_lease", {
                    request: {
                      pool_id: leasePool || null,
                      country: null,
                      providers: [],
                      profile_id: null,
                      ttl_seconds: leaseTtl,
                      protocol: "socks5",
                      wait_when_full: true,
                      max_wait_seconds: 60,
                    },
                  }),
                t("vpnPool.leases.acquired"),
              )
            }
          >
            {t("vpnPool.leases.acquire")}
          </Button>
        </section>
        <div className="space-y-2">
          {leases.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              {t("vpnPool.leases.empty")}
            </p>
          ) : (
            leases.map((lease) => (
              <div
                key={lease.id}
                className="flex items-center gap-3 rounded-lg border border-border p-3"
              >
                <div className="min-w-0 flex-1">
                  <p className="truncate font-mono text-xs">{lease.id}</p>
                  <p className="text-xs text-muted-foreground">
                    {lease.provider} ·{" "}
                    {lease.country ?? t("vpnPool.common.notAvailable")} ·{" "}
                    {lease.local_host}:{lease.local_port} ·{" "}
                    {lease.exit_ip ?? t("vpnPool.common.notAvailable")}
                  </p>
                  <p className="text-xs text-muted-foreground">
                    {t("vpnPool.leases.profile")}:{" "}
                    {lease.profile_id ?? t("vpnPool.common.notAvailable")} ·{" "}
                    {new Date(lease.created_at * 1000).toLocaleString()} ·{" "}
                    {lease.expires_at
                      ? new Date(lease.expires_at * 1000).toLocaleString()
                      : t("vpnPool.leases.noExpiry")}
                  </p>
                </div>
                <Badge variant="outline">
                  {t(`vpnPool.status.${lease.status}`)}
                </Badge>
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy}
                  onClick={() =>
                    void run(
                      () => invoke("release_vpn_lease", { leaseId: lease.id }),
                      t("vpnPool.leases.released"),
                    )
                  }
                >
                  {t("vpnPool.leases.release")}
                </Button>
              </div>
            ))
          )}
        </div>
      </AnimatedTabsContent>
    </AnimatedTabs>
  );
}
