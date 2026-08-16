"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { LuRefreshCw } from "react-icons/lu";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { AnimatedSwitch } from "./ui/animated-switch";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./ui/table";

const FLOWISE_API = "http://localhost:3000/api/v1";

interface FlowiseWorkflow {
  id: string;
  name: string;
  flowData: string;
}

interface ScheduleRecord {
  cronExpression?: string;
  nextRunAt?: string;
}

interface TaskStatus {
  configured: boolean;
  enabled: boolean;
  canEnable: boolean;
  reason?: string;
  record?: ScheduleRecord | null;
}

interface WorkflowListResponse {
  data?: FlowiseWorkflow[];
}

function hasScheduleTrigger(flowData: string) {
  try {
    const parsed = JSON.parse(flowData) as {
      nodes?: Array<{
        data?: { name?: string; inputs?: { startInputType?: string } };
      }>;
    };
    return (parsed.nodes ?? []).some(
      (node) =>
        node.data?.name === "startAgentflow" &&
        node.data.inputs?.startInputType === "scheduleInput",
    );
  } catch {
    return false;
  }
}

async function flowiseRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${FLOWISE_API}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...init?.headers,
    },
  });
  if (!response.ok) throw new Error(`Flowise API ${response.status}`);
  return (await response.json()) as T;
}

export function FlowiseTasksPage() {
  const { t } = useTranslation();
  const [workflows, setWorkflows] = useState<FlowiseWorkflow[]>([]);
  const [statuses, setStatuses] = useState<Record<string, TaskStatus>>({});
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);

  const loadTasks = useCallback(async () => {
    setLoading(true);
    setError(false);
    try {
      const result = await flowiseRequest<WorkflowListResponse>(
        "/chatflows?type=AGENTFLOW&page=1&limit=1000",
      );
      const items = result.data ?? [];
      setWorkflows(items);
      const entries = await Promise.all(
        items.map(async (workflow): Promise<[string, TaskStatus]> => {
          if (!hasScheduleTrigger(workflow.flowData)) {
            return [
              workflow.id,
              { configured: false, enabled: false, canEnable: false },
            ];
          }
          try {
            const status = await flowiseRequest<{
              enabled?: boolean;
              canEnable?: boolean;
              reason?: string;
              record?: ScheduleRecord | null;
            }>(`/chatflows/${workflow.id}/schedule/status`);
            return [
              workflow.id,
              {
                configured: true,
                enabled: status.enabled === true,
                canEnable: status.canEnable === true,
                reason: status.reason,
                record: status.record,
              },
            ];
          } catch {
            return [
              workflow.id,
              { configured: true, enabled: false, canEnable: false },
            ];
          }
        }),
      );
      setStatuses(Object.fromEntries(entries));
    } catch {
      setError(true);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadTasks();
  }, [loadTasks]);

  const filtered = useMemo(() => {
    const query = search.trim().toLocaleLowerCase();
    return query
      ? workflows.filter((workflow) =>
          workflow.name.toLocaleLowerCase().includes(query),
        )
      : workflows;
  }, [search, workflows]);

  const toggleTask = async (workflowId: string, enabled: boolean) => {
    setError(false);
    try {
      await flowiseRequest(`/chatflows/${workflowId}/schedule/enabled`, {
        method: "PATCH",
        body: JSON.stringify({ enabled }),
      });
      await loadTasks();
    } catch {
      setError(true);
    }
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-hidden p-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold">{t("flowiseTasks.title")}</h2>
          <p className="text-xs text-muted-foreground">
            {t("flowiseTasks.description")}
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={() => void loadTasks()}>
          <LuRefreshCw className="size-3.5" />
          {t("common.buttons.refresh")}
        </Button>
      </div>

      <Input
        value={search}
        onChange={(event) => setSearch(event.target.value)}
        placeholder={t("flowiseTasks.search")}
        className="max-w-sm"
      />

      {error && (
        <div className="rounded-md border border-destructive/50 bg-destructive/10 p-3 text-sm text-destructive">
          {t("flowiseTasks.connectionError")}
        </div>
      )}

      <div className="min-h-0 flex-1 overflow-auto rounded-lg border border-border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t("flowiseTasks.workflow")}</TableHead>
              <TableHead>{t("flowiseTasks.schedule")}</TableHead>
              <TableHead>{t("flowiseTasks.nextRun")}</TableHead>
              <TableHead>{t("flowiseTasks.status")}</TableHead>
              <TableHead className="text-right">
                {t("flowiseTasks.enabled")}
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {!loading && filtered.length === 0 && (
              <TableRow>
                <TableCell colSpan={5} className="h-40 text-center text-muted-foreground">
                  {t("flowiseTasks.empty")}
                </TableCell>
              </TableRow>
            )}
            {filtered.map((workflow) => {
              const status = statuses[workflow.id];
              const nextRun = status?.record?.nextRunAt;
              return (
                <TableRow key={workflow.id}>
                  <TableCell className="font-medium">{workflow.name}</TableCell>
                  <TableCell>
                    {status?.record?.cronExpression ??
                      t("flowiseTasks.notConfigured")}
                  </TableCell>
                  <TableCell>
                    {nextRun
                      ? new Date(nextRun).toLocaleString()
                      : t("flowiseTasks.notAvailable")}
                  </TableCell>
                  <TableCell>
                    {!status?.configured ? (
                      <Badge variant="outline">{t("flowiseTasks.needsTrigger")}</Badge>
                    ) : status.enabled ? (
                      <Badge className="border-transparent bg-success text-success-foreground">
                        {t("flowiseTasks.running")}
                      </Badge>
                    ) : (
                      <Badge variant="secondary">{t("flowiseTasks.paused")}</Badge>
                    )}
                  </TableCell>
                  <TableCell className="text-right">
                    <AnimatedSwitch
                      checked={status?.enabled === true}
                      disabled={
                        !status?.configured ||
                        (!status.enabled && !status.canEnable)
                      }
                      title={status?.reason}
                      onCheckedChange={(checked) =>
                        void toggleTask(workflow.id, checked)
                      }
                    />
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
