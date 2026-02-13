// oxlint-disable typescript/no-explicit-any

import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type {
  TaskLogsRequest,
  TaskLogsResponse,
  TaskRecord,
  TaskRunnerConfig,
  TaskRunnerStateFile,
  TaskSignal,
  TaskStartRequest,
  TaskStatus,
} from "./types.js";
import { recoverTaskRunnerState } from "./recovery.js";

function now() {
  return Date.now();
}

function isRunningPid(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function safeBasename(input: string) {
  return input.replace(/[^a-zA-Z0-9_-]+/g, "-").slice(0, 60);
}

function isSubPath(candidate: string, parent: string) {
  const rel = path.relative(parent, candidate);
  return rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel));
}

async function ensureDir(dirPath: string) {
  await fsp.mkdir(dirPath, { recursive: true });
}

async function writeJsonAtomic(filePath: string, payload: unknown) {
  const dir = path.dirname(filePath);
  await ensureDir(dir);
  const tmpPath = `${filePath}.tmp`;
  await fsp.writeFile(tmpPath, JSON.stringify(payload, null, 2), "utf8");
  await fsp.rename(tmpPath, filePath);
}

async function truncateLogIfNeeded(logPath: string, maxBytes: number) {
  if (maxBytes <= 0) {
    return;
  }
  let stat: fs.Stats | null = null;
  try {
    stat = await fsp.stat(logPath);
  } catch {
    return;
  }
  if (!stat.isFile() || stat.size <= maxBytes) {
    return;
  }
  const start = Math.max(0, stat.size - maxBytes);
  const fh = await fsp.open(logPath, "r");
  try {
    const buf = Buffer.alloc(Math.min(maxBytes, stat.size));
    const { bytesRead } = await fh.read(buf, 0, buf.length, start);
    const tmp = `${logPath}.trim.tmp`;
    await fsp.writeFile(tmp, buf.subarray(0, bytesRead));
    await fsp.rename(tmp, logPath);
  } finally {
    await fh.close();
  }
}

function sanitizeEnv(env: Record<string, string> | undefined, blocked: string[]) {
  if (!env) {
    return undefined;
  }
  const blockedSet = new Set(blocked.map((k) => k.toUpperCase()));
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(env)) {
    if (!k) {
      continue;
    }
    if (blockedSet.has(k.toUpperCase())) {
      continue;
    }
    if (typeof v !== "string") {
      continue;
    }
    out[k] = v;
  }
  return out;
}

function normalizeTags(tags: string[] | undefined) {
  if (!Array.isArray(tags)) {
    return undefined;
  }
  const out = tags
    .filter((t) => typeof t === "string")
    .map((t) => t.trim())
    .filter(Boolean)
    .slice(0, 20);
  return out.length ? Array.from(new Set(out)) : undefined;
}

export class TaskRunnerService {
  readonly baseDir: string;
  readonly logsDir: string;
  readonly pidsDir: string;
  readonly statePath: string;

  readonly config: TaskRunnerConfig;

  private state: TaskRunnerStateFile | null = null;
  private children = new Map<string, ChildProcess>();

  constructor(config?: Partial<TaskRunnerConfig>) {
    const home = os.homedir();
    this.baseDir = path.join(home, ".task-runner");
    this.logsDir = path.join(this.baseDir, "logs");
    this.pidsDir = path.join(this.baseDir, "pids");
    this.statePath = path.join(this.baseDir, "state.json");

    const envAllowed = (process.env.OPENCLAW_TASK_ALLOWED_CWDS ?? "")
      .split(",")
      .map((v) => v.trim())
      .filter(Boolean);

    this.config = {
      maxConcurrentTasks: config?.maxConcurrentTasks ?? 20,
      maxLogSizeBytes: config?.maxLogSizeBytes ?? 10 * 1024 * 1024,
      allowedCwds: config?.allowedCwds ?? (envAllowed.length ? envAllowed : [home]),
      blockedEnvVars: config?.blockedEnvVars ?? ["AWS_SECRET_KEY", "GITHUB_TOKEN"],
    };
  }

  async init() {
    if (this.state) {
      return;
    }
    await ensureDir(this.baseDir);
    await ensureDir(this.logsDir);
    await ensureDir(this.pidsDir);

    const recovered = await recoverTaskRunnerState({ statePath: this.statePath });
    this.state = recovered.state;
    if (recovered.changed) {
      await this.persist();
    }
  }

  private requireState() {
    if (!this.state) {
      throw new Error("Task runner not initialized");
    }
    return this.state;
  }

  private async persist() {
    const state = this.requireState();
    state.updatedAt = now();
    await writeJsonAtomic(this.statePath, state);
  }

  private resolveLogPath(id: string) {
    return path.join(this.logsDir, `${safeBasename(id)}.log`);
  }

  private resolvePidPath(id: string) {
    return path.join(this.pidsDir, `${safeBasename(id)}.pid`);
  }

  private assertCwdAllowed(cwd?: string) {
    if (!cwd) {
      return;
    }
    const resolved = path.resolve(cwd);
    const allowed = this.config.allowedCwds.some((root) => isSubPath(resolved, path.resolve(root)));
    if (!allowed) {
      throw new Error(`cwd not allowed: ${cwd}`);
    }
  }

  private countRunning() {
    const state = this.requireState();
    let count = 0;
    for (const task of Object.values(state.tasks)) {
      if (task.status === "running" || task.status === "pending") {
        if (typeof task.pid === "number" && isRunningPid(task.pid)) {
          count += 1;
        }
      }
    }
    return count;
  }

  list(params?: { tags?: string[] }) {
    const state = this.requireState();
    const filterTags = normalizeTags(params?.tags);
    const tasks = Object.values(state.tasks).toSorted((a, b) => b.createdAt - a.createdAt);
    if (!filterTags?.length) {
      return tasks;
    }
    const tagSet = new Set(filterTags);
    return tasks.filter((t) => (t.tags ?? []).some((tag) => tagSet.has(tag)));
  }

  get(id: string) {
    const state = this.requireState();
    return state.tasks[id];
  }

  private isTerminalStatus(status: TaskStatus) {
    return (
      status === "stopped" ||
      status === "failed" ||
      status === "killed" ||
      status === "timeout" ||
      status === "lost"
    );
  }

  private async cleanupTaskFiles(task: TaskRecord) {
    try {
      await fsp.unlink(task.logPath);
    } catch {
      // ignore
    }
    if (task.pidPath) {
      try {
        await fsp.unlink(task.pidPath);
      } catch {
        // ignore
      }
    }
  }

  private async removeTaskRecord(id: string) {
    const state = this.requireState();
    const task = state.tasks[id];
    if (!task) {
      return;
    }
    delete state.tasks[id];
    this.children.delete(id);
    await this.cleanupTaskFiles(task);
  }

  async start(req: TaskStartRequest): Promise<{ id: string; pid: number; status: TaskStatus }> {
    await this.init();
    this.assertCwdAllowed(req.cwd);

    const runningCount = this.countRunning();
    if (runningCount >= this.config.maxConcurrentTasks) {
      throw new Error(
        `Too many concurrent tasks (${runningCount}/${this.config.maxConcurrentTasks})`,
      );
    }

    const id = (req.id ?? "").trim() || randomUUID();
    const state = this.requireState();
    const existing = state.tasks[id];
    if (existing) {
      if (this.isTerminalStatus(existing.status)) {
        // Auto-replace terminal tasks with the same id.
        await this.removeTaskRecord(id);
      } else {
        throw new Error(`Task already exists: ${id}`);
      }
    }

    const createdAt = now();
    const logPath = this.resolveLogPath(id);
    const pidPath = this.resolvePidPath(id);

    await truncateLogIfNeeded(logPath, this.config.maxLogSizeBytes);

    const record: TaskRecord = {
      id,
      status: "pending",
      command: req.command,
      args: req.args ?? [],
      cwd: req.cwd,
      env: sanitizeEnv(req.env, this.config.blockedEnvVars),
      tags: normalizeTags(req.tags),
      createdAt,
      updatedAt: createdAt,
      logPath,
      pidPath,
      stdinAttached: false,
    };

    state.tasks[id] = record;
    await this.persist();

    const outFd = fs.openSync(logPath, "a");
    const child = spawn(req.command, req.args ?? [], {
      cwd: req.cwd,
      env: { ...process.env, ...record.env },
      detached: true,
      stdio: ["pipe", outFd, outFd],
    });

    child.unref();

    if (!child.pid) {
      record.status = "failed";
      record.updatedAt = now();
      await this.persist();
      throw new Error("Failed to start task (no pid)");
    }

    record.pid = child.pid;
    record.startedAt = now();
    record.status = "running";
    record.stdinAttached = true;
    record.updatedAt = now();

    this.children.set(id, child);

    await fsp.writeFile(pidPath, String(child.pid), "utf8");
    await this.persist();

    // Observe exit while this gateway instance is alive.
    child.once("exit", (code, signal) => {
      void this.onExit(id, code, signal);
    });

    child.stdin?.on("error", () => {
      // ignore
    });

    return { id, pid: child.pid, status: record.status };
  }

  private async onExit(id: string, exitCode: number | null, exitSignal: TaskSignal | null) {
    await this.init();
    const state = this.requireState();
    const task = state.tasks[id];
    if (!task) {
      return;
    }

    // If already terminal (e.g. stop set killed), keep its status.
    const prior = task.status;

    task.exitCode = exitCode;
    task.exitSignal = exitSignal;
    task.endedAt = task.endedAt ?? now();
    task.stdinAttached = false;

    if (prior === "running" || prior === "pending") {
      if (exitSignal) {
        task.status = "killed";
      } else if (exitCode === 0) {
        task.status = "stopped";
      } else {
        task.status = "failed";
      }
    }

    task.updatedAt = now();
    this.children.delete(id);

    await this.persist();
  }

  async status(id: string) {
    await this.init();
    const task = this.get(id);
    if (!task) {
      throw new Error(`Task not found: ${id}`);
    }

    // Refresh running/lost state based on PID.
    if ((task.status === "running" || task.status === "pending") && typeof task.pid === "number") {
      if (!isRunningPid(task.pid)) {
        task.status = "lost";
        task.endedAt = task.endedAt ?? now();
        task.updatedAt = now();
        task.stdinAttached = false;
        await this.persist();
      }
    }

    const started = task.startedAt ?? task.createdAt;
    const ended = task.endedAt;
    const uptimeMs = ended ? ended - started : now() - started;

    return {
      ...task,
      uptimeMs,
    };
  }

  async signal(id: string, signal: TaskSignal) {
    await this.init();
    const task = this.get(id);
    if (!task || typeof task.pid !== "number") {
      throw new Error(`Task not found: ${id}`);
    }
    process.kill(-task.pid, signal as any);
    task.updatedAt = now();
    await this.persist();
    return { id: task.id, pid: task.pid, status: task.status, signal };
  }

  async stop(id: string, params?: { timeoutMs?: number }) {
    await this.init();
    const task = this.get(id);
    if (!task || typeof task.pid !== "number") {
      throw new Error(`Task not found: ${id}`);
    }

    const timeoutMs = typeof params?.timeoutMs === "number" ? params.timeoutMs : 5000;

    // best-effort terminate process group
    try {
      process.kill(-task.pid, "SIGTERM");
    } catch {
      // already gone
    }

    const deadline = now() + Math.max(0, timeoutMs);
    while (now() < deadline) {
      if (!isRunningPid(task.pid)) {
        break;
      }
      await new Promise((r) => setTimeout(r, 200));
    }

    if (isRunningPid(task.pid)) {
      try {
        process.kill(-task.pid, "SIGKILL");
      } catch {
        // ignore
      }
    }

    task.status = "killed";
    task.endedAt = task.endedAt ?? now();
    task.updatedAt = now();
    task.stdinAttached = false;
    await this.persist();

    return { id: task.id, pid: task.pid, status: task.status };
  }

  async restart(
    id: string,
    overrides?: Partial<Pick<TaskRecord, "command" | "args" | "cwd" | "env" | "tags">>,
  ) {
    await this.init();

    const state = this.requireState();
    const task = state.tasks[id];
    if (!task) {
      throw new Error(`Task not found: ${id}`);
    }

    // Stop if still running/pending.
    if ((task.status === "running" || task.status === "pending") && typeof task.pid === "number") {
      await this.stop(id);
    }

    const next = {
      id,
      command: overrides?.command ?? task.command,
      args: overrides?.args ?? task.args,
      cwd: overrides?.cwd ?? task.cwd,
      env: overrides?.env ?? task.env,
      tags: overrides?.tags ?? task.tags,
    } satisfies TaskStartRequest;

    // Remove old record and files so we can reuse the same id.
    await this.removeTaskRecord(id);
    await this.persist();

    return await this.start(next);
  }

  async write(id: string, data: string) {
    await this.init();
    const task = this.get(id);
    if (!task) {
      throw new Error(`Task not found: ${id}`);
    }
    const child = this.children.get(id);
    const stdin = child?.stdin;
    if (!child || !stdin || stdin.destroyed) {
      throw new Error("stdin not available (task likely started in a previous gateway session)");
    }
    await new Promise<void>((resolve, reject) => {
      stdin.write(data, (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
    return { id, written: data.length };
  }

  async wait(id: string, params?: { timeoutMs?: number }) {
    await this.init();
    const task = this.get(id);
    if (!task) {
      throw new Error(`Task not found: ${id}`);
    }

    const timeoutMs = typeof params?.timeoutMs === "number" ? params.timeoutMs : 30_000;

    if (task.status !== "running" && task.status !== "pending") {
      return await this.status(id);
    }

    const child = this.children.get(id);
    if (child) {
      await Promise.race([
        new Promise<void>((resolve) => {
          child.once("exit", () => resolve());
        }),
        new Promise<void>((resolve) => setTimeout(resolve, Math.max(0, timeoutMs))),
      ]);
      return await this.status(id);
    }

    // No child handle (previous session). Poll PID.
    const pid = task.pid;
    if (typeof pid !== "number") {
      return await this.status(id);
    }

    const deadline = now() + Math.max(0, timeoutMs);
    while (now() < deadline) {
      if (!isRunningPid(pid)) {
        break;
      }
      await new Promise((r) => setTimeout(r, 500));
    }
    if (isRunningPid(pid)) {
      // still running; no state change
      return await this.status(id);
    }

    // If process exited while gateway down, mark as lost (no exit code available).
    const refreshed = await this.status(id);
    if (refreshed.status === "running") {
      task.status = "lost";
      task.endedAt = task.endedAt ?? now();
      task.updatedAt = now();
      await this.persist();
      return await this.status(id);
    }
    return refreshed;
  }

  async logs(req: TaskLogsRequest): Promise<TaskLogsResponse> {
    await this.init();
    const task = this.get(req.id);
    if (!task) {
      throw new Error(`Task not found: ${req.id}`);
    }

    await truncateLogIfNeeded(task.logPath, this.config.maxLogSizeBytes);

    const maxBytes = Math.min(
      typeof req.maxBytes === "number" ? req.maxBytes : 64 * 1024,
      512 * 1024,
    );
    const stat = await fsp.stat(task.logPath);
    const fileSize = stat.size;

    let start = 0;
    if (typeof req.sinceBytes === "number" && Number.isFinite(req.sinceBytes)) {
      start = Math.max(0, Math.min(fileSize, Math.trunc(req.sinceBytes)));
    } else if (typeof req.tailBytes === "number" && Number.isFinite(req.tailBytes)) {
      const tailBytes = Math.max(0, Math.trunc(req.tailBytes));
      start = Math.max(0, fileSize - Math.min(tailBytes, maxBytes));
    } else {
      start = Math.max(0, fileSize - Math.min(8 * 1024, maxBytes));
    }

    const endExclusive = Math.min(fileSize, start + maxBytes);

    const fh = await fsp.open(task.logPath, "r");
    try {
      const buf = Buffer.alloc(endExclusive - start);
      const { bytesRead } = await fh.read(buf, 0, buf.length, start);
      const text = buf.subarray(0, bytesRead).toString("utf8");
      const truncated = endExclusive < fileSize;
      return {
        id: task.id,
        logPath: task.logPath,
        text,
        nextSinceBytes: endExclusive,
        truncated,
      };
    } finally {
      await fh.close();
    }
  }

  async prune(params?: { olderThanMs?: number }) {
    await this.init();
    const state = this.requireState();
    const olderThanMs =
      typeof params?.olderThanMs === "number" ? params.olderThanMs : 24 * 60 * 60 * 1000;

    // Edge case: when olderThanMs is 0, callers typically mean "prune everything",
    // but `cutoff = now()` can race with tasks ending in the same millisecond.
    const cutoff = olderThanMs <= 0 ? Number.POSITIVE_INFINITY : now() - Math.max(0, olderThanMs);

    let removed = 0;
    for (const [id, task] of Object.entries(state.tasks)) {
      if (task.status === "running" || task.status === "pending") {
        continue;
      }
      const ended = task.endedAt ?? task.updatedAt;
      if (ended > cutoff) {
        continue;
      }
      delete state.tasks[id];
      removed += 1;
      try {
        await fsp.unlink(task.logPath);
      } catch {
        // ignore
      }
      if (task.pidPath) {
        try {
          await fsp.unlink(task.pidPath);
        } catch {
          // ignore
        }
      }
    }

    await this.persist();
    return { removed };
  }
}

let singleton: TaskRunnerService | null = null;

export function getTaskRunnerService() {
  singleton ??= new TaskRunnerService();
  return singleton;
}
