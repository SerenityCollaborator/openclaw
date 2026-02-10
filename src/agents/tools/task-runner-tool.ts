import { Type } from "@sinclair/typebox";
import { getTaskRunnerService } from "../../task-runner/service.js";
import { isRecord } from "../../utils.js";
import { stringEnum } from "../schema/typebox.js";
import {
  type AnyAgentTool,
  jsonResult,
  readNumberParam,
  readStringArrayParam,
  readStringParam,
} from "./common.js";

const TASK_RUNNER_ACTIONS = [
  "task_start",
  "task_stop",
  "task_signal",
  "task_status",
  "task_logs",
  "task_list",
  "task_write",
  "task_wait",
  "task_prune",
] as const;

// Flattened schema: runtime validates per-action requirements.
const TaskRunnerToolSchema = Type.Object({
  action: stringEnum(TASK_RUNNER_ACTIONS),

  // shared
  id: Type.Optional(Type.String()),
  tags: Type.Optional(Type.Array(Type.String())),

  // start
  command: Type.Optional(Type.String()),
  args: Type.Optional(Type.Array(Type.String())),
  cwd: Type.Optional(Type.String()),
  env: Type.Optional(Type.Object({}, { additionalProperties: true })),

  // signal
  signal: Type.Optional(Type.String()),

  // stop/wait
  timeoutMs: Type.Optional(Type.Number()),

  // logs
  sinceBytes: Type.Optional(Type.Number()),
  tailBytes: Type.Optional(Type.Number()),
  maxBytes: Type.Optional(Type.Number()),

  // write
  text: Type.Optional(Type.String()),

  // prune
  olderThanMs: Type.Optional(Type.Number()),
});

function readEnvObject(params: Record<string, unknown>) {
  const raw = params.env;
  if (!raw) {
    return undefined;
  }
  if (!isRecord(raw)) {
    throw new Error("env must be an object");
  }
  const env: Record<string, string> = {};
  for (const [k, v] of Object.entries(raw)) {
    if (typeof v === "string") {
      env[k] = v;
    } else if (typeof v === "number" && Number.isFinite(v)) {
      env[k] = String(v);
    } else if (typeof v === "boolean") {
      env[k] = v ? "true" : "false";
    }
  }
  return env;
}

export function createTaskRunnerTool(): AnyAgentTool {
  const svc = getTaskRunnerService();

  return {
    label: "Task Runner",
    name: "task_runner",
    description: `Manage long-running background tasks that persist across gateway restarts.

ACTIONS:
- task_start: Start a new task (detached) and persist it to disk.
- task_stop: Stop a task (SIGTERM then SIGKILL).
- task_signal: Send a signal to a task's process group.
- task_status: Get current status + uptime.
- task_logs: Read combined stdout/stderr logs (tail or incremental sinceBytes).
- task_list: List tasks (optionally filter by tags).
- task_write: Write to stdin (only available while the gateway instance that started it is alive).
- task_wait: Wait until exit or timeout.
- task_prune: Remove old completed tasks + delete their files.

NOTES:
- Logs are stored at ~/.task-runner/logs/{id}.log
- Tasks survive gateway restarts (detached). After a restart, task_write may not work.
- Prefer this over exec for dev servers, watchers, build processes, and anything that should outlive the current session.
`,
    parameters: TaskRunnerToolSchema,
    execute: async (_toolCallId, args) => {
      const params = args as Record<string, unknown>;
      const action = readStringParam(params, "action", { required: true });

      switch (action) {
        case "task_start": {
          const command = readStringParam(params, "command", { required: true });
          const id = readStringParam(params, "id");
          const cwd = readStringParam(params, "cwd");
          const argsList = readStringArrayParam(params, "args", { allowEmpty: true }) ?? [];
          const tags = readStringArrayParam(params, "tags", { allowEmpty: true });
          const env = readEnvObject(params);
          return jsonResult(await svc.start({ id, command, args: argsList, cwd, env, tags }));
        }
        case "task_stop": {
          const id = readStringParam(params, "id", { required: true });
          const timeoutMs = readNumberParam(params, "timeoutMs");
          return jsonResult(await svc.stop(id, { timeoutMs }));
        }
        case "task_signal": {
          const id = readStringParam(params, "id", { required: true });
          const signal = readStringParam(params, "signal", { required: true, trim: true });
          return jsonResult(await svc.signal(id, signal));
        }
        case "task_status": {
          const id = readStringParam(params, "id", { required: true });
          return jsonResult(await svc.status(id));
        }
        case "task_logs": {
          const id = readStringParam(params, "id", { required: true });
          const sinceBytes = readNumberParam(params, "sinceBytes");
          const tailBytes = readNumberParam(params, "tailBytes");
          const maxBytes = readNumberParam(params, "maxBytes");
          return jsonResult(await svc.logs({ id, sinceBytes, tailBytes, maxBytes }));
        }
        case "task_list": {
          const tags = readStringArrayParam(params, "tags", { allowEmpty: true });
          await svc.init();
          return jsonResult({ tasks: svc.list({ tags }) });
        }
        case "task_write": {
          const id = readStringParam(params, "id", { required: true });
          const text = readStringParam(params, "text", {
            required: true,
            trim: false,
            allowEmpty: true,
          });
          return jsonResult(await svc.write(id, text));
        }
        case "task_wait": {
          const id = readStringParam(params, "id", { required: true });
          const timeoutMs = readNumberParam(params, "timeoutMs");
          return jsonResult(await svc.wait(id, { timeoutMs }));
        }
        case "task_prune": {
          const olderThanMs = readNumberParam(params, "olderThanMs");
          return jsonResult(await svc.prune({ olderThanMs }));
        }
        default:
          throw new Error(`Unknown action: ${action}`);
      }
    },
  };
}
