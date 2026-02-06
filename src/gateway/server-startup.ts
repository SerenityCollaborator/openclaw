import type { CliDeps } from "../cli/deps.js";
import type { loadConfig } from "../config/config.js";
import type { loadOpenClawPlugins } from "../plugins/loader.js";
import { DEFAULT_MODEL, DEFAULT_PROVIDER } from "../agents/defaults.js";
import { loadModelCatalog } from "../agents/model-catalog.js";
import {
  getModelRefStatus,
  resolveConfiguredModelRef,
  resolveHooksGmailModel,
} from "../agents/model-selection.js";
import { startGmailWatcher } from "../hooks/gmail-watcher.js";
import {
  clearInternalHooks,
  createInternalHookEvent,
  triggerInternalHook,
} from "../hooks/internal-hooks.js";
import { loadInternalHooks } from "../hooks/loader.js";
import { onAgentEvent } from "../infra/agent-events.js";
import { isTruthyEnvValue } from "../infra/env.js";
import { type PluginServicesHandle, startPluginServices } from "../plugins/services.js";
import { startBrowserControlServerIfEnabled } from "./server-browser.js";
import {
  scheduleRestartSentinelWake,
  shouldWakeFromRestartSentinel,
} from "./server-restart-sentinel.js";
import { startGatewayMemoryBackend } from "./server-startup-memory.js";

export async function startGatewaySidecars(params: {
  cfg: ReturnType<typeof loadConfig>;
  pluginRegistry: ReturnType<typeof loadOpenClawPlugins>;
  defaultWorkspaceDir: string;
  deps: CliDeps;
  startChannels: () => Promise<void>;
  log: { warn: (msg: string) => void };
  logHooks: {
    info: (msg: string) => void;
    warn: (msg: string) => void;
    error: (msg: string) => void;
  };
  logChannels: { info: (msg: string) => void; error: (msg: string) => void };
  logBrowser: { error: (msg: string) => void };
}) {
  // Start OpenClaw browser control server (unless disabled via config).
  let browserControl: Awaited<ReturnType<typeof startBrowserControlServerIfEnabled>> = null;
  try {
    browserControl = await startBrowserControlServerIfEnabled();
  } catch (err) {
    params.logBrowser.error(`server failed to start: ${String(err)}`);
  }

  // Start Gmail watcher if configured (hooks.gmail.account).
  if (!isTruthyEnvValue(process.env.OPENCLAW_SKIP_GMAIL_WATCHER)) {
    try {
      const gmailResult = await startGmailWatcher(params.cfg);
      if (gmailResult.started) {
        params.logHooks.info("gmail watcher started");
      } else if (
        gmailResult.reason &&
        gmailResult.reason !== "hooks not enabled" &&
        gmailResult.reason !== "no gmail account configured"
      ) {
        params.logHooks.warn(`gmail watcher not started: ${gmailResult.reason}`);
      }
    } catch (err) {
      params.logHooks.error(`gmail watcher failed to start: ${String(err)}`);
    }
  }

  // Validate hooks.gmail.model if configured.
  if (params.cfg.hooks?.gmail?.model) {
    const hooksModelRef = resolveHooksGmailModel({
      cfg: params.cfg,
      defaultProvider: DEFAULT_PROVIDER,
    });
    if (hooksModelRef) {
      const { provider: defaultProvider, model: defaultModel } = resolveConfiguredModelRef({
        cfg: params.cfg,
        defaultProvider: DEFAULT_PROVIDER,
        defaultModel: DEFAULT_MODEL,
      });
      const catalog = await loadModelCatalog({ config: params.cfg });
      const status = getModelRefStatus({
        cfg: params.cfg,
        catalog,
        ref: hooksModelRef,
        defaultProvider,
        defaultModel,
      });
      if (!status.allowed) {
        params.logHooks.warn(
          `hooks.gmail.model "${status.key}" not in agents.defaults.models allowlist (will use primary instead)`,
        );
      }
      if (!status.inCatalog) {
        params.logHooks.warn(
          `hooks.gmail.model "${status.key}" not in the model catalog (may fail at runtime)`,
        );
      }
    }
  }

  // Load internal hook handlers from configuration and directory discovery.
  try {
    // Clear any previously registered hooks to ensure fresh loading
    clearInternalHooks();
    const loadedCount = await loadInternalHooks(params.cfg, params.defaultWorkspaceDir);
    if (loadedCount > 0) {
      params.logHooks.info(
        `loaded ${loadedCount} internal hook handler${loadedCount > 1 ? "s" : ""}`,
      );
    }
  } catch (err) {
    params.logHooks.error(`failed to load hooks: ${String(err)}`);
  }

  // Bridge agent events (messages, tool calls, tool results) to the internal hook system
  // so hooks registered for "activity" events receive them.
  if (params.cfg.hooks?.internal?.enabled) {
    // Track the latest assistant text per run so we can emit a single message
    // event when the run ends or a tool starts (i.e. assistant turn is complete).
    const assistantBuffer = new Map<string, { text: string; sessionKey: string; ts: number }>();

    const flushAssistant = (runId: string) => {
      const buf = assistantBuffer.get(runId);
      if (!buf || !buf.text.trim()) {
        return;
      }
      assistantBuffer.delete(runId);
      void triggerInternalHook(
        createInternalHookEvent("activity", "message", buf.sessionKey, {
          sessionKey: buf.sessionKey,
          runId,
          role: "assistant",
          message: {
            role: "assistant",
            content: [{ type: "text", text: buf.text }],
            timestamp: buf.ts,
          },
        }),
      );
    };

    onAgentEvent((evt) => {
      const data = evt.data ?? {};
      const sessionKey = evt.sessionKey ?? "";

      if (evt.stream === "assistant" && data.text) {
        // Buffer the latest accumulated text; don't emit yet (deltas are frequent).
        assistantBuffer.set(evt.runId, {
          text: data.text as string,
          sessionKey,
          ts: evt.ts,
        });
        return;
      }

      if (evt.stream === "tool") {
        const phase = data.phase as string | undefined;

        if (phase === "start") {
          // A tool call means the assistant turn before it is complete.
          flushAssistant(evt.runId);

          void triggerInternalHook(
            createInternalHookEvent("activity", "tool_call", sessionKey, {
              sessionKey,
              runId: evt.runId,
              role: "assistant",
              toolName: data.name,
              toolCallId: data.toolCallId,
              message: {
                role: "assistant",
                content: [
                  {
                    type: "toolCall",
                    id: data.toolCallId,
                    name: data.name,
                    arguments: data.args,
                  },
                ],
                timestamp: evt.ts,
              },
            }),
          );
        } else if (phase === "result") {
          void triggerInternalHook(
            createInternalHookEvent("activity", "tool_result", sessionKey, {
              sessionKey,
              runId: evt.runId,
              role: "toolResult",
              toolName: data.name,
              toolCallId: data.toolCallId,
              message: {
                role: "toolResult",
                toolCallId: data.toolCallId,
                toolName: data.name,
                isError: data.isError,
                content: data.result
                  ? [
                      {
                        type: "text",
                        text:
                          typeof data.result === "string"
                            ? data.result
                            : JSON.stringify(data.result),
                      },
                    ]
                  : [],
                details: { status: data.isError ? "error" : "completed" },
                timestamp: evt.ts,
              },
            }),
          );
        }
        return;
      }

      if (evt.stream === "lifecycle") {
        const phase = data.phase as string | undefined;
        if (phase === "end" || phase === "error") {
          // Agent run is done — flush any buffered assistant text.
          flushAssistant(evt.runId);
        }
      }
    });
  }

  // Launch configured channels so gateway replies via the surface the message came from.
  // Tests can opt out via OPENCLAW_SKIP_CHANNELS (or legacy OPENCLAW_SKIP_PROVIDERS).
  const skipChannels =
    isTruthyEnvValue(process.env.OPENCLAW_SKIP_CHANNELS) ||
    isTruthyEnvValue(process.env.OPENCLAW_SKIP_PROVIDERS);
  if (!skipChannels) {
    try {
      await params.startChannels();
    } catch (err) {
      params.logChannels.error(`channel startup failed: ${String(err)}`);
    }
  } else {
    params.logChannels.info(
      "skipping channel start (OPENCLAW_SKIP_CHANNELS=1 or OPENCLAW_SKIP_PROVIDERS=1)",
    );
  }

  if (params.cfg.hooks?.internal?.enabled) {
    setTimeout(() => {
      const hookEvent = createInternalHookEvent("gateway", "startup", "gateway:startup", {
        cfg: params.cfg,
        deps: params.deps,
        workspaceDir: params.defaultWorkspaceDir,
      });
      void triggerInternalHook(hookEvent);
    }, 250);
  }

  let pluginServices: PluginServicesHandle | null = null;
  try {
    pluginServices = await startPluginServices({
      registry: params.pluginRegistry,
      config: params.cfg,
      workspaceDir: params.defaultWorkspaceDir,
    });
  } catch (err) {
    params.log.warn(`plugin services failed to start: ${String(err)}`);
  }

  void startGatewayMemoryBackend({ cfg: params.cfg, log: params.log }).catch((err) => {
    params.log.warn(`qmd memory startup initialization failed: ${String(err)}`);
  });

  if (shouldWakeFromRestartSentinel()) {
    setTimeout(() => {
      void scheduleRestartSentinelWake({ deps: params.deps });
    }, 750);
  }

  return { browserControl, pluginServices };
}
