const queues = new Map<string, Promise<unknown>>();

/**
 * Serialize all outbound Telegram Bot API calls per chat_id.
 *
 * Telegram rate limits are per-chat. We also rely on this to make message
 * ordering deterministic when multiple code-paths send to the same chat.
 */
export function queueTelegramSend<T>(chatId: string, fn: () => Promise<T>): Promise<T> {
  const key = String(chatId);
  const prev = queues.get(key) ?? Promise.resolve();

  // Always proceed even if the previous send failed.
  const next = prev.then(fn, fn);

  // Store a tail promise that never rejects so future sends don't get blocked.
  const tail = next.then(
    () => undefined,
    () => undefined,
  );
  queues.set(key, tail);

  // Best-effort cleanup: only delete if we're still the last tail for this chat.
  void tail.finally(() => {
    if (queues.get(key) === tail) {
      queues.delete(key);
    }
  });

  // Important: do not swallow errors for the caller.
  return next;
}
