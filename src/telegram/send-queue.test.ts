import { describe, expect, it } from "vitest";
import { queueTelegramSend } from "./send-queue.js";

describe("queueTelegramSend", () => {
  it("serializes sends per chatId", async () => {
    const seen: number[] = [];
    await Promise.all([
      queueTelegramSend("1", async () => {
        await new Promise((r) => setTimeout(r, 20));
        seen.push(1);
      }),
      queueTelegramSend("1", async () => {
        seen.push(2);
      }),
    ]);
    expect(seen).toEqual([1, 2]);
  });

  it("does not block future sends when a previous send fails", async () => {
    const seen: number[] = [];
    await expect(
      queueTelegramSend("2", async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    await queueTelegramSend("2", async () => {
      seen.push(1);
    });

    expect(seen).toEqual([1]);
  });

  it("does not serialize across different chatIds", async () => {
    const seen: string[] = [];
    await Promise.all([
      queueTelegramSend("a", async () => {
        await new Promise((r) => setTimeout(r, 10));
        seen.push("a");
      }),
      queueTelegramSend("b", async () => {
        seen.push("b");
      }),
    ]);

    expect(seen).toContain("a");
    expect(seen).toContain("b");
  });
});
