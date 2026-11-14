import crypto from "node:crypto";
import { describe, expect, it } from "bun:test";
import { buildSystemPrompt, sanitizeAndValidateUrls } from "./brandbook.js";
import { shouldApplyTakeoverUpdate } from "./db.js";
import { handleWebhookRequest, isTimestampFresh, processInboundWhatsAppMessage } from "./index.js";

describe("whatsappbot security, takeover, and grounding", () => {
  it("enforces monotonic takeover state and sends ZERO internal messages on #stop/#bot", async () => {
    const sentMessages: Array<{ phone: string; text: string }> = [];
    let state = { active: false, updatedAtMs: 0 };
    const nowSec = Math.floor(Date.now() / 1000);

    const setTakeover = async (_phone: string, active: boolean, _dur: number | null, tsMs: number) => {
      if (shouldApplyTakeoverUpdate(state.updatedAtMs, tsMs)) {
        state = { active, updatedAtMs: tsMs };
      }
    };

    const stopRes = await processInboundWhatsAppMessage(
      {
        Info: { ID: "m1", Chat: "15551234567@s.whatsapp.net", IsFromMe: true, Timestamp: nowSec },
        Message: { conversation: "#stop" },
      },
      {
        setTakeover,
        sendReply: async (phone, text) => {
          sentMessages.push({ phone, text });
          return true;
        },
        appendSessionFn: async () => {},
      }
    );
    expect(stopRes.status).toBe("human_takeover_stopped_permanent");
    expect(state.active).toBe(true);
    expect(sentMessages.length).toBe(0);

    // Out-of-order older #bot event must NOT override newer #stop
    await setTakeover("15551234567", false, null, (nowSec - 60) * 1000);
    expect(state.active).toBe(true);

    // Newer #bot clears takeover with ZERO messages sent to customer
    const botRes = await processInboundWhatsAppMessage(
      {
        Info: { ID: "m2", Chat: "15551234567@s.whatsapp.net", IsFromMe: true, Timestamp: nowSec + 10 },
        Message: { conversation: "#bot" },
      },
      {
        setTakeover,
        sendReply: async (phone, text) => {
          sentMessages.push({ phone, text });
          return true;
        },
        appendSessionFn: async () => {},
      }
    );
    expect(botRes.status).toBe("human_takeover_cleared");
    expect(state.active).toBe(false);
    expect(sentMessages.length).toBe(0);
  });

  it("rejects unauthenticated and stale (>120s) webhook requests, accepts valid HMAC", async () => {
    const secret = "test-webhook-secret";
    const freshTs = Math.floor(Date.now() / 1000);
    const staleTs = freshTs - 300;

    const noAuthReq = new Request("http://localhost:3005/webhook", {
      method: "POST",
      body: JSON.stringify({ text: "hola", timestamp: freshTs }),
    });
    const noAuthRes = await handleWebhookRequest(noAuthReq, { secret, dispatchFn: async () => ({}) });
    expect(noAuthRes.status).toBe(401);

    const staleBody = JSON.stringify({ text: "hola", timestamp: staleTs });
    const staleReq = new Request("http://localhost:3005/webhook", {
      method: "POST",
      headers: { "x-webhook-secret": secret },
      body: staleBody,
    });
    const staleRes = await handleWebhookRequest(staleReq, { secret, dispatchFn: async () => ({}) });
    expect(staleRes.status).toBe(403);
    expect(isTimestampFresh({ timestamp: staleTs })).toBe(false);

    const freshBody = JSON.stringify({ text: "hola", timestamp: freshTs });
    const sig = "sha256=" + crypto.createHmac("sha256", secret).update(freshBody).digest("hex");
    const validReq = new Request("http://localhost:3005/webhook", {
      method: "POST",
      headers: { "x-hub-signature-256": sig },
      body: freshBody,
    });
    const validRes = await handleWebhookRequest(validReq, { secret, dispatchFn: async () => ({ status: "ok" }) });
    expect(validRes.status).toBe(200);
  });

  it("isolates verified sender PII and blocks hallucinated or spoofed URLs", () => {
    const prompt = buildSystemPrompt(
      { phone: "15550001111", alias: "Alex", isRegistered: true, recentOrders: [] },
      { itemsSummary: "- Plan Pro | precio=99 | ruta=/planes", tablesInspected: ["plans"] },
      { baseUrl: "https://acme.example.com", allowedPaths: ["/", "/planes", "/contacto"], summaryText: "Acme" }
    );

    expect(prompt).toContain("<verified_sender_profile>");
    expect(prompt).toContain("15550001111");
    expect(prompt).toContain("<security_boundary>");
    expect(prompt).toContain("AISLAMIENTO ESTRICTO DE PII");

    const llmReply =
      "Mira https://acme.example.com/planes y https://acme.example.com/ruta-inventada o https://evil-acme.example.com/phish";
    const sanitized = sanitizeAndValidateUrls(llmReply, "https://acme.example.com", ["/", "/planes", "/contacto"]);

    expect(sanitized).toContain("https://acme.example.com/planes");
    expect(sanitized).not.toContain("/ruta-inventada");
    expect(sanitized).not.toContain("evil-acme.example.com");
  });
});
