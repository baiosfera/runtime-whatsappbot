import crypto from "node:crypto";
import { connect, JSONCodec } from "nats";
import {
  checkDbConnection,
  getPublicCatalogFromDb,
  getTakeoverStateFromDb,
  getVerifiedCustomerProfile,
  lookupPhoneByLidFromDb,
  recordIncomingLead,
  setTakeoverStateInDb,
  syncLidMappingsToValkey,
} from "./db.js";
import {
  acquireLock,
  appendSessionMessage,
  checkRateLimit,
  checkValkeyConnection,
  clearHumanTakeover,
  getSessionHistory,
  isHumanTakeover,
  linkPhoneAndLid,
  redis,
  resolveCanonicalPhone,
  setHumanTakeover,
} from "./valkey.js";
import {
  checkBifrostConnection,
  detectPurchaseIntent,
  requestLlmCompletion,
  type CompletionMessage,
} from "./bifrost.js";
import {
  checkEvolutionConnection,
  downloadMediaFromEvolution,
  sendWhatsAppMessage,
  transcribeAudioGroq,
} from "./evolution.js";
import {
  buildSystemPrompt,
  fetchWebGrounding,
  resolveBrandSSoT,
  sanitizeAndValidateUrls,
  sanitizeTone,
} from "./brandbook.js";
import { cleanEnv } from "./env.js";

const PORT = parseInt(process.env.PORT || "3005", 10);
const NATS_URL = cleanEnv(process.env.NATS_URL);
const BOT_PHONE = cleanEnv(process.env.BOT_PHONE);
const MAX_WEBHOOK_AGE_SECS = 120;

const jsonCodec = JSONCodec();

function cleanPhoneNumber(jidOrPhone: string): string {
  return jidOrPhone.replace(/@.*$/, "").replace(/\D/g, "");
}

function isBriefAcknowledgement(text: string): boolean {
  const clean = text.trim().toLowerCase();
  const acks = ["ok", "gracias", "muchas gracias", "listo", "vale", "dale", "de una", "perfecto", "bueno", "chao"];
  return acks.includes(clean);
}

/**
 * Verifies webhook secret (header secret or HMAC-SHA256 signature) fail-closed.
 */
export function verifyWebhookAuth(
  req: Request,
  rawBody: string,
  configuredSecret?: string
): { ok: boolean; status: number; error?: string } {
  const secret =
    configuredSecret ??
    (cleanEnv(process.env.WHATSAPP_WEBHOOK_SECRET) ||
      cleanEnv(process.env.EVOLUTION_API_KEY) ||
      cleanEnv(process.env.GLOBAL_API_KEY));

  if (!secret) {
    return { ok: false, status: 503, error: "Webhook secret is not configured" };
  }

  const sigHeader = (req.headers.get("x-hub-signature-256") || "").trim();
  if (sigHeader.startsWith("sha256=")) {
    const providedHex = sigHeader.slice("sha256=".length);
    const expectedHex = crypto.createHmac("sha256", secret).update(rawBody).digest("hex");
    const a = Buffer.from(providedHex, "utf8");
    const b = Buffer.from(expectedHex, "utf8");
    if (a.length === b.length && crypto.timingSafeEqual(a, b)) {
      return { ok: true, status: 200 };
    }
    return { ok: false, status: 401, error: "Invalid HMAC signature" };
  }

  const authHeader = (req.headers.get("authorization") || "").trim();
  const bearerToken = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : "";
  const candidate =
    (req.headers.get("x-webhook-secret") || "").trim() ||
    (req.headers.get("apikey") || "").trim() ||
    bearerToken;

  if (!candidate) {
    return { ok: false, status: 401, error: "Missing webhook authentication" };
  }

  const a = Buffer.from(candidate, "utf8");
  const b = Buffer.from(secret, "utf8");
  if (a.length === b.length && crypto.timingSafeEqual(a, b)) {
    return { ok: true, status: 200 };
  }
  return { ok: false, status: 401, error: "Invalid webhook credentials" };
}

/**
 * Extracts event timestamp in epoch milliseconds and rejects stale replays (> 120s).
 */
export function extractPayloadTimestampMs(rawPayload: any): number | null {
  const eventData = rawPayload?.data || rawPayload || {};
  const info = eventData.Info || eventData.info || rawPayload?.Info || rawPayload?.info || {};
  const rawTs =
    info.Timestamp ??
    eventData.messageTimestamp ??
    rawPayload?.messageTimestamp ??
    rawPayload?.timestamp ??
    eventData.timestamp;

  if (rawTs === undefined || rawTs === null || rawTs === "") return null;
  if (typeof rawTs === "number") {
    return rawTs < 1e12 ? rawTs * 1000 : rawTs;
  }
  const asNum = Number(rawTs);
  if (!Number.isNaN(asNum) && asNum > 0) {
    return asNum < 1e12 ? asNum * 1000 : asNum;
  }
  const parsedDate = Date.parse(String(rawTs));
  return Number.isNaN(parsedDate) ? null : parsedDate;
}

export function isTimestampFresh(
  rawPayload: any,
  nowMs = Date.now(),
  maxAgeSecs = MAX_WEBHOOK_AGE_SECS
): boolean {
  const tsMs = extractPayloadTimestampMs(rawPayload);
  if (tsMs === null) return true;
  const ageMs = nowMs - tsMs;
  if (ageMs > maxAgeSecs * 1000) return false;
  if (ageMs < -60_000) return false; // Reject far-future timestamps (> 60s skew)
  return true;
}

export interface InboundDeps {
  setTakeover?: (phone: string, active: boolean, durationSecs: number | null, tsMs: number) => Promise<void>;
  isTakeover?: (phone: string) => Promise<boolean>;
  sendReply?: (phone: string, text: string) => Promise<boolean>;
  acquireLockFn?: (messageId: string) => Promise<boolean>;
  checkRateLimitFn?: (phone: string, limit: number, windowSecs: number) => Promise<boolean>;
  getSessionHistoryFn?: (phone: string, limit: number) => Promise<ChatMessage[]>;
  appendSessionFn?: (phone: string, role: "user" | "assistant", content: string) => Promise<void>;
  getProfileFn?: (phone: string) => Promise<any>;
  getCatalogFn?: () => Promise<any>;
  getWebGroundingFn?: (url: string) => Promise<any>;
  completeLlmFn?: (messages: CompletionMessage[], opts: any) => Promise<string>;
}

export async function processInboundWhatsAppMessage(
  rawPayload: any,
  deps: InboundDeps = {}
): Promise<{ status: string; detail?: string }> {
  if (!isTimestampFresh(rawPayload)) {
    return { status: "ignored_stale_replay" };
  }

  const eventTsMs = extractPayloadTimestampMs(rawPayload) ?? Date.now();
  const eventData = rawPayload?.data || rawPayload || {};
  const info = eventData.Info || eventData.info || rawPayload?.Info || rawPayload?.info || {};
  const key = eventData.key || rawPayload?.key || {};

  const isFromMe = Boolean(
    info.IsFromMe ?? info.isFromMe ?? key.fromMe ?? eventData.fromMe ?? rawPayload?.fromMe ?? false
  );

  const chatJid = String(
    info.Chat || key.remoteJid || eventData.Chat || eventData.chatJid || rawPayload?.chatJid || rawPayload?.remoteJid || ""
  );

  const senderJid = String(
    info.Sender || eventData.Sender || eventData.senderJid || key.participant || rawPayload?.sender || ""
  );

  const senderAlt = String(info.SenderAlt || eventData.SenderAlt || eventData.senderAlt || "");

  const isGroup = Boolean((info.IsGroup ?? false) || chatJid.includes("@g.us") || senderJid.includes("@g.us"));
  if (isGroup || chatJid.includes("status@broadcast")) {
    return { status: "ignored_group_or_status" };
  }

  const allCandidateIds = [
    chatJid,
    senderJid,
    senderAlt,
    key.remoteJid,
    key.participant,
    eventData.Chat,
    eventData.Sender,
    eventData.SenderAlt,
    eventData.remoteJid,
  ]
    .filter(Boolean)
    .map(String);

  const lidCandidate = allCandidateIds.find((id) => id.includes("@lid"));
  const phoneCandidate = allCandidateIds.find(
    (id) => id.includes("@s.whatsapp.net") && cleanPhoneNumber(id) !== cleanPhoneNumber(BOT_PHONE)
  );

  if (lidCandidate && phoneCandidate && !deps.setTakeover) {
    await linkPhoneAndLid(cleanPhoneNumber(phoneCandidate), cleanPhoneNumber(lidCandidate));
  }

  const rawTargetIds = isFromMe
    ? [chatJid, key.remoteJid, eventData.remoteJid]
    : [senderAlt, senderJid, chatJid, key.remoteJid];

  const realPhoneId =
    rawTargetIds.find((id) => id && String(id).includes("@s.whatsapp.net")) ||
    rawTargetIds.find((id) => id && !String(id).includes("@lid") && !String(id).includes("@g.us")) ||
    rawTargetIds[0];

  const rawCleanPhone = cleanPhoneNumber(String(realPhoneId || (isFromMe ? chatJid : senderJid || chatJid)));
  if (!rawCleanPhone || (BOT_PHONE && rawCleanPhone === cleanPhoneNumber(BOT_PHONE))) {
    return { status: "ignored_bot_self_message" };
  }

  let canonicalPhone = rawCleanPhone;
  if (!deps.setTakeover) {
    canonicalPhone = await resolveCanonicalPhone(rawCleanPhone);
    if (canonicalPhone === rawCleanPhone && rawCleanPhone.length > 13) {
      const dbPhone = await lookupPhoneByLidFromDb(rawCleanPhone);
      if (dbPhone) {
        await linkPhoneAndLid(dbPhone, rawCleanPhone);
        canonicalPhone = dbPhone;
      }
    }
  }

  const primaryCustomerPhone = canonicalPhone;
  const messageId = String(info.ID || key.id || eventData.messageId || rawPayload?.messageId || `msg-${Date.now()}`);

  const msgObj = eventData.Message || eventData.message || rawPayload?.Message || rawPayload?.message || {};
  let messageText = String(
    msgObj.conversation ||
      msgObj.extendedTextMessage?.text ||
      msgObj.imageMessage?.caption ||
      msgObj.videoMessage?.caption ||
      eventData.text ||
      rawPayload?.text ||
      ""
  ).trim();

  const contextInfo = msgObj.extendedTextMessage?.contextInfo || msgObj.contextInfo || eventData.contextInfo;
  const quotedMessage = contextInfo?.quotedMessage;
  if (quotedMessage) {
    const quotedText = String(
      quotedMessage.conversation ||
        quotedMessage.extendedTextMessage?.text ||
        quotedMessage.imageMessage?.caption ||
        quotedMessage.videoMessage?.caption ||
        ""
    ).trim();
    if (quotedText && messageText) {
      messageText = `[En respuesta a: "${quotedText}"] ${messageText}`;
    }
  }

  const isAudio = Boolean(
    msgObj.audioMessage ||
      msgObj.AudioMessage ||
      msgObj.mimetype?.includes("audio") ||
      eventData.audioMessage ||
      eventData.AudioMessage ||
      eventData.mimetype?.includes("audio") ||
      eventData.mediaType === "audio" ||
      rawPayload?.mediaType === "audio" ||
      rawPayload?.messageType === "audio" ||
      rawPayload?.messageType === "audioMessage" ||
      eventData.messageType === "audio" ||
      eventData.messageType === "audioMessage"
  );

  const sendFn = deps.sendReply ?? sendWhatsAppMessage;
  if (isAudio && !messageText) {
    let audioData: string | null =
      msgObj.base64 ||
      msgObj.audioMessage?.base64 ||
      msgObj.AudioMessage?.base64 ||
      eventData.base64 ||
      eventData.audioBase64 ||
      rawPayload?.base64 ||
      rawPayload?.data?.base64 ||
      null;

    if (!audioData && (msgObj.audioMessage || msgObj.AudioMessage || eventData.Message)) {
      audioData = await downloadMediaFromEvolution(msgObj);
    }
    if (audioData) {
      const transcription = await transcribeAudioGroq(audioData);
      if (transcription) messageText = transcription.trim();
    }
    if (!messageText) {
      await sendFn(
        primaryCustomerPhone,
        "Recibí tu nota de voz, pero no logré escucharla con claridad. ¿Podrías escribirme tu consulta brevemente?"
      );
      return { status: "audio_transcription_fallback" };
    }
  }

  if (!messageText) {
    return { status: "ignored_empty_payload" };
  }

  const persistTakeover =
    deps.setTakeover ??
    (async (phone: string, active: boolean, durationSecs: number | null, tsMs: number) => {
      const applied = await setTakeoverStateInDb(phone, active, durationSecs, tsMs);
      if (!applied) return;
      if (active && durationSecs !== null) {
        await setHumanTakeover(phone, durationSecs);
      } else if (!active) {
        await clearHumanTakeover(phone);
      }
    });

  const checkTakeover =
    deps.isTakeover ??
    (async (phone: string) => {
      if (await isHumanTakeover(phone)) return true;
      return getTakeoverStateFromDb(phone);
    });

  const appendFn = deps.appendSessionFn ?? appendSessionMessage;

  // 3. Monotonic operator takeover controls (NEVER send internal control notices to customer)
  if (isFromMe) {
    const trimmed = messageText.trim().toLowerCase();
    if (trimmed === "#bot") {
      await persistTakeover(primaryCustomerPhone, false, null, eventTsMs);
      return { status: "human_takeover_cleared" };
    }
    if (["#stop", "#off", "#silencio", "#desactivar"].includes(trimmed)) {
      await persistTakeover(primaryCustomerPhone, true, 315360000, eventTsMs);
      return { status: "human_takeover_stopped_permanent" };
    }
    if (trimmed === "#mute") {
      await persistTakeover(primaryCustomerPhone, true, 86400, eventTsMs);
      return { status: "human_takeover_muted_24h" };
    }

    await persistTakeover(primaryCustomerPhone, true, 7200, eventTsMs);
    await appendFn(primaryCustomerPhone, "assistant", `[Operador humano]: ${messageText}`);
    return { status: "human_takeover_active_2h" };
  }

  const cleanLower = messageText.trim().toLowerCase();
  const isHumanRequest =
    ["#stop", "#off", "#silencio", "#desactivar"].includes(cleanLower) ||
    /^(asesor|humano|persona|hablar con un humano|hablar con una persona|asesor humano)$/i.test(cleanLower) ||
    cleanLower.includes("asesor humano") ||
    cleanLower.includes("comunicarme con un humano") ||
    cleanLower.includes("hablar con una persona");

  if (isHumanRequest) {
    await persistTakeover(primaryCustomerPhone, true, 172800, eventTsMs);
    await appendFn(primaryCustomerPhone, "user", messageText);
    const ackMessage =
      "Entendido. He pausado la atención automática para que un asesor de nuestro equipo te responda personalmente en este chat.";
    await sendFn(primaryCustomerPhone, ackMessage);
    await appendFn(primaryCustomerPhone, "assistant", ackMessage);
    return { status: "customer_requested_human_takeover" };
  }

  if (await checkTakeover(primaryCustomerPhone)) {
    await appendFn(primaryCustomerPhone, "user", messageText);
    return { status: "ignored_human_takeover_active" };
  }

  const lockFn = deps.acquireLockFn ?? acquireLock;
  if (!(await lockFn(messageId))) {
    return { status: "ignored_duplicate_message" };
  }

  const rateFn = deps.checkRateLimitFn ?? checkRateLimit;
  if (!(await rateFn(primaryCustomerPhone, 10, 60))) {
    return { status: "ignored_rate_limited" };
  }

  if (isBriefAcknowledgement(messageText)) {
    await appendFn(primaryCustomerPhone, "user", messageText);
    return { status: "silence_brief_ack" };
  }

  const brand = resolveBrandSSoT();
  const [customerProfile, publicCatalog, webGrounding] = await Promise.all([
    (deps.getProfileFn ?? getVerifiedCustomerProfile)(primaryCustomerPhone),
    (deps.getCatalogFn ?? getPublicCatalogFromDb)(),
    (deps.getWebGroundingFn ?? fetchWebGrounding)(brand.canonicalUrls.home),
  ]);

  if (!deps.getProfileFn) {
    recordIncomingLead(primaryCustomerPhone).catch(() => {});
  }

  const sessionHistory = await (deps.getSessionHistoryFn ?? getSessionHistory)(primaryCustomerPhone, 20);
  const systemPrompt = buildSystemPrompt(customerProfile, publicCatalog, webGrounding);

  const messages: CompletionMessage[] = [
    { role: "system", content: systemPrompt },
    ...sessionHistory.map((m) => ({ role: m.role, content: m.content })),
    { role: "user", content: messageText },
  ];

  const isPurchase = detectPurchaseIntent(messageText);
  const tier = isPurchase ? "production" : "auto";
  const completeFn = deps.completeLlmFn ?? requestLlmCompletion;

  let replyText = "";
  try {
    replyText = await completeFn(messages, { tier });
  } catch (err) {
    if (tier === "auto") {
      replyText = await completeFn(messages, { tier: "production" });
    } else {
      throw err;
    }
  }

  let finalReply = sanitizeAndValidateUrls(replyText, webGrounding?.baseUrl, webGrounding?.allowedPaths);
  finalReply = sanitizeTone(finalReply);

  if (!deps.sendReply) {
    const typingDelayMs = Math.min(5000, Math.max(1500, finalReply.length * 20));
    await new Promise((resolve) => setTimeout(resolve, typingDelayMs));
  }

  await sendFn(primaryCustomerPhone, finalReply);
  await appendFn(primaryCustomerPhone, "user", messageText);
  await appendFn(primaryCustomerPhone, "assistant", finalReply);

  return { status: "processed", detail: `Dispatched ${finalReply.length} chars to ${primaryCustomerPhone}` };
}

/**
 * Handles HTTP POST /webhook with fail-closed auth and anti-replay validation.
 */
export async function handleWebhookRequest(
  req: Request,
  opts: { secret?: string; dispatchFn?: (payload: any) => Promise<any> } = {}
): Promise<Response> {
  const rawBody = await req.text();
  const auth = verifyWebhookAuth(req, rawBody, opts.secret);
  if (!auth.ok) {
    return new Response(JSON.stringify({ error: auth.error }), {
      status: auth.status,
      headers: { "Content-Type": "application/json" },
    });
  }

  let body: any;
  try {
    body = JSON.parse(rawBody);
  } catch (err: any) {
    return new Response(JSON.stringify({ error: err?.message || "Invalid JSON" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  if (!isTimestampFresh(body)) {
    return new Response(JSON.stringify({ status: "ignored_stale_replay" }), {
      status: 403,
      headers: { "Content-Type": "application/json" },
    });
  }

  const dispatch = opts.dispatchFn ?? processInboundWhatsAppMessage;
  dispatch(body).catch((err) => console.error("[Webhook] Background error:", err));

  return new Response(JSON.stringify({ status: "acknowledged" }), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });
}

async function startNatsConsumer() {
  if (!NATS_URL) {
    console.warn("[NATS] NATS_URL not provided, skipping NATS subscriber");
    return;
  }

  try {
    const parsed = new URL(NATS_URL);
    const nc = await connect({
      servers: `${parsed.hostname}:${parsed.port || "4222"}`,
      user: parsed.username ? decodeURIComponent(parsed.username) : undefined,
      pass: parsed.password ? decodeURIComponent(parsed.password) : undefined,
    });

    // Ensure JetStream stream exists on nats:single@2.12 (idempotent)
    try {
      const jsm = await nc.jetstreamManager();
      await jsm.streams.add({
        name: "EVOLUTION_EVENTS",
        subjects: ["message", "sendmessage", "whatsapp.messages"],
      });
      console.log("[NATS] JetStream stream 'EVOLUTION_EVENTS' ready");
    } catch {
      // Stream already exists or JetStream disabled; Core NATS subscription still runs
    }

    const subjects = ["message", "whatsapp.messages"];
    for (const subject of subjects) {
      const sub = nc.subscribe(subject, { queue: "whatsapp-worker-group" });
      console.log(`[NATS] Subscribed to subject '${subject}' (queue: whatsapp-worker-group)`);
      (async () => {
        for await (const msg of sub) {
          try {
            const payload = jsonCodec.decode(msg.data);
            await processInboundWhatsAppMessage(payload);
          } catch (err) {
            console.error(`[NATS] Error processing message on '${subject}':`, err);
          }
        }
      })();
    }
  } catch (err) {
    console.warn("[NATS] NATS broker connection skipped (relying on HTTP webhook):", err);
  }
}

if (import.meta.main) {
  Bun.serve({
    port: PORT,
    async fetch(req: Request) {
      const url = new URL(req.url);

      if (url.pathname === "/health" || url.pathname === "/healthz") {
        const [dbOk, valkeyOk, bifrostOk, evoOk] = await Promise.all([
          checkDbConnection(),
          checkValkeyConnection(),
          checkBifrostConnection(),
          checkEvolutionConnection(),
        ]);

        const brand = resolveBrandSSoT();
        const healthy = dbOk && valkeyOk && bifrostOk;

        return new Response(
          JSON.stringify({
            status: healthy ? "ok" : "degraded",
            service: "whatsappbot",
            uptime: process.uptime(),
            checks: {
              database: dbOk ? "healthy" : "failing",
              valkey: valkeyOk ? "healthy" : "failing",
              bifrost: bifrostOk ? "healthy" : "failing",
              evolution: evoOk ? "healthy" : "failing",
              brandbook: brand.brandName ? "loaded" : "fallback",
            },
            brand: {
              name: brand.brandName,
              archetype: brand.archetype,
            },
          }),
          { status: healthy ? 200 : 503, headers: { "Content-Type": "application/json" } }
        );
      }

      if (url.pathname === "/webhook" && req.method === "POST") {
        return handleWebhookRequest(req);
      }

      return new Response(JSON.stringify({ service: "whatsappbot", status: "running" }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    },
  });

  console.log(`[whatsappbot] HTTP Server running on port :${PORT}`);
  startNatsConsumer();
  syncLidMappingsToValkey(redis).catch((err) => console.warn("[Startup] LID sync error:", err));
}
