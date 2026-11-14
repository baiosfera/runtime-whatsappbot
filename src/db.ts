import pg from "pg";
import { cleanEnv } from "./env.js";

const { Pool } = pg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: Number(process.env.PG_POOL_MAX || 3),
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

export interface CustomerData {
  id?: string;
  alias?: string;
  phone: string;
  email?: string;
  city?: string;
  isRegistered: boolean;
  recentOrders: Array<{
    summary: string;
    status: string;
    total: string;
    date: string;
  }>;
}

export interface PublicCatalogContext {
  itemsSummary: string;
  tablesInspected: string[];
}

export interface TakeoverRecord {
  phone: string;
  active: boolean;
  untilAt: Date | null;
  updatedAtMs: number;
}

let tablesCache: Set<string> | null = null;
let columnsCache: Map<string, Set<string>> = new Map();
let takeoverTableReady = false;

export function buildPhoneVariants(rawPhone: string): { digits: string; last10: string; variants: string[] } {
  const digits = rawPhone.replace(/\D/g, "");
  const last10 = digits.length >= 10 ? digits.slice(-10) : digits;
  const cc = cleanEnv(process.env.DEFAULT_COUNTRY_CODE).replace(/\D/g, "");
  const variants = new Set<string>([digits, `+${digits}`]);
  if (last10) variants.add(last10);
  if (cc && digits) {
    if (digits.startsWith(cc)) {
      variants.add(digits.slice(cc.length));
    } else {
      variants.add(`${cc}${digits}`);
      variants.add(`+${cc}${digits}`);
    }
  }
  return { digits, last10, variants: Array.from(variants).filter(Boolean) };
}

async function getAvailableTables(): Promise<Set<string>> {
  if (tablesCache) return tablesCache;
  try {
    const res = await pool.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
    tablesCache = new Set(res.rows.map((r: any) => String(r.table_name)));
    return tablesCache;
  } catch {
    return new Set();
  }
}

async function getTableColumns(tableName: string): Promise<Set<string>> {
  if (columnsCache.has(tableName)) return columnsCache.get(tableName)!;
  try {
    const res = await pool.query(
      "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1",
      [tableName]
    );
    const cols = new Set(res.rows.map((r: any) => String(r.column_name)));
    columnsCache.set(tableName, cols);
    return cols;
  } catch {
    return new Set();
  }
}

export async function checkDbConnection(): Promise<boolean> {
  try {
    const res = await pool.query("SELECT 1 AS ok");
    return res.rows[0]?.ok === 1;
  } catch (err) {
    console.error("[PostgreSQL] Connection check failed:", err);
    return false;
  }
}

/**
 * Ensures monotonic takeover persistence table exists in PostgreSQL 18.
 */
export async function ensureTakeoverTable(): Promise<void> {
  if (takeoverTableReady) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS whatsapp_takeovers (
      phone TEXT PRIMARY KEY,
      active BOOLEAN NOT NULL DEFAULT FALSE,
      until_at TIMESTAMPTZ,
      updated_at_ms BIGINT NOT NULL
    )
  `);
  takeoverTableReady = true;
}

/**
 * Pure monotonic merge helper (also used by unit tests).
 */
export function shouldApplyTakeoverUpdate(
  currentUpdatedAtMs: number | null | undefined,
  incomingUpdatedAtMs: number
): boolean {
  if (currentUpdatedAtMs === null || currentUpdatedAtMs === undefined) return true;
  return incomingUpdatedAtMs >= currentUpdatedAtMs;
}

export async function setTakeoverStateInDb(
  phone: string,
  active: boolean,
  durationSecs: number | null,
  eventTimestampMs = Date.now()
): Promise<boolean> {
  const { digits } = buildPhoneVariants(phone);
  if (!digits) return false;
  try {
    await ensureTakeoverTable();
    const untilAt = active && durationSecs !== null ? new Date(eventTimestampMs + durationSecs * 1000) : null;
    const res = await pool.query(
      `INSERT INTO whatsapp_takeovers (phone, active, until_at, updated_at_ms)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (phone) DO UPDATE
       SET active = EXCLUDED.active,
           until_at = EXCLUDED.until_at,
           updated_at_ms = EXCLUDED.updated_at_ms
       WHERE whatsapp_takeovers.updated_at_ms <= EXCLUDED.updated_at_ms
       RETURNING active`,
      [digits, active, untilAt, eventTimestampMs]
    );
    return res.rowCount !== null && res.rowCount > 0;
  } catch (err) {
    console.warn("[PostgreSQL] Failed persisting takeover state:", err);
    return false;
  }
}

export async function getTakeoverStateFromDb(phone: string): Promise<boolean> {
  const { digits } = buildPhoneVariants(phone);
  if (!digits) return false;
  try {
    await ensureTakeoverTable();
    const res = await pool.query(
      `SELECT active, until_at FROM whatsapp_takeovers WHERE phone = $1 LIMIT 1`,
      [digits]
    );
    const row = res.rows[0];
    if (!row || !row.active) return false;
    if (row.until_at && new Date(row.until_at) <= new Date()) {
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

/**
 * Loads ONLY public non-PII catalog data via dynamic table introspection.
 */
export async function getPublicCatalogFromDb(): Promise<PublicCatalogContext> {
  const tables = await getAvailableTables();
  const candidateTables = ["products", "services", "events", "catalog", "plans", "courses"];
  const inspected: string[] = [];
  const lines: string[] = [];

  for (const tbl of candidateTables) {
    if (!tables.has(tbl)) continue;
    try {
      const cols = await getTableColumns(tbl);
      const nameCol = ["name", "title", "nombre", "titulo"].find((c) => cols.has(c));
      if (!nameCol) continue;
      const priceCol = ["price", "precio", "amount", "monto"].find((c) => cols.has(c));
      const slugCol = ["slug", "url", "path"].find((c) => cols.has(c));
      const statusCol = ["status", "estado", "active", "activo"].find((c) => cols.has(c));

      const selectCols = [nameCol, priceCol, slugCol, statusCol].filter(Boolean) as string[];
      const res = await pool.query(
        `SELECT ${selectCols.map((c) => `"${c}"`).join(", ")} FROM "${tbl}" LIMIT 15`
      );
      if (res.rows.length > 0) {
        inspected.push(tbl);
        lines.push(`[${tbl}]`);
        for (const r of res.rows) {
          const parts = [String(r[nameCol] || "")];
          if (priceCol && r[priceCol] !== undefined && r[priceCol] !== null) parts.push(`precio=${r[priceCol]}`);
          if (slugCol && r[slugCol]) parts.push(`ruta=${r[slugCol]}`);
          if (statusCol && r[statusCol] !== undefined) parts.push(`estado=${r[statusCol]}`);
          lines.push(`- ${parts.join(" | ")}`);
        }
      }
    } catch {
      // Skip tables that cannot be queried cleanly
    }
  }

  return {
    itemsSummary: lines.join("\n"),
    tablesInspected: inspected,
  };
}

/**
 * Loads verified sender profile strictly scoped to the authenticated WhatsApp phone.
 */
export async function getVerifiedCustomerProfile(verifiedPhone: string): Promise<CustomerData> {
  const { digits, last10, variants } = buildPhoneVariants(verifiedPhone);

  const result: CustomerData = {
    phone: digits,
    isRegistered: false,
    recentOrders: [],
  };

  if (!digits) return result;

  try {
    const tables = await getAvailableTables();

    if (tables.has("customers")) {
      const cols = await getTableColumns("customers");
      const phoneCol = ["phone", "whatsapp", "telefono", "phone_number"].find((c) => cols.has(c));
      if (phoneCol) {
        const nameCol = ["name", "full_name", "nombre", "alias"].find((c) => cols.has(c));
        const emailCol = cols.has("email") ? "email" : null;
        const cityCol = ["city", "ciudad"].find((c) => cols.has(c));
        const sel = ["id", nameCol, emailCol, cityCol].filter(Boolean) as string[];
        const res = await pool.query(
          `SELECT ${sel.map((c) => `"${c}"`).join(", ")}
           FROM customers
           WHERE "${phoneCol}" = ANY($1::varchar[]) OR RIGHT(regexp_replace("${phoneCol}", '\\D', '', 'g'), 10) = $2
           LIMIT 1`,
          [variants, last10]
        );
        const row = res.rows[0];
        if (row) {
          result.id = row.id ? String(row.id) : undefined;
          result.alias = nameCol && row[nameCol] ? String(row[nameCol]) : undefined;
          result.email = emailCol && row[emailCol] ? String(row[emailCol]) : undefined;
          result.city = cityCol && row[cityCol] ? String(row[cityCol]) : undefined;
          result.isRegistered = true;
        }
      }
    }

    if (!result.isRegistered && tables.has("leads")) {
      const cols = await getTableColumns("leads");
      const phoneCol = ["whatsapp", "phone", "telefono"].find((c) => cols.has(c));
      if (phoneCol) {
        const nameCol = ["alias_nombre", "name", "nombre"].find((c) => cols.has(c));
        const emailCol = cols.has("email") ? "email" : null;
        const cityCol = ["ciudad", "city"].find((c) => cols.has(c));
        const sel = ["id", nameCol, emailCol, cityCol].filter(Boolean) as string[];
        const res = await pool.query(
          `SELECT ${sel.map((c) => `"${c}"`).join(", ")}
           FROM leads
           WHERE "${phoneCol}" = ANY($1::varchar[]) OR RIGHT(regexp_replace("${phoneCol}", '\\D', '', 'g'), 10) = $2
           ORDER BY updated_at DESC NULLS LAST, created_at DESC
           LIMIT 1`,
          [variants, last10]
        );
        const row = res.rows[0];
        if (row) {
          result.id = row.id ? String(row.id) : undefined;
          result.alias = nameCol && row[nameCol] ? String(row[nameCol]) : undefined;
          result.email = emailCol && row[emailCol] ? String(row[emailCol]) : undefined;
          result.city = cityCol && row[cityCol] ? String(row[cityCol]) : undefined;
          result.isRegistered = Boolean(result.alias || result.email);
        }
      }
    }

    if (!result.isRegistered && tables.has("user")) {
      const cols = await getTableColumns("user");
      if (cols.has("phoneNumber")) {
        const res = await pool.query(
          `SELECT id, name, email FROM "user"
           WHERE "phoneNumber" = ANY($1::varchar[]) OR RIGHT(regexp_replace("phoneNumber", '\\D', '', 'g'), 10) = $2
           LIMIT 1`,
          [variants, last10]
        );
        const row = res.rows[0];
        if (row) {
          result.id = String(row.id);
          result.alias = row.name || undefined;
          result.email = row.email || undefined;
          result.isRegistered = true;
        }
      }
    }
  } catch (err) {
    console.error(`[PostgreSQL] Error fetching verified profile for ${digits}:`, err);
  }

  return result;
}

export async function recordIncomingLead(phone: string, alias?: string): Promise<void> {
  const { digits, last10, variants } = buildPhoneVariants(phone);
  if (!digits) return;
  const canonicalPhone = `+${digits}`;

  try {
    const tables = await getAvailableTables();
    if (!tables.has("leads")) return;
    const cols = await getTableColumns("leads");
    const phoneCol = ["whatsapp", "phone", "telefono"].find((c) => cols.has(c));
    if (!phoneCol) return;

    const existing = await pool.query(
      `SELECT id FROM leads WHERE "${phoneCol}" = ANY($1::varchar[]) OR RIGHT(regexp_replace("${phoneCol}", '\\D', '', 'g'), 10) = $2 LIMIT 1`,
      [variants, last10]
    );

    if (existing.rows.length === 0) {
      if (cols.has("alias_nombre")) {
        await pool.query(
          `INSERT INTO leads ("${phoneCol}", alias_nombre) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
          [canonicalPhone, alias || null]
        );
      } else {
        await pool.query(
          `INSERT INTO leads ("${phoneCol}") VALUES ($1) ON CONFLICT DO NOTHING`,
          [canonicalPhone]
        );
      }
    }
  } catch {
    // Non-blocking lead capture
  }
}

export async function lookupPhoneByLidFromDb(lid: string): Promise<string | null> {
  const cleanLid = lid.replace(/\D/g, "");
  if (!cleanLid) return null;
  try {
    const res = await pool.query(
      `SELECT phone FROM whatsapp_lid_mappings WHERE lid = $1 LIMIT 1`,
      [cleanLid]
    );
    return res.rows[0]?.phone || null;
  } catch {
    return null;
  }
}

export async function syncLidMappingsToValkey(redisClient: any): Promise<void> {
  try {
    const tables = await getAvailableTables();
    if (!tables.has("whatsapp_lid_mappings")) return;
    const res = await pool.query("SELECT lid, phone FROM whatsapp_lid_mappings LIMIT 1000");
    if (res.rows.length === 0) return;
    const pipeline = redisClient.pipeline();
    for (const row of res.rows) {
      if (row.lid && row.phone) {
        pipeline.set(`wa:map:lid:${row.lid}`, row.phone, "EX", 2592000);
        pipeline.set(`wa:map:phone:${row.phone}`, row.lid, "EX", 2592000);
      }
    }
    await pipeline.exec();
  } catch {
    // Optional table
  }
}
