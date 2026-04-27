"use strict";
require("dotenv").config();
const cron = require("node-cron");
const Afip = require("@afipsdk/afip.js");
const nodemailer = require("nodemailer");
const Resend = require("resend").Resend;
const resendClient = process.env.RESEND_API_KEY ? new Resend(process.env.RESEND_API_KEY) : null;
const express = require("express");
const cors = require("cors");
const fs = require("fs");
const path = require("path");
const QRCode = require("qrcode");
const https = require("https");
const multer = require("multer");
const crypto = require("crypto");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const pdfParseModule = require("pdf-parse");
const pdfParse = pdfParseModule?.default || pdfParseModule;
const XLSX = require("xlsx");

// ================================================================
// ✅ SUPABASE — persistencia real (sobrevive redeploys de Render)
// ================================================================
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_KEY = process.env.SUPABASE_KEY || "";
const SUPABASE_STORAGE_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || "facturas-pdf";

let supabase = null;
if (SUPABASE_URL && SUPABASE_KEY) {
  supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  console.log("✅ [Supabase] Cliente inicializado");
} else {
  console.warn("⚠️  [Supabase] Sin credenciales — usando solo archivo local (riesgo de pérdida en redeploy)");
}

const app = express();
app.use(express.json({ limit: "50mb" }));
app.use(cors());

const APP_VERSION = "2026-04-EMAIL-EXTRACTO-TOTALES";
const DEBUG = String(process.env.DEBUG || "0") === "1";

const CUIT_DISTRIBUIDORA = Number(process.env.CUIT_DISTRIBUIDORA);
const AFIPSDK_ACCESS_TOKEN = process.env.AFIPSDK_ACCESS_TOKEN;

const GMAIL_USER = process.env.GMAIL_USER;
const GMAIL_APP_PASS = (process.env.GMAIL_APP_PASS || "").replace(/\s+/g, "");
const DEFAULT_EMAIL = "distribuidoramercadolimpio@gmail.com";

const PUNTO_VENTA_ENV = Number(process.env.PUNTO_VENTA || 0);
const PRODUCTION = String(process.env.PRODUCTION || "true").toLowerCase() === "true";
const PORT = Number(process.env.PORT || 3000);

const CBTE_TIPO_REAL = 51;
const ITEMS_POR_FACTURA = 25;
const PUBLIC_URL = String(process.env.PUBLIC_URL || "https://api-mercadolimpio.onrender.com").replace(/\/+$/, "");
const ENABLE_PADRON_10 = String(process.env.ENABLE_PADRON_10 || "false").toLowerCase() === "true";

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
let genAI = null;
let geminiModel = null;
if (GEMINI_API_KEY) {
  genAI = new GoogleGenerativeAI(GEMINI_API_KEY);
  geminiModel = genAI.getGenerativeModel({
    model: "gemini-2.5-flash",
    generationConfig: { responseMimeType: "application/json", temperature: 0.1 }
  });
}

const EMISOR = {
  nombreVisible: "MERCADO LIMPIO DISTRIBUIDORA",
  domicilio: "Languenhein 1095 - Longchamps, Buenos Aires",
  condicionIVA: "IVA Responsable Inscripto",
  leyenda: "OPERACIÓN SUJETA A RETENCIÓN",
  condicionVentaDefault: "Transferencia Bancaria",
};

function log(...args) { if (DEBUG) console.log(...args); }
function errlog(...args) { console.error(...args); }

// --- HELPERS ---
const pad = (n, len) => String(n).padStart(len, "0");
const onlyDigits = (s) => String(s ?? "").replace(/\D/g, "");
function safeText(s) { return String(s ?? "").replace(/[<>]/g, ""); }
const round2 = (n) => Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;

function todayISO() {
  return new Date(Date.now() - new Date().getTimezoneOffset() * 60000).toISOString().split("T")[0];
}
function yyyymmdd(iso) { return Number(String(iso).replace(/-/g, "")); }

function parseMoneyArToNumber(v) {
  const s = String(v ?? "").trim().replace(/\./g, "").replace(",", ".");
  const n = Number(s);
  return Number.isFinite(n) ? round2(n) : 0;
}
function formatMoneyAR(n) {
  return new Intl.NumberFormat("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(n || 0));
}

function cleanupTempFile(p) { try { if (p && fs.existsSync(p)) fs.unlinkSync(p); } catch {} }

async function downloadToBuffer(url) {
  if (typeof fetch === "function") {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`No pude descargar: ${r.status}`);
    return Buffer.from(await r.arrayBuffer());
  }
  return new Promise((resolve, reject) => {
    https.get(url, (resp) => {
      if (resp.statusCode !== 200) return reject(new Error(`Status: ${resp.statusCode}`));
      const chunks = [];
      resp.on("data", (c) => chunks.push(c));
      resp.on("end", () => resolve(Buffer.concat(chunks)));
    }).on("error", reject);
  });
}

const afip = new Afip({
  CUIT: CUIT_DISTRIBUIDORA,
  cert: fs.readFileSync("certificado.crt", "utf-8"),
  key: fs.readFileSync("privada.key", "utf-8"),
  access_token: AFIPSDK_ACCESS_TOKEN,
  production: PRODUCTION,
});

const transporter = nodemailer.createTransport({
  host: "smtp.gmail.com",
  port: 587,
  secure: false, // ⚠️ Cambia a false para este puerto
  requireTLS: true, // ⚠️ Obliga a usar conexión segura igual
  family: 4, // Esto dejalo, está perfecto
  auth: { user: GMAIL_USER, pass: GMAIL_APP_PASS }
});

if (!GMAIL_USER || !GMAIL_APP_PASS) {
  console.warn("⚠️ [Email] Faltan credenciales Gmail en variables de entorno");
} else {
  console.log(`✅ [Email] Gmail configurado para: ${GMAIL_USER}`);
}

// Gmail configurado como fallback (envíos van por Resend)

const uploadDir = path.join(process.cwd(), "uploads");
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });

const publicPdfDir = path.join(uploadDir, "public_pdfs");
if (!fs.existsSync(publicPdfDir)) fs.mkdirSync(publicPdfDir, { recursive: true });

app.use("/public_pdfs", express.static(publicPdfDir, {
  setHeaders(res, filePath) {
    if (String(filePath || "").toLowerCase().endsWith(".pdf")) {
      res.setHeader("Content-Type", "application/pdf");
      res.setHeader("Content-Disposition", "inline");
      res.setHeader("Cache-Control", "public, max-age=3600");
    }
  }
}));

function safeFileName(s) {
  return String(s || "")
    .replace(/\s+/g, "_")
    .replace(/[^a-zA-Z0-9._-]/g, "_")
    .replace(/_+/g, "_")
    .slice(0, 80);
}
function guessPdfContentType() {
  return "application/pdf";
}

async function savePdfToSupabaseStorage(buffer, baseNameNoExt) {
  if (!supabase) {
    throw new Error("Supabase no está configurado");
  }

  const stamp = Date.now().toString(36) + "_" + crypto.randomBytes(4).toString("hex");
  const fname = `${safeFileName(baseNameNoExt)}_${stamp}.pdf`;
  const storagePath = `pdf/${fname}`;

  const { error: uploadError } = await supabase.storage
    .from(SUPABASE_STORAGE_BUCKET)
    .upload(storagePath, buffer, {
      contentType: guessPdfContentType(),
      upsert: false
    });

  if (uploadError) {
    throw uploadError;
  }

  const { data: publicData } = supabase.storage
    .from(SUPABASE_STORAGE_BUCKET)
    .getPublicUrl(storagePath);

  const publicUrl = publicData?.publicUrl || "";
  if (!publicUrl) {
    throw new Error("No se pudo obtener la URL pública del PDF");
  }

  return publicUrl;
}

async function savePublicPdf(buffer, baseNameNoExt) {
  if (supabase) {
    try {
      const url = await savePdfToSupabaseStorage(buffer, baseNameNoExt);
      console.log(`✅ [Storage] PDF subido a Supabase Storage: ${url}`);
      return url;
    } catch (e) {
      console.error("⚠️ [Storage] Falló subida a Supabase Storage, uso disco local:", e?.message || e);
    }
  }

  const stamp = Date.now().toString(36) + "_" + crypto.randomBytes(4).toString("hex");
  const fname = `${safeFileName(baseNameNoExt)}_${stamp}.pdf`;
  const fpath = path.join(publicPdfDir, fname);
  fs.writeFileSync(fpath, buffer);
  return `${PUBLIC_URL}/public_pdfs/${fname}`;
}

function cleanupOldPublicPdfs(maxAgeMs = 7 * 24 * 60 * 60 * 1000) {
  try {
    const now = Date.now();
    const files = fs.readdirSync(publicPdfDir);
    for (const f of files) {
      if (!String(f).toLowerCase().endsWith(".pdf")) continue;
      const fp = path.join(publicPdfDir, f);
      const st = fs.statSync(fp);
      if (now - st.mtimeMs > maxAgeMs) { try { fs.unlinkSync(fp); } catch {} }
    }
  } catch {}
}
setInterval(() => cleanupOldPublicPdfs(), 6 * 60 * 60 * 1000).unref?.();

const upload = multer({ dest: uploadDir, limits: { fileSize: 50 * 1024 * 1024 } });

// Multer en memoria para el mailer de proveedores (no necesita disco)
const mailerUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 }
});

let pvCache = null;
async function getPtoVentaSeguro() {
  if (pvCache) return pvCache;
  const puntos = await afip.ElectronicBilling.getSalesPoints();
  const habilitados = (Array.isArray(puntos) ? puntos : [])
    .map((p) => Number(p.Nro))
    .filter((n) => n > 0);
  if (!habilitados.length) throw new Error("No hay PV habilitados.");
  if (habilitados.includes(PUNTO_VENTA_ENV)) return (pvCache = PUNTO_VENTA_ENV);
  return (pvCache = habilitados[0]);
}

// ============================
// PADRÓN A13 + Constancia
// ============================
function buildDomicilioString(df) {
  if (!df) return "";
  const calle = df.calle || df.direccion || df.domicilio || df.nombreCalle || df.street || "";
  const nro   = df.numero || df.nro || df.num || df.numeroCalle || "";
  const piso  = df.piso || "";
  const dpto  = df.departamento || df.dpto || "";
  const loc   = df.localidad || df.descripcionLocalidad || "";
  const prov  = df.descripcionProvincia || df.provincia || df.descProvincia || "";
  const cp    = df.codPostal || df.codigoPostal || df.cp || "";
  const linea1 = [calle, nro].filter(Boolean).join(" ").trim();
  const linea2 = [piso ? `Piso ${piso}` : "", dpto ? `Dto ${dpto}` : ""].filter(Boolean).join(" ").trim();
  const out = [linea1, linea2, loc, prov, cp ? `CP: ${cp}` : ""].filter(Boolean).join(" - ");
  return String(out || "").replace(/\s{2,}/g, " ").trim();
}

function normalizePadronDetails(padron, cuitCliente) {
  const base = { nombre: `CUIT ${cuitCliente}`, domicilioAfip: "", condicionIVA: "IVA Responsable Inscripto" };
  if (!padron) return base;
  const dg = padron?.datosGenerales || padron;
  const nombre =
    (dg ? [dg.apellido, dg.nombre].filter(Boolean).join(" ").trim() : "") ||
    dg?.razonSocial || dg?.denominacion || dg?.nombre || dg?.razonSocialNombre || "";
  const df =
    padron?.datosGenerales?.domicilioFiscal || padron?.domicilioFiscal ||
    dg?.domicilioFiscal || dg?.domicilio || padron?.domicilio || null;
  let domicilio = buildDomicilioString(df);
  if (!domicilio) {
    const arr = padron?.datosGenerales?.domicilios || padron?.domicilios || dg?.domicilios || [];
    if (Array.isArray(arr) && arr.length) domicilio = buildDomicilioString(arr[0]);
  }
  return { nombre: nombre || base.nombre, domicilioAfip: domicilio || "", condicionIVA: "IVA Responsable Inscripto" };
}

const padronCache = new Map();
const PADRON_TTL_MS = 12 * 60 * 60 * 1000;

async function getReceptorDesdePadron(cuitCliente) {
  const cuitStr = onlyDigits(cuitCliente);
  const cuitNum = Number(cuitStr);
  const base = { nombre: `CUIT ${cuitCliente}`, domicilioAfip: "", condicionIVA: "IVA Responsable Inscripto" };
  if (!cuitStr || cuitStr.length !== 11) return base;
  const cached = padronCache.get(cuitStr);
  if (cached && Date.now() < cached.exp) return cached.data;

  try {
    const p13 = await afip.RegisterScopeThirteen.getTaxpayerDetails(cuitNum);
    const r13 = normalizePadronDetails(p13, cuitCliente);
    if (r13.nombre && r13.nombre !== base.nombre) base.nombre = r13.nombre;
    if (r13.domicilioAfip) {
      padronCache.set(cuitStr, { data: r13, exp: Date.now() + PADRON_TTL_MS });
      return r13;
    }
  } catch (e) { if (DEBUG) errlog("PADRON A13 error:", e?.message || e); }

  if (ENABLE_PADRON_10 && afip.RegisterScopeTen?.getTaxpayerDetails) {
    try {
      const p10 = await afip.RegisterScopeTen.getTaxpayerDetails(cuitNum);
      const r10 = normalizePadronDetails(p10, cuitCliente);
      if (r10.nombre && r10.nombre !== base.nombre) base.nombre = r10.nombre;
      if (r10.domicilioAfip) {
        padronCache.set(cuitStr, { data: r10, exp: Date.now() + PADRON_TTL_MS });
        return r10;
      }
    } catch (e) { if (DEBUG) errlog("PADRON A10 error:", e?.message || e); }
  }

  try {
    const svc = afip.RegisterInscriptionProof || afip.InscriptionProof || afip.RegistrationProof || null;
    if (svc && typeof svc.getTaxpayerDetails === "function") {
      const pc = await svc.getTaxpayerDetails(cuitNum);
      const rc = normalizePadronDetails(pc, cuitCliente);
      if (rc.nombre && rc.nombre !== base.nombre) base.nombre = rc.nombre;
      if (rc.domicilioAfip) {
        padronCache.set(cuitStr, { data: rc, exp: Date.now() + PADRON_TTL_MS });
        return rc;
      }
    }
  } catch (e) { if (DEBUG) errlog("CONSTANCIA error:", e?.message || e); }

  padronCache.set(cuitStr, { data: base, exp: Date.now() + PADRON_TTL_MS });
  return base;
}

// ============================
// TEXTO PDF
// ============================
function normalizePdfText(texto) {
  return String(texto || "")
    .replace(/\u00A0/g, " ").replace(/\t/g, " ").replace(/\r/g, "")
    .replace(/[ ]{2,}/g, " ").trim();
}

function extractAllCuitts(texto) {
  const a = new Set();
  for (const m of String(texto).matchAll(/\b(\d{2})-(\d{8})-(\d)\b/g)) a.add(`${m[1]}${m[2]}${m[3]}`);
  for (const m of String(texto).matchAll(/\b(20|23|24|27|30|33|34)\d{9}\b/g)) a.add(m[0]);
  return [...a];
}
function pickCuitCliente(texto) {
  const cuits = extractAllCuitts(texto);
  if (!cuits.length) return "";
  const emisor = String(CUIT_DISTRIBUIDORA || "");
  return cuits.find((c) => c !== emisor) || cuits[0];
}

// ============================
// DESCUENTO GLOBAL + TOTALES
// ============================
const MONEY_RX = "\\d{1,3}(?:\\.\\d{3})*(?:,\\d{2})";

function extractDocPricingSummary(texto) {
  const lines = String(texto || "").split(/\n/).map(l => l.trim()).filter(Boolean);
  const tail = lines.slice(-180);
  const rxMoneyOnlyLine = new RegExp(`^(${MONEY_RX})$`);
  const moneyOnly = [];
  for (let i = 0; i < tail.length; i++) {
    const mm = tail[i].match(rxMoneyOnlyLine);
    if (mm) moneyOnly.push({ v: parseMoneyArToNumber(mm[1]), pos: i });
  }

  let descuentoPct = 0, descuentoImporte = 0, discStrategy = "none";
  const rxPctFirst = new RegExp(`^\\s*(\\d{1,2}(?:[\\.,]\\d{1,2})?)\\s*%\\s*(${MONEY_RX})\\s*$`);
  const rxAmtFirst = new RegExp(`^\\s*(${MONEY_RX})\\s*(\\d{1,2}(?:[\\.,]\\d{1,2})?)\\s*%\\s*$`);
  const discCandidates = [];

  for (const lnRaw of tail) {
    const ln = String(lnRaw || "").trim();
    if (!ln.includes("%") || ln.includes("(") || /saldo/i.test(ln) || ln.length > 60) continue;
    let m = ln.match(rxPctFirst);
    if (m) { const pct = Number(String(m[1]).replace(",", ".")); const imp = parseMoneyArToNumber(m[2]); if (pct > 0 && pct < 80 && imp > 0) discCandidates.push({ pct, imp, why: "pct_first" }); continue; }
    m = ln.match(rxAmtFirst);
    if (m) { const imp = parseMoneyArToNumber(m[1]); const pct = Number(String(m[2]).replace(",", ".")); if (pct > 0 && pct < 80 && imp > 0) discCandidates.push({ pct, imp, why: "amt_first" }); continue; }
    const mPct = ln.match(/(\d{1,2}(?:[.,]\d{1,2})?)\s*%/);
    const mAmt = ln.match(new RegExp(`(${MONEY_RX})`));
    if (mPct && mAmt) { const pct = Number(String(mPct[1]).replace(",", ".")); const imp = parseMoneyArToNumber(mAmt[1]); if (pct > 0 && pct < 80 && imp > 0) discCandidates.push({ pct, imp, why: "fallback_mixed" }); }
  }

  if (discCandidates.length) {
    discCandidates.sort((a, b) => b.imp - a.imp);
    descuentoPct = round2(discCandidates[0].pct);
    descuentoImporte = round2(discCandidates[0].imp);
    discStrategy = discCandidates[0].why;
  }

  let subtotalBruto = moneyOnly.length ? moneyOnly[moneyOnly.length - 1].v : 0;
  let subStrategy = moneyOnly.length ? "tail_last_money_only" : "none";
  let totalFinal = 0, totStrategy = "none", verified = false;
  const findNear = (arr, target, tol = 0.06) => arr.find(x => Math.abs(x.v - target) <= tol);

  if (subtotalBruto > 0 && descuentoImporte > 0) {
    const expected = round2(subtotalBruto - descuentoImporte);
    const found = findNear(moneyOnly, expected);
    if (found) { totalFinal = found.v; totStrategy = "subtotal_minus_desc_found_in_tail"; verified = true; }
    else {
      const maxMoney = moneyOnly.reduce((m, x) => Math.max(m, x.v), 0);
      const found2 = findNear(moneyOnly, round2(maxMoney - descuentoImporte));
      if (found2) { subtotalBruto = maxMoney; subStrategy = "tail_max_money_only"; totalFinal = found2.v; totStrategy = "max_minus_desc_found_in_tail"; verified = true; }
      else { totalFinal = expected; totStrategy = "subtotal_minus_desc_calc_only"; }
    }
  } else if (moneyOnly.length) {
    const vals = moneyOnly.map(x => x.v).sort((a, b) => a - b);
    const groups = [];
    for (const v of vals) {
      const g = groups[groups.length - 1];
      if (!g || Math.abs(g.v - v) > 0.05) groups.push({ v, n: 1 }); else g.n++;
    }
    const dup = groups.filter(g => g.n >= 2).sort((a, b) => (b.n - a.n) || (b.v - a.v))[0];
    if (dup) { totalFinal = dup.v; totStrategy = "dup_money_only"; verified = true; }
    else { totalFinal = subtotalBruto; totStrategy = "no_discount_total_equals_subtotal"; verified = true; }
  }

  return { subtotalBruto, descuentoPct, descuentoImporte, totalFinal, verified, strategies: { discStrategy, subStrategy, totStrategy } };
}

function sumItemsBruto(items) {
  return round2((items || []).reduce((acc, it) => {
    const sub = Number(it.subtotalConIva || 0);
    if (sub > 0) return acc + sub;
    const c = Number(it.cantidad || 0), p = Number(it.precioConIva || 0);
    return acc + (c > 0 && p > 0 ? c * p : 0);
  }, 0));
}

function applyFactorAndReconcile(items, factor, totalTarget) {
  const out = (items || []).map((it) => {
    const qty = Number(it.cantidad || 0);
    const sub = Number(it.subtotalConIva || 0);
    const baseSub = sub > 0 ? sub : (qty * Number(it.precioConIva || 0));
    const newSub = round2(baseSub * factor);
    const newUnit = qty > 0 ? round2(newSub / qty) : round2(Number(it.precioConIva || 0) * factor);
    return { ...it, precioConIva: newUnit, subtotalConIva: newSub };
  });
  const sumNow = round2(out.reduce((a, x) => a + Number(x.subtotalConIva || 0), 0));
  const diff = round2(totalTarget - sumNow);
  if (out.length && Math.abs(diff) >= 0.01) {
    const last = out[out.length - 1];
    const qty = Number(last.cantidad || 0) || 1;
    last.subtotalConIva = round2(Number(last.subtotalConIva || 0) + diff);
    last.precioConIva = round2(last.subtotalConIva / qty);
  }
  return out;
}

function escapeRegExp(s) { return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }

function extractDomicilioLocalidadSmart(texto) {
  const lines = String(texto || "").split(/\n/).map(l => l.trim()).filter(Boolean);
  const head = lines.slice(0, 40), tail = lines.slice(-160);
  let domicilio = "", domStrategy = "none";
  for (const ln of head) {
    if (/\bCP\s*:/i.test(ln) && /[A-Za-zÁÉÍÓÚÑáéíóúñüÜ]/.test(ln) && /\d/.test(ln)) {
      if (/(Saldo|Subtotal|Total)\b/i.test(ln)) continue;
      domicilio = ln; domStrategy = "head_cp"; break;
    }
  }
  domicilio = String(domicilio || "").replace(/\bCP\s*:\s*\d+\b/gi, "").replace(/\s{2,}/g, " ").trim();
  let localidad = "", locStrategy = "none";
  for (let i = tail.length - 1; i >= 0; i--) {
    if (/^[A-Za-zÁÉÍÓÚÑáéíóúñüÜ ]{4,}$/.test(tail[i])) { localidad = tail[i].trim(); locStrategy = "tail_plain_text"; break; }
  }
  const domicilioRemito = [domicilio, localidad].filter(Boolean).join(", ").trim();
  return { domicilio, localidad, domicilioRemito, domStrategy, locStrategy };
}

async function extractCabeceraIA(texto) {
  try {
    const prompt = `Sos un especialista en lectura de cabeceras de remitos. Tu ÚNICO trabajo es extraer el CUIT y la DIRECCIÓN de este texto desordenado extraído de un PDF.
Reglas:
1. CUIT: 11 números seguidos.
2. Domicilio: Uní dirección + localidad. Ignorá basura como "CP: 0".
Devolvé ÚNICAMENTE JSON: {"cuitCliente":"...","domicilioRemito":"..."}
TEXTO: ${String(texto).slice(0, 4000)}`;
    const result = await geminiModel.generateContent(prompt);
    let txt = result.response.text().replace(/```json/gi, "").replace(/```/g, "").trim();
    const parsed = JSON.parse(txt);
    let domLimpio = String(parsed.domicilioRemito || "").replace(/CP:\s*0/gi, "").replace(/\s{2,}/g, " ").replace(/\s+,/g, ",").trim();
    return { cuitCliente: parsed.cuitCliente || "", domicilioRemito: domLimpio };
  } catch { return { cuitCliente: "", domicilioRemito: "" }; }
}

function splitQtyUnitFromMoneyToken(tokenMoney, subtotal) {
  const t = String(tokenMoney || "").replace(/\./g, "").trim();
  const m = t.match(/^(\d+),(\d{2})$/);
  if (!m) return null;
  const intPart = m[1], decPart = m[2];
  let best = null;
  const maxQtyLen = Math.min(4, intPart.length - 1);
  for (let qtyLen = 1; qtyLen <= maxQtyLen; qtyLen++) {
    const qty = Number(intPart.slice(0, qtyLen));
    const unit = parseMoneyArToNumber(intPart.slice(qtyLen) + "," + decPart);
    if (!Number.isInteger(qty) || qty <= 0 || qty > 5000 || !(unit > 0)) continue;
    const diff = Math.abs(round2(qty * unit) - subtotal);
    const tol = Math.max(2.0, subtotal * 0.003);
    if (diff <= tol && (!best || diff < best.diff)) best = { qty, unit, diff };
  }
  return best ? { qty: best.qty, unit: best.unit } : null;
}

function extractItemsSmartRegex(texto) {
  const lines = String(texto || "").split(/\n/).map(l => l.trim()).filter(Boolean);
  const items = [];
  const rxMoney = new RegExp(MONEY_RX, "g");
  for (const ln0 of lines) {
    if (!ln0.includes("%") || !/[A-Za-zÁÉÍÓÚÑáéíóúñüÜ]/.test(ln0)) continue;
    if (/(saldo\s+actual|saldo\s+anterior|subtotal\b|total\b|observaciones|firma|fecha\s+vto)/i.test(ln0)) continue;
    let desc = "";
    const idxClose = ln0.lastIndexOf(")");
    if (idxClose >= 0 && idxClose < ln0.length - 1) {
      const after = ln0.slice(idxClose + 1).trim();
      if (/[A-Za-zÁÉÍÓÚÑáéíóúñüÜ]/.test(after)) desc = after;
    }
    if (!desc) desc = ln0.replace(/\(.*?\)/g, " ").replace(rxMoney, " ").replace(/\b\d+\b/g, " ").replace(/[^\p{L}\s]/gu, " ").replace(/\s{2,}/g, " ").trim();
    if (!desc) continue;
    const ln = ln0.replace(/\(.*?\)/g, " ").replace(/\s{2,}/g, " ").trim();
    const moneyTokens = ln.match(rxMoney) || [];
    if (moneyTokens.length < 2) continue;
    const subtotal = parseMoneyArToNumber(moneyTokens[moneyTokens.length - 1]);
    if (!(subtotal > 0)) continue;
    let qty = 0, unit = 0;
    const lead = ln.match(/^(\d{1,5})\s/);
    if (lead) {
      const q = Number(lead[1]);
      if (q > 0 && q <= 5000) {
        qty = q;
        let bestU = null;
        for (let j = 0; j < moneyTokens.length - 1; j++) {
          const u = parseMoneyArToNumber(moneyTokens[j]);
          if (!(u > 0)) continue;
          const diff = Math.abs(round2(qty * u) - subtotal);
          const tol = Math.max(2.0, subtotal * 0.003);
          if (diff <= tol && (!bestU || diff < bestU.diff)) bestU = { u, diff };
        }
        if (bestU) unit = bestU.u;
      }
    }
    if (!qty || !unit) { const split = splitQtyUnitFromMoneyToken(moneyTokens[0], subtotal); if (split) { qty = split.qty; unit = split.unit; } }
    if (!qty || !unit) {
      for (let j = 0; j < moneyTokens.length - 1 && (!qty || !unit); j++) {
        const u = parseMoneyArToNumber(moneyTokens[j]);
        if (!(u > 0)) continue;
        const q = Math.round(subtotal / u);
        if (q >= 1 && q <= 5000 && Math.abs(round2(q * u) - subtotal) <= Math.max(2.0, subtotal * 0.003)) { qty = q; unit = u; }
      }
    }
    if (qty > 0 && unit > 0) {
      if (Math.abs(round2(qty * unit) - subtotal) > Math.max(2.0, subtotal * 0.003)) unit = round2(subtotal / qty);
      items.push({ cantidad: qty, descripcion: desc, precioConIva: unit, subtotalConIva: subtotal });
    }
  }
  return items;
}

async function extractItemsIA(texto) {
  const smart = extractItemsSmartRegex(texto);
  if (smart.length >= 3) { if (DEBUG) log("🧾 ITEMS smartRegex count:", smart.length); return { items: smart }; }
  try {
    const prompt = `Sos un auditor matemático. Extraé SOLO los artículos de este remito.
Reglas:
- Ignorá domicilios/cuits.
- Tomá el IMPORTE (columna final) como subtotalConIva (IVA incluido).
- Convertí números AR (19.200,00) a JSON (19200.00).
Devolvé JSON: {"items":[{"cantidad":1,"descripcion":"...","precioConIva":100.00,"subtotalConIva":100.00}]}
TEXTO: ${String(texto).slice(0, 15000)}`;
    const result = await geminiModel.generateContent(prompt);
    let txt = result.response.text().replace(/```json/gi, "").replace(/```/g, "").trim();
    const parsed = JSON.parse(txt);
    const items = Array.isArray(parsed.items) ? parsed.items : [];
    const clean = items.map((it) => ({
      cantidad: Number(it.cantidad || 0), descripcion: String(it.descripcion || "").trim(),
      precioConIva: Number(it.precioConIva || 0), subtotalConIva: Number(it.subtotalConIva || 0),
    })).filter((x) => x.descripcion && (x.subtotalConIva > 0 || x.precioConIva > 0));
    for (const x of clean) {
      if (x.subtotalConIva > 0 && x.precioConIva > 0) {
        const qInfer = Math.round(x.subtotalConIva / x.precioConIva);
        const tol = Math.max(2.0, x.subtotalConIva * 0.003);
        if ((!Number.isInteger(x.cantidad) || x.cantidad <= 0 || x.cantidad > 5000) && qInfer >= 1 && qInfer <= 5000 && Math.abs(round2(qInfer * x.precioConIva) - round2(x.subtotalConIva)) <= tol) x.cantidad = qInfer;
      }
      if (x.subtotalConIva > 0 && (!x.precioConIva || x.precioConIva <= 0) && x.cantidad > 0) x.precioConIva = round2(x.subtotalConIva / x.cantidad);
      if (x.precioConIva > 0 && (!x.subtotalConIva || x.subtotalConIva <= 0) && x.cantidad > 0) x.subtotalConIva = round2(x.precioConIva * x.cantidad);
    }
    const final = clean.filter(x => x.cantidad > 0 && x.precioConIva > 0 && x.subtotalConIva > 0);
    if (DEBUG) log("🧾 ITEMS gemini count:", final.length);
    return { items: final };
  } catch { return smart.length ? { items: smart } : { items: [] }; }
}

function extractDataRegex(texto) {
  const cuit = pickCuitCliente(texto);
  const textLimpio = String(texto || "").replace(/[\r\n",]/g, " ").replace(/\s{2,}/g, " ");
  let dom = "", loc = "";
  const matchDom = textLimpio.match(/Domicilio:\s*(.*?)\s*(?:CP:|I\.V\.A|Localidad|C\.U\.I\.T)/i);
  if (matchDom && matchDom[1]) dom = matchDom[1].trim();
  const matchLoc = textLimpio.match(/Localidad:\s*(.*?)\s*(?:C\.U\.I\.T|Remito|Condición)/i);
  if (matchLoc && matchLoc[1]) loc = matchLoc[1].trim();
  return { cuit, domicilioRemito: [dom, loc].filter(Boolean).join(", ").replace(/CP:\s*0/gi, "").trim(), items: [] };
}

async function extractData(texto) {
  let cuit = "", domicilioRemito = "", items = [];
  const smart = extractDomicilioLocalidadSmart(texto);
  if (geminiModel) {
    log("🚀 Iniciando Motor IA Bicefálico...");
    const [cabecera, detalle] = await Promise.all([extractCabeceraIA(texto), extractItemsIA(texto)]);
    cuit = String(cabecera.cuitCliente || "");
    domicilioRemito = String(cabecera.domicilioRemito || "");
    items = detalle.items;
    if (DEBUG) { log("🤖 IA cabecera:", cabecera); log("🤖 IA items count:", Array.isArray(items) ? items.length : 0); }
  }
  const fb = extractDataRegex(texto);
  if (!cuit || onlyDigits(cuit).length !== 11) cuit = fb.cuit || pickCuitCliente(texto);
  if (!domicilioRemito || domicilioRemito.length < 5) domicilioRemito = fb.domicilioRemito || smart.domicilioRemito || "";
  const loc = String(smart.localidad || "").trim();
  const smartFull = String(smart.domicilioRemito || "").trim();
  const hasLocComma = loc ? new RegExp(`,\\s*${escapeRegExp(loc)}\\b`, "i").test(domicilioRemito) : false;
  if (smartFull && loc && (!domicilioRemito || !hasLocComma)) domicilioRemito = smartFull;
  domicilioRemito = String(domicilioRemito || "").replace(/CP:\s*0/gi, "").replace(/\s+,/g, ",").replace(/\s{2,}/g, " ").trim();
  cuit = onlyDigits(cuit);
  if (cuit.length !== 11) cuit = pickCuitCliente(texto);
  return { cuit, domicilioRemito, items };
}

async function parseMultiplesRemitos(files) {
  let textoCombinado = "";
  const docsPricing = [], docCuitts = new Set();
  for (const file of files) {
    const dataBuffer = fs.readFileSync(file.path);
    const parsed = await pdfParse(dataBuffer);
    const textoDoc = normalizePdfText(parsed?.text || "");
    if (DEBUG) { log("📄 DOC:", file.originalname || file.filename, "| chars:", textoDoc.length); }
    const cDoc = onlyDigits(pickCuitCliente(textoDoc));
    if (cDoc && cDoc.length === 11 && String(cDoc) !== String(CUIT_DISTRIBUIDORA || "")) docCuitts.add(cDoc);
    docsPricing.push(extractDocPricingSummary(textoDoc));
    textoCombinado += "\n\n--- DOC ---\n\n" + textoDoc;
  }
  if (docCuitts.size > 1) throw new Error(`Se detectaron remitos de distintos CUIT (${[...docCuitts].join(", ")}). Subí remitos del mismo cliente.`);
  const extracted = await extractData(textoCombinado);
  const itemsRaw = extracted.items || [];
  const subtotalItems = sumItemsBruto(itemsRaw);
  const subtotalDocs = round2(docsPricing.reduce((a, x) => a + Number(x.subtotalBruto || 0), 0));
  const totalDocs = round2(docsPricing.reduce((a, x) => a + Number(x.totalFinal || 0), 0));
  const subtotalBruto = (subtotalDocs > 0 && totalDocs > 0) ? subtotalDocs : subtotalItems;
  const totalFinal = (totalDocs > 0) ? totalDocs : subtotalItems;
  if (subtotalDocs > 0 && subtotalItems > 0) {
    const delta = Math.abs(subtotalItems - subtotalDocs);
    if (delta > Math.max(1.0, subtotalDocs * 0.002)) throw new Error(`No coincide subtotal de ítems vs subtotal del remito. Ítems=$${formatMoneyAR(subtotalItems)} | Remito=$${formatMoneyAR(subtotalDocs)}.`);
  }
  const factor = (subtotalItems > 0 && totalFinal > 0 && totalFinal < subtotalItems - 0.01) ? (totalFinal / subtotalItems) : 1;
  let items = itemsRaw;
  if (factor > 0 && factor < 1 && totalFinal > 0) items = applyFactorAndReconcile(itemsRaw, factor, totalFinal);
  const descuentoImporte = round2(subtotalBruto - totalFinal);
  let descuentoPct = 0;
  const pcts = docsPricing.map(x => Number(x.descuentoPct || 0)).filter(x => x > 0);
  if (pcts.length && pcts.every(p => Math.abs(p - pcts[0]) <= 0.05)) descuentoPct = round2(pcts[0]);
  else if (subtotalBruto > 0 && descuentoImporte > 0) descuentoPct = round2((descuentoImporte / subtotalBruto) * 100);
  if (DEBUG) log("🧾 PRICING:", { subtotalItems, subtotalDocs, totalDocs, factor, descuentoPct, descuentoImporte });
  return { texto: textoCombinado, cuit: extracted.cuit, domicilioRemito: extracted.domicilioRemito, items, subtotalBruto, descuentoPct, descuentoImporte, totalFinal };
}

// ============================
// LOGO
// ============================
function findLogoPath() {
  const candidates = ["logo.jpeg","logo.jpg","logo.png","Logo.jpeg","Logo.jpg","Logo.png"].map(f => path.join(process.cwd(), f));
  for (const p of candidates) { try { if (fs.existsSync(p)) return p; } catch {} }
  return "";
}
function logoPathToDataUrl(p) {
  try {
    if (!p) return "";
    const ext = path.extname(p).toLowerCase();
    const mime = ext === ".png" ? "image/png" : "image/jpeg";
    return `data:${mime};base64,${fs.readFileSync(p).toString("base64")}`;
  } catch { return ""; }
}
const LOGO_PATH = findLogoPath();
const LOGO_DATA_URL = logoPathToDataUrl(LOGO_PATH);

// ============================
// HTML FACTURA
// ============================
function buildFacturaHtml({ receptor, fechaISO, pv, nro, items, neto, iva, total, cae, caeVtoISO, condicionVenta, qrDataUrl, isPreview = false, notaFactura = "", subtotalBruto = 0, descuentoPct = 0, descuentoImporte = 0, totalFinal = 0 }) {
  const pvStr = pad(pv, 5), nroStr = pad(nro, 8);
  const fechaAR = fechaISO.split("-").reverse().join("/");
  const caeVtoAR = caeVtoISO ? String(caeVtoISO).split("-").reverse().join("/") : "VISTA PREVIA";
  const caeText = cae || "VISTA PREVIA";
  const rows = items.map((it, i) => `
    <tr style="background-color: ${i % 2 === 0 ? "#ffffff" : "#f8fafc"}; page-break-inside: avoid;">
      <td style="padding:10px; border-bottom:1px solid #e2e8f0; color:#334155; font-size:12px;">${safeText(it.descripcion)}</td>
      <td class="r" style="padding:10px; border-bottom:1px solid #e2e8f0; color:#334155; font-size:12px;">${it.cantidad}</td>
      <td class="r" style="padding:10px; border-bottom:1px solid #e2e8f0; color:#334155; font-size:12px;">$ ${formatMoneyAR(it.precioNeto)}</td>
      <td class="r" style="padding:10px; border-bottom:1px solid #e2e8f0; color:#334155; font-size:12px;">$ ${formatMoneyAR(it.subtotalNeto)}</td>
      <td class="r" style="padding:10px; border-bottom:1px solid #e2e8f0; color:#334155; font-size:12px;">21%</td>
      <td class="r" style="padding:10px; border-bottom:1px solid #e2e8f0; font-weight:bold; color:#0f172a; font-size:12px;">$ ${formatMoneyAR(it.subtotalConIva)}</td>
    </tr>`).join("");
  const pageStyle = isPreview ? `width: 100%; padding: 10px; zoom: 0.8;` : `width: 820px; margin: 0 auto; padding: 30px;`;
  const domRemito = String(receptor?.domicilioRemito || "").trim();
  const domAfip = String(receptor?.domicilioAfip || "").trim();
  const normAddr = (s) => String(s || "").toLowerCase().replace(/\s+/g, " ").replace(/[.,;]+/g, "").trim();
  const domMain = domRemito || domAfip || "Domicilio no informado";
  const showBoth = domRemito && domAfip && normAddr(domRemito) !== normAddr(domAfip);
  const domicilioHtml = showBoth
    ? `<div class="muted">Domicilio (Entrega/Remito): <strong>${safeText(domRemito)}</strong></div><div class="muted">Domicilio Fiscal (AFIP): <strong>${safeText(domAfip)}</strong></div>`
    : `<div class="muted">Domicilio: <strong>${safeText(domMain)}</strong></div>`;
  const showDesc = Number(descuentoImporte || 0) > 0 && Number(subtotalBruto || 0) > 0;

  return `<!doctype html><html lang="es"><head><meta charset="utf-8"/><title>Factura A ${pvStr}-${nroStr}</title>
<style>* { box-sizing: border-box; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; } body { margin: 0; color: #0f172a; background: #ffffff; } .page { ${pageStyle} } .header-box { display: flex; justify-content: space-between; border: 2px solid #1e293b; border-radius: 8px; position: relative; margin-bottom: 20px; } .header-left { flex: 1; padding: 20px; border-right: 1px solid #e2e8f0; } .header-right { flex: 1; padding: 20px; position: relative; text-align: right; } .letter-box { position: absolute; top: -2px; left: 50%; transform: translateX(-50%); width: 50px; height: 50px; border: 2px solid #1e293b; border-bottom-left-radius: 8px; border-bottom-right-radius: 8px; border-top: none; background: #fff; display: flex; align-items: center; justify-content: center; } .letter { font-size: 32px; font-weight: 900; color: #1e293b; margin: 0; } .title { font-weight: 900; font-size: 26px; color: #1e293b; margin-bottom: 5px; text-transform: uppercase; letter-spacing: 0.5px; } .muted { color: #475569; font-size: 11px; margin: 3px 0; } .muted strong { color: #1e293b; } .client-box { border: 1px solid #cbd5e1; border-radius: 8px; padding: 15px; margin-bottom: 20px; background: #f8fafc; page-break-inside: avoid; position: relative; } .nota-fraccion { position: absolute; top: 10px; right: 15px; background: #fef08a; color: #a16207; font-size: 10px; font-weight: bold; padding: 4px 8px; border-radius: 4px; white-space: pre-line; text-align: right; } table { width: 100%; border-collapse: collapse; margin-top: 10px; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; } th { background: #1e293b; color: #ffffff; padding: 12px 10px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; text-align: left; } th.r { text-align: right; } .r { text-align: right; } .footer-grid { display: flex; gap: 20px; margin-top: 20px; page-break-inside: avoid; } .totals-box { flex: 1; border: 1px solid #cbd5e1; border-radius: 8px; padding: 15px; } .total-row { display: flex; justify-content: space-between; font-size: 12px; margin-bottom: 8px; color: #475569; } .total-row.final { font-size: 16px; font-weight: 900; color: #0f172a; margin-top: 10px; padding-top: 10px; border-top: 2px solid #e2e8f0; } .arca-box { flex: 1; display: flex; align-items: center; justify-content: space-between; border: 1px solid #cbd5e1; border-radius: 8px; padding: 15px; background: #f8fafc; } .arca-info { font-size: 11px; color: #334155; line-height: 1.6; } .qr { width: 110px; height: 110px; } .leyenda { text-align: center; font-size: 10px; font-weight: bold; color: #64748b; margin-top: 20px; border-top: 1px dashed #cbd5e1; padding-top: 10px; page-break-inside: avoid; }</style>
</head><body><div class="page">
  ${isPreview ? `<div style="background:#fef08a; padding:10px; text-align:center; font-weight:bold; color:#a16207; margin-bottom:15px; border-radius:8px;">MODO VISTA PREVIA (AÚN NO AUTORIZADO POR ARCA)</div>` : ""}
  <div class="header-box">
    <div class="letter-box"><div class="letter">A</div></div>
    <div class="header-left">
      ${LOGO_DATA_URL ? `<div style="display:flex; align-items:center; gap:14px; margin-bottom:10px;"><img src="${LOGO_DATA_URL}" alt="Logo" style="height:64px; width:auto; object-fit:contain; display:block;" /><div style="border-left:1px solid #e2e8f0; height:42px;"></div></div>` : ""}
      <div class="title" style="text-align: left; font-size: 22px;">${safeText(EMISOR.nombreVisible)}</div>
      <div class="muted">Razón Social: <strong>${safeText(EMISOR.nombreVisible)}</strong></div>
      <div class="muted">Domicilio Comercial: <strong>${safeText(EMISOR.domicilio)}</strong></div>
      <div class="muted">Condición frente al IVA: <strong>${safeText(EMISOR.condicionIVA)}</strong></div>
    </div>
    <div class="header-right">
      <div class="title">FACTURA</div><div style="height: 10px;"></div>
      <div class="muted">Punto de Venta: <strong>${pvStr}</strong> &nbsp;&nbsp; Comp. Nro: <strong>${nroStr}</strong></div>
      <div class="muted">Fecha de Emisión: <strong>${fechaAR}</strong></div>
      <div class="muted">CUIT: <strong>${safeText(CUIT_DISTRIBUIDORA)}</strong></div>
      <div class="muted">Ingresos Brutos: <strong>${safeText(CUIT_DISTRIBUIDORA)}</strong></div>
      <div class="muted">Inicio de Actividades: <strong>01/01/2020</strong></div>
    </div>
  </div>
  <div class="client-box">
    ${notaFactura ? `<div class="nota-fraccion">${safeText(notaFactura)}</div>` : ""}
    <table style="border: none; margin: 0;"><tr style="background: transparent;">
      <td style="padding: 0; width: 60%; vertical-align: top;">
        <div class="muted">CUIT: <strong>${safeText(receptor.cuit)}</strong></div>
        <div class="muted">Condición frente al IVA: <strong>${safeText(receptor.condicionIVA)}</strong></div>
        <div class="muted">Condición de Venta: <strong>${safeText(condicionVenta)}</strong></div>
      </td>
      <td style="padding: 0; width: 40%; vertical-align: top;">
        <div class="muted">Apellido y Nombre / Razón Social: <strong>${safeText(receptor.nombre)}</strong></div>
        ${domicilioHtml}
      </td>
    </tr></table>
  </div>
  <table>
    <thead><tr>
      <th>Producto / Servicio</th><th class="r" style="width:50px;">Cant</th>
      <th class="r" style="width:90px;">Precio Unit.</th><th class="r" style="width:90px;">Subtotal Neto</th>
      <th class="r" style="width:50px;">Alic. IVA</th><th class="r" style="width:100px;">Subtotal c/IVA</th>
    </tr></thead>
    <tbody>${rows || `<tr><td colspan="6" style="padding:20px; text-align:center;">Sin ítems detallados</td></tr>`}</tbody>
  </table>
  <div class="footer-grid">
    <div class="arca-box">
      ${qrDataUrl ? `<img class="qr" src="${qrDataUrl}" alt="QR Code ARCA" />` : '<div class="qr" style="border:1px dashed #ccc; display:flex; align-items:center; justify-content:center; color:#999; font-size:10px;">(QR ARCA)</div>'}
      <div class="arca-info" style="text-align: right;">
        <img src="https://www.afip.gob.ar/images/afip.png" alt="ARCA" style="height:25px; margin-bottom:10px; opacity:0.8;" onerror="this.style.display='none'"><br>
        Comprobante Autorizado por ARCA<br><br>
        <strong>CAE Nro:</strong> ${safeText(caeText)}<br>
        <strong>Fecha Vto. CAE:</strong> ${safeText(caeVtoAR)}
      </div>
    </div>
    <div class="totals-box">
      ${showDesc ? `
        <div class="total-row"><span>Subtotal (Remito) c/IVA:</span><strong>$ ${formatMoneyAR(subtotalBruto)}</strong></div>
        <div class="total-row"><span>Descuento (${formatMoneyAR(descuentoPct)}%):</span><strong>-$ ${formatMoneyAR(descuentoImporte)}</strong></div>
        <div class="total-row"><span>Total Remito c/desc:</span><strong>$ ${formatMoneyAR(totalFinal || total)}</strong></div>
        <div style="height:10px;"></div>` : ""}
      <div class="total-row"><span>Importe Neto Gravado:</span><strong>$ ${formatMoneyAR(neto)}</strong></div>
      <div class="total-row"><span>IVA 21%:</span><strong>$ ${formatMoneyAR(iva)}</strong></div>
      <div class="total-row"><span>Importe Otros Tributos:</span><strong>$ 0,00</strong></div>
      <div class="total-row final"><span>IMPORTE TOTAL:</span><span>$ ${formatMoneyAR(total)}</span></div>
    </div>
  </div>
  <div class="leyenda">${safeText(EMISOR.leyenda)}</div>
</div></body></html>`;
}

// ============================
// ROUTES
// ============================
app.get("/health", (req, res) => res.json({ ok: true, version: APP_VERSION, iaIntegrada: !!geminiModel, supabase: !!supabase }));

app.get("/tipos", async (req, res) => {
  try { res.json({ pv: await getPtoVentaSeguro(), tipos: [{ id: CBTE_TIPO_REAL, name: "Factura (WS)", habilitado: true }] }); }
  catch (e) { res.status(500).json({ message: e.message }); }
});

app.post("/leer-remito", upload.array("remito", 10), async (req, res) => {
  const started = Date.now();
  try {
    const files = req.files || (req.file ? [req.file] : []);
    if (!files.length) return res.status(400).json({ message: "No llegó archivo PDF" });
    const parsed = await parseMultiplesRemitos(files);
    files.forEach(f => cleanupTempFile(f.path));
    res.json({ cuit: parsed.cuit, domicilioRemito: parsed.domicilioRemito, items: parsed.items, total: parsed.totalFinal, subtotalBruto: parsed.subtotalBruto, descuentoPct: parsed.descuentoPct, descuentoImporte: parsed.descuentoImporte, ms: Date.now() - started, version: APP_VERSION });
  } catch (err) {
    (req.files || []).forEach(f => cleanupTempFile(f.path));
    res.status(500).json({ message: "Error", detail: String(err?.message || err) });
  }
});

app.post("/debug/preview", async (req, res) => {
  try {
    const pv = await getPtoVentaSeguro();
    const previewParteReq = (req.body.previewParte ?? 1);
    const cuitCliente = onlyDigits(req.body.cuitCliente);
    const domicilioRemitoIn = String(req.body.domicilioRemito || "").trim();
    const condicionVenta = String(req.body.condicionVenta || EMISOR.condicionVentaDefault);
    const subtotalBrutoIn = Number(req.body.subtotalBruto || 0);
    const descuentoPctIn = Number(req.body.descuentoPct || 0);
    const descuentoImporteIn = Number(req.body.descuentoImporte || 0);
    const totalFinalIn = Number(req.body.total || req.body.totalFinal || 0);
    const itemsIn = Array.isArray(req.body.items) ? req.body.items : [];

    let allItems = itemsIn.map((x) => {
      const cantidad = Number(x.cantidad || 0), descripcion = String(x.descripcion || "").trim();
      const precioConIva = Number(x.precioConIva || 0), subtotalConIva = Number(x.subtotalConIva || 0);
      if (!cantidad || !descripcion) return null;
      const sub = subtotalConIva > 0 ? round2(subtotalConIva) : round2(cantidad * precioConIva);
      const unit = precioConIva > 0 ? round2(precioConIva) : (cantidad > 0 ? round2(sub / cantidad) : 0);
      if (!sub || !unit) return null;
      return { cantidad, descripcion, precioConIva: unit, subtotalConIva: sub };
    }).filter(Boolean);

    if (!allItems.length && (!cuitCliente || cuitCliente.length < 11)) return res.status(204).end();

    const subtotalPre = sumItemsBruto(allItems);
    let factor = 1;
    if (totalFinalIn > 0 && subtotalPre > 0) factor = totalFinalIn / subtotalPre;
    else if (descuentoPctIn > 0) factor = 1 - (descuentoPctIn / 100);
    else if (descuentoImporteIn > 0 && subtotalPre > 0) factor = (subtotalPre - descuentoImporteIn) / subtotalPre;
    if (factor > 0 && factor < 1 && totalFinalIn > 0) allItems = applyFactorAndReconcile(allItems, factor, totalFinalIn);

    const chunks = [];
    for (let i = 0; i < allItems.length; i += ITEMS_POR_FACTURA) chunks.push(allItems.slice(i, i + ITEMS_POR_FACTURA));
    const partes = chunks.map(ch => round2(ch.reduce((a, it) => a + Number(it.subtotalConIva || 0), 0)));
    const totalRemitoGlobal = totalFinalIn > 0 ? round2(totalFinalIn) : round2(partes.reduce((a, x) => a + x, 0));

    // ── MODO ALL: todas las partes en iframes ──
    if (String(previewParteReq).toUpperCase() === "ALL") {
      const frames = [];
      for (let p = 1; p <= chunks.length; p++) {
        frames.push(`
          <div style="background:#0f172a;color:#fff;padding:10px 12px;font-family:Arial;font-weight:900;border-radius:12px;margin:12px 0 8px 0;">
            Parte ${p} de ${chunks.length} — Total remito: $ ${safeText(formatMoneyAR(totalRemitoGlobal))}
          </div>
          <iframe srcdoc="__SRC_${p}__" style="width:100%;height:900px;border:1px solid #e2e8f0;border-radius:12px;background:#fff;"></iframe>
        `);
      }

      const htmlsPorParte = [];
      for (let p = 1; p <= chunks.length; p++) {
        const itemsParte = chunks[p - 1] || [];
        const totalParte = round2(itemsParte.reduce((a, it) => a + Number(it.subtotalConIva || 0), 0));
        const factorUsado = (factor > 0 && factor < 1) ? factor : 1;
        let subBrutoParte = 0, descParte = 0, pctParte = 0;
        if (factorUsado < 1 && totalParte > 0) {
          subBrutoParte = round2(totalParte / factorUsado);
          descParte = round2(subBrutoParte - totalParte);
          pctParte = descuentoPctIn > 0 ? round2(descuentoPctIn) : 0;
        }
        let impTotal = 0, impNeto = 0, impIVA = 0;
        const itemsCalc = itemsParte.map((it) => {
          const subConIva = Number(it.subtotalConIva || 0);
          const subNeto = round2(subConIva / 1.21), subIva = round2(subConIva - subNeto);
          impTotal += subConIva; impNeto += subNeto; impIVA += subIva;
          return { ...it, subtotalNeto: subNeto, precioNeto: it.cantidad > 0 ? round2(subNeto / it.cantidad) : 0 };
        });
        impTotal = round2(impTotal); impNeto = round2(impNeto); impIVA = round2(impIVA);
        let rec = { nombre: "Completá el CUIT para ver al cliente...", domicilioAfip: "", condicionIVA: "-" };
        if (cuitCliente.length === 11) rec = await getReceptorDesdePadron(cuitCliente);
        let nota = "";
        if (chunks.length > 1) {
          nota = `FACTURA PARTE ${p} DE ${chunks.length}\nTotal remito: $ ${formatMoneyAR(totalRemitoGlobal)}\n`;
          partes.forEach((t, idx) => (nota += `Parte ${idx + 1}: $ ${formatMoneyAR(t)}\n`));
          nota = nota.trim();
        }
        const htmlParte = buildFacturaHtml({
          receptor: { cuit: cuitCliente || "00000000000", nombre: rec.nombre, condicionIVA: rec.condicionIVA, domicilioAfip: rec.domicilioAfip, domicilioRemito: domicilioRemitoIn },
          fechaISO: todayISO(), pv, nro: 0, items: itemsCalc, neto: impNeto, iva: impIVA, total: impTotal,
          cae: null, caeVtoISO: null, condicionVenta, qrDataUrl: null, isPreview: true, notaFactura: nota,
          subtotalBruto: subBrutoParte, descuentoPct: pctParte, descuentoImporte: descParte, totalFinal: totalParte
        });
        htmlsPorParte.push(htmlParte.replace(/&/g, "&amp;").replace(/"/g, "&quot;"));
      }

      let container = `<!doctype html><html><head><meta charset="utf-8"><title>Vista previa (Todas)</title></head>
      <body style="margin:0;padding:14px;background:#f8fafc;">
      ${frames.join("\n")}
      </body></html>`;
      for (let p = 1; p <= chunks.length; p++) {
        container = container.replace(`__SRC_${p}__`, htmlsPorParte[p - 1]);
      }
      return res.send(container);
    }

    // ── MODO PARTE ÚNICA ──
    const idxParte = Math.max(0, Number(previewParteReq || 1) - 1);
    const itemsParte = chunks[idxParte] || [];
    const totalParte = round2(itemsParte.reduce((a, it) => a + Number(it.subtotalConIva || 0), 0));
    const factorUsado = (factor > 0 && factor < 1) ? factor : 1;
    let subBrutoParte = 0, descParte = 0, pctParte = 0;
    if (factorUsado < 1 && totalParte > 0) {
      subBrutoParte = round2(totalParte / factorUsado);
      descParte = round2(subBrutoParte - totalParte);
      pctParte = descuentoPctIn > 0 ? round2(descuentoPctIn) : 0;
    }

    let impTotal = 0, impNeto = 0, impIVA = 0;
    const itemsCalc = itemsParte.map((it) => {
      const subConIva = Number(it.subtotalConIva || 0);
      const subNeto = round2(subConIva / 1.21), subIva = round2(subConIva - subNeto);
      impTotal += subConIva; impNeto += subNeto; impIVA += subIva;
      return { ...it, subtotalNeto: subNeto, precioNeto: it.cantidad > 0 ? round2(subNeto / it.cantidad) : 0 };
    });
    impTotal = round2(impTotal); impNeto = round2(impNeto); impIVA = round2(impIVA);

    let rec = { nombre: "Completá el CUIT para ver al cliente...", domicilioAfip: "", condicionIVA: "-" };
    if (cuitCliente.length === 11) rec = await getReceptorDesdePadron(cuitCliente);

    let nota = "";
    if (chunks.length > 1) {
      nota = `FACTURA PARTE ${idxParte + 1} DE ${chunks.length}\nTotal remito: $ ${formatMoneyAR(totalRemitoGlobal)}\n`;
      partes.forEach((t, i) => (nota += `Parte ${i + 1}: $ ${formatMoneyAR(t)}\n`));
      nota = nota.trim();
    }

    res.send(buildFacturaHtml({
      receptor: { cuit: cuitCliente || "00000000000", nombre: rec.nombre, condicionIVA: rec.condicionIVA, domicilioAfip: rec.domicilioAfip, domicilioRemito: domicilioRemitoIn },
      fechaISO: todayISO(), pv, nro: 0, items: itemsCalc, neto: impNeto, iva: impIVA, total: impTotal,
      cae: null, caeVtoISO: null, condicionVenta, qrDataUrl: null, isPreview: true, notaFactura: nota,
      subtotalBruto: subBrutoParte, descuentoPct: pctParte, descuentoImporte: descParte, totalFinal: totalParte
    }));
  } catch (err) { res.status(500).send("Error generando vista previa: " + err.message); }
});

async function enviarEmailFactura({ mailParts, mailAttachments, rec, cuitCliente, domicilioRemitoIn, condicionVenta, subtotalBrutoIn, descuentoPctIn, descuentoImporteIn, totalFinalIn, emailAEnviar }) {
  console.log("📨 [Email] Inicio envío vía Resend/Gmail...");

  try {
    const domRemitoMail = String(domicilioRemitoIn || "").trim();
    const domAfipMail = String(rec.domicilioAfip || "").trim();
    const normAddr = s => String(s || "").toLowerCase().replace(/\s+/g, " ").replace(/[.,;]+/g, "").trim();

    const showBothDom = domRemitoMail && domAfipMail && normAddr(domRemitoMail) !== normAddr(domAfipMail);
    
    // FORMATO DOMICILIO
    let domicilioHtml = "";
    if (showBothDom) {
      domicilioHtml = `
        <div style="margin-bottom: 5px;"><strong>Domicilio de Entrega/Remito:</strong> ${safeText(domRemitoMail)}</div>
        <div><strong>Domicilio Fiscal (ARCA):</strong> ${safeText(domAfipMail)}</div>`;
    } else {
      domicilioHtml = `<div><strong>Domicilio:</strong> ${safeText(domRemitoMail || domAfipMail || "Domicilio no informado")}</div>`;
    }

    const showDescGlobal = descuentoImporteIn > 0 && subtotalBrutoIn > 0 && totalFinalIn > 0;
    
    // FILAS DE LA TABLA DE COMPROBANTES
    const partsRows = mailParts.map(p => `
      <tr style="border-bottom: 1px solid #e2e8f0;">
        <td style="padding: 12px; color: #475569;">Parte ${p.parte}/${p.totalPartes}</td>
        <td style="padding: 12px; font-weight: bold; color: #0f172a;">${p.comprobante}</td>
        <td style="padding: 12px; color: #475569;">${safeText(p.cae)}</td>
        <td style="padding: 12px; text-align: right; font-weight: 900; color: #0f172a;">$ ${formatMoneyAR(p.total)}</td>
      </tr>
    `).join("");
    
    const totalMail = round2(mailParts.reduce((a, x) => a + x.total, 0));

    const subject = mailParts.length > 1
      ? `Facturas emitidas (${mailParts.length} partes) - ${EMISOR.nombreVisible}`
      : `Factura ${mailParts[0].comprobante} - ${EMISOR.nombreVisible}`;

    // NUEVO DISEÑO HTML PROFESIONAL Y LIMPIO (Igual a la NC)
    const mailHtml = `
      <div style="font-family: 'Segoe UI', Helvetica, Arial, sans-serif; color: #1a1a1a; max-width: 600px; margin: 20px auto; border: 1px solid #e2e8f0; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);">
        
        <div style="background-color: #0f172a; padding: 30px; text-align: center;">
          <h1 style="color: #ffffff; margin: 0; font-size: 24px; letter-spacing: 1px; font-weight: 900;">${safeText(EMISOR.nombreVisible)}</h1>
          <p style="color: #94a3b8; margin: 5px 0 0 0; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;">Comprobante Electrónico</p>
        </div>
        
        <div style="padding: 30px;">
          <p style="font-size: 15px; line-height: 1.6; color: #334155; margin-top: 0;">Estimado/a <strong>${safeText(rec.nombre)}</strong>,</p>
          <p style="font-size: 15px; line-height: 1.6; color: #334155;">
            Adjuntamos a este correo el/los comprobante(s) oficial(es) correspondiente(s) a su última operación.
          </p>
          
          <div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 20px; margin: 25px 0; border-left: 4px solid #3b82f6;">
            <h3 style="margin: 0 0 10px 0; font-size: 14px; color: #0f172a; text-transform: uppercase;">Datos de Facturación</h3>
            <div style="font-size: 13px; color: #475569; line-height: 1.8;">
              <div><strong>CUIT:</strong> ${safeText(cuitCliente)}</div>
              ${domicilioHtml}
              <div style="margin-top: 5px;"><strong>Condición de Venta:</strong> ${safeText(condicionVenta)}</div>
              
              ${showDescGlobal ? `
                <div style="margin-top: 15px; padding-top: 10px; border-top: 1px dashed #cbd5e1;">
                  <div><strong>Subtotal Bruto:</strong> $ ${formatMoneyAR(subtotalBrutoIn)}</div>
                  <div><strong>Descuento (${formatMoneyAR(descuentoPctIn)}%):</strong> -$ ${formatMoneyAR(descuentoImporteIn)}</div>
                  <div style="font-size: 15px; color: #0f172a; font-weight: bold; margin-top: 5px;">
                    <strong>Total a pagar:</strong> $ ${formatMoneyAR(totalFinalIn)}
                  </div>
                </div>
              ` : ""}
            </div>
          </div>
          
          <div style="margin-top: 25px; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden;">
            <div style="background-color: #1e293b; color: #ffffff; padding: 12px 15px; font-size: 13px; font-weight: bold; text-transform: uppercase; letter-spacing: 0.5px;">
              Detalle de Comprobantes
            </div>
            <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
              <thead>
                <tr style="background-color: #f1f5f9; text-transform: uppercase; font-size: 11px; color: #64748b;">
                  <th style="padding: 10px 12px; text-align: left;">Parte</th>
                  <th style="padding: 10px 12px; text-align: left;">Comprobante</th>
                  <th style="padding: 10px 12px; text-align: left;">CAE</th>
                  <th style="padding: 10px 12px; text-align: right;">Total</th>
                </tr>
              </thead>
              <tbody>
                ${partsRows}
              </tbody>
              <tfoot>
                <tr style="background-color: #f8fafc;">
                  <td colspan="3" style="padding: 15px 12px; text-align: right; font-weight: 900; color: #334155; text-transform: uppercase; font-size: 12px;">Total Facturado</td>
                  <td style="padding: 15px 12px; text-align: right; font-weight: 900; color: #3b82f6; font-size: 16px;">$ ${formatMoneyAR(totalMail)}</td>
                </tr>
              </tfoot>
            </table>
          </div>
          
          <p style="font-size: 14px; color: #64748b; line-height: 1.6; background: #f1f5f9; padding: 12px; border-radius: 6px; text-align: center; margin-top: 25px;">
            📎 <strong>Importante:</strong> Adjunto a este correo encontrará el/los archivo(s) PDF oficial(es) autorizado(s) por ARCA/AFIP.
          </p>
          
          <p style="font-size: 15px; line-height: 1.6; color: #334155; margin-top: 30px;">
            Atentamente,<br>
            <strong>Administración Mercado Limpio</strong>
          </p>
        </div>
        
        <div style="background-color: #f1f5f9; padding: 20px; text-align: center; font-size: 11px; color: #64748b; border-top: 1px solid #e2e8f0;">
          <p style="margin: 0;">Este es un envío automático desde el sistema de facturación.</p>
          <p style="margin: 5px 0 0 0;">Buenos Aires, Argentina</p>
        </div>
      </div>`;

    // --- LÓGICA DE ENVÍO ---
    if (!resendClient) throw new Error("Resend no configurado — revisar RESEND_API_KEY en variables de entorno");
    console.log("🚀 Enviando por Resend API...");
    const { data: resendData, error: resendError } = await resendClient.emails.send({
      from: `"${EMISOR.nombreVisible}" <ventas@mercadolimpio.ar>`,
      to: emailAEnviar,
      reply_to: GMAIL_USER,
      subject: subject,
      html: mailHtml,
      attachments: mailAttachments.map(at => ({
        filename: at.filename,
        content: at.content
      }))
    });

    if (resendError) throw new Error(`Resend error: ${resendError.message || JSON.stringify(resendError)}`);
    console.log(`✅ [Email] Enviado a ${emailAEnviar} | id=${resendData?.id}`);

    for (const p of mailParts) {
      await actualizarEstadoEmail(p.comprobante, "sent", "", emailAEnviar);
    }
    return { sent: true, error: "" };

  } catch (err) {
    console.error("❌ [Email] Error crítico:", err.message);
    for (const p of mailParts) {
      await actualizarEstadoEmail(p.comprobante, "failed", err.message, emailAEnviar);
    }
    return { sent: false, error: err.message };
  }
}

// ================================================================
// ✅ RUTA /facturar — EMAIL SINCRÓNICO antes de responder
// La factura se autoriza en AFIP, se envía el email, y LUEGO
// se responde al frontend. Esto evita que Render mate el proceso.
// ================================================================
app.post("/facturar", async (req, res) => {
  try {
    const pv = await getPtoVentaSeguro();
    const cuitCliente = onlyDigits(req.body.cuitCliente);
    if (!cuitCliente || cuitCliente.length !== 11) {
      return res.status(400).json({ message: "CUIT inválido" });
    }

    const domicilioRemitoIn = String(req.body.domicilioRemito || "").trim();
    const condicionVenta = String(req.body.condicionVenta || EMISOR.condicionVentaDefault);
    const emailAEnviar = String(req.body.emailCliente || "").trim() || DEFAULT_EMAIL;
    const subtotalBrutoIn = Number(req.body.subtotalBruto || 0);
    const descuentoPctIn = Number(req.body.descuentoPct || 0);
    const descuentoImporteIn = Number(req.body.descuentoImporte || 0);
    const totalFinalIn = Number(req.body.total || req.body.totalFinal || 0);
    const itemsIn = Array.isArray(req.body.items) ? req.body.items : [];

    let allItems = itemsIn.map((x) => {
      const cantidad = Number(x.cantidad || 0);
      const descripcion = String(x.descripcion || "").trim();
      const precioConIva = Number(x.precioConIva || 0);
      const subtotalConIva = Number(x.subtotalConIva || 0);

      if (!cantidad || !descripcion) return null;

      const sub = subtotalConIva > 0 ? round2(subtotalConIva) : round2(cantidad * precioConIva);
      const unit = precioConIva > 0 ? round2(precioConIva) : (cantidad > 0 ? round2(sub / cantidad) : 0);

      if (!sub || !unit) return null;

      return {
        cantidad,
        descripcion,
        precioConIva: unit,
        subtotalConIva: sub
      };
    }).filter(Boolean);

    if (!allItems.length) {
      return res.status(400).json({ message: "Ítems inválidos" });
    }

    const rec = await getReceptorDesdePadron(cuitCliente);

    const subtotalCalc = sumItemsBruto(allItems);
    let factor = 1;
    if (totalFinalIn > 0 && subtotalCalc > 0) factor = totalFinalIn / subtotalCalc;
    else if (descuentoPctIn > 0) factor = 1 - (descuentoPctIn / 100);
    else if (descuentoImporteIn > 0 && subtotalCalc > 0) factor = (subtotalCalc - descuentoImporteIn) / subtotalCalc;

    if (factor > 0 && factor < 1 && totalFinalIn > 0) {
      allItems = applyFactorAndReconcile(allItems, factor, totalFinalIn);
    }

    const chunks = [];
    for (let i = 0; i < allItems.length; i += ITEMS_POR_FACTURA) {
      chunks.push(allItems.slice(i, i + ITEMS_POR_FACTURA));
    }

    const partes = chunks.map(ch => round2(ch.reduce((a, it) => a + Number(it.subtotalConIva || 0), 0)));
    const totalRemitoGlobal = totalFinalIn > 0
      ? round2(totalFinalIn)
      : round2(partes.reduce((a, x) => a + x, 0));

    const resultados = [];
    const fecha = req.body.fechaNC || todayISO();
    const cbteFch = yyyymmdd(fecha);
    const factorUsado = (factor > 0 && factor < 1) ? factor : 1;
    let acumuladoSubBruto = 0;
    const mailParts = [];
    const mailAttachments = [];

    for (let i = 0; i < chunks.length; i++) {
      const chunkItems = chunks[i];

      let notaFactura = "";
      if (chunks.length > 1) {
        notaFactura = `FACTURA PARTE ${i + 1} DE ${chunks.length}\n`;
        notaFactura += `Total remito: $ ${formatMoneyAR(totalRemitoGlobal)}\n`;
        partes.forEach((t, idx) => {
          notaFactura += `Parte ${idx + 1}: $ ${formatMoneyAR(t)}\n`;
        });
        notaFactura = notaFactura.trim();
      }

      let impTotal = 0;
      let impNeto = 0;
      let impIVA = 0;

      const itemsCalc = chunkItems.map((it) => {
        const subConIva = Number(it.subtotalConIva || 0);
        const subNeto = round2(subConIva / 1.21);
        const subIva = round2(subConIva - subNeto);
        impTotal += subConIva;
        impNeto += subNeto;
        impIVA += subIva;

        return {
          ...it,
          subtotalNeto: subNeto,
          precioNeto: it.cantidad > 0 ? round2(subNeto / it.cantidad) : 0
        };
      });

      impTotal = round2(impTotal);
      impNeto = round2(impNeto);
      impIVA = round2(impIVA);

      let subBrutoParte = 0;
      let descParte = 0;
      let pctParte = 0;

      if (factorUsado < 1 && impTotal > 0) {
        pctParte = descuentoPctIn > 0 ? round2(descuentoPctIn) : 0;

        if (i < chunks.length - 1) {
          subBrutoParte = round2(impTotal / factorUsado);
        } else if (subtotalBrutoIn > 0) {
          subBrutoParte = round2(subtotalBrutoIn - acumuladoSubBruto);
          if (subBrutoParte < 0) subBrutoParte = round2(impTotal / factorUsado);
        } else {
          subBrutoParte = round2(impTotal / factorUsado);
        }

        descParte = round2(subBrutoParte - impTotal);
        acumuladoSubBruto = round2(acumuladoSubBruto + subBrutoParte);
      }

      const nro = (await afip.ElectronicBilling.getLastVoucher(pv, CBTE_TIPO_REAL)) + 1;

      const voucherData = {
        CantReg: 1,
        PtoVta: pv,
        CbteTipo: CBTE_TIPO_REAL,
        Concepto: 1,
        DocTipo: 80,
        DocNro: Number(cuitCliente),
        CbteDesde: nro,
        CbteHasta: nro,
        CbteFch: cbteFch,
        ImpTotal: impTotal,
        ImpTotConc: 0,
        ImpNeto: impNeto,
        ImpOpEx: 0,
        ImpIVA: impIVA,
        ImpTrib: 0,
        MonId: "PES",
        MonCotiz: 1,
        Iva: [{ Id: 5, BaseImp: impNeto, Importe: impIVA }]
      };

      const result = await afip.ElectronicBilling.createVoucher(voucherData);

      const qrPayload = {
        ver: 1,
        fecha,
        cuit: CUIT_DISTRIBUIDORA,
        ptoVta: pv,
        tipoCmp: CBTE_TIPO_REAL,
        nroCmp: nro,
        importe: impTotal,
        moneda: "PES",
        ctz: 1,
        tipoDocRec: 80,
        nroDocRec: Number(cuitCliente),
        tipoCodAut: "E",
        codAut: Number(result.CAE)
      };

      const qrDataUrl = await QRCode.toDataURL(
        `https://www.arca.gob.ar/fe/qr/?p=${Buffer.from(JSON.stringify(qrPayload)).toString("base64")}`,
        { margin: 0, width: 170 }
      );

      const htmlPDF = buildFacturaHtml({
        receptor: {
          cuit: cuitCliente,
          nombre: rec.nombre,
          condicionIVA: rec.condicionIVA,
          domicilioAfip: rec.domicilioAfip,
          domicilioRemito: domicilioRemitoIn
        },
        fechaISO: fecha,
        pv,
        nro,
        items: itemsCalc,
        neto: impNeto,
        iva: impIVA,
        total: impTotal,
        cae: result.CAE,
        caeVtoISO: result.CAEFchVto,
        condicionVenta,
        qrDataUrl,
        isPreview: false,
        notaFactura,
        subtotalBruto: subBrutoParte,
        descuentoPct: pctParte,
        descuentoImporte: descParte,
        totalFinal: impTotal
      });

      const pdfRes = await afip.ElectronicBilling.createPDF({
        html: htmlPDF,
        file_name: `FA_${pad(pv, 5)}-${pad(nro, 8)}`,
        options: {
          width: 8.27,
          marginTop: 0.35,
          marginBottom: 0.35,
          marginLeft: 0.35,
          marginRight: 0.35
        }
      });

      async function downloadPdfWithRetry(url, maxRetries = 5, delayMs = 1500) {
        let lastErr;
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
          try {
            const buffer = await downloadToBuffer(url);
            if (buffer && buffer.length > 0) return buffer;
            throw new Error("PDF vacío");
          } catch (e) {
            lastErr = e;
            if (attempt < maxRetries) {
              await new Promise(res => setTimeout(res, delayMs));
            }
          }
        }
        throw lastErr || new Error("No se pudo descargar el PDF");
      }

      let pdfBuffer = null;
      try {
        pdfBuffer = await downloadPdfWithRetry(pdfRes.file);
        console.log(`✅ [PDF] Descargado ${pad(pv, 5)}-${pad(nro, 8)} | bytes=${pdfBuffer.length}`);
      } catch (e) {
        console.error("⚠️ [PDF] No pude bajar PDF:", e?.message || e);
      }

      let pdfPublicUrl = "";
      try {
        if (pdfBuffer?.length) {
          pdfPublicUrl = await savePublicPdf(pdfBuffer, `FA_${pad(pv, 5)}-${pad(nro, 8)}`);
          console.log(`✅ [PDF] URL pública: ${pdfPublicUrl}`);
        } else {
          pdfPublicUrl = String(pdfRes.file || "");
          console.warn(`⚠️ [PDF] Uso URL original de AFIPSDK: ${pdfPublicUrl}`);
        }
      } catch (e) {
        pdfPublicUrl = String(pdfRes.file || "");
        console.error("⚠️ [PDF] Error guardando copia pública:", e?.message || e);
      }

      const comprobante = await guardarFacturaEnDB({
        cuitCliente,
        rec,
        nro,
        pv,
        cae: result.CAE,
        impTotal,
        pdfPublicUrl,
        condicionVenta,
        fecha,
        chunkItems,
        emailAEnviar
      });

      mailParts.push({
        parte: i + 1,
        totalPartes: chunks.length,
        pv,
        nro,
        cae: result.CAE,
        total: impTotal,
        pdfUrl: pdfPublicUrl,
        comprobante
      });

      if (pdfBuffer?.length) {
        mailAttachments.push({
          filename: `FA_${pad(pv, 5)}-${pad(nro, 8)}.pdf`,
          content: pdfBuffer,
          contentType: "application/pdf"
        });
      }

      resultados.push({
        nroFactura: nro,
        cae: result.CAE,
        total: impTotal,
        pdfUrl: pdfPublicUrl,
        comprobante
      });
    }

    // ── EMAIL SINCRÓNICO — se envía ANTES de responder al frontend ──
    let emailResult = { sent: false, error: "no intentado" };
    try {
      emailResult = await enviarEmailFactura({
        mailParts,
        mailAttachments,
        rec,
        cuitCliente,
        domicilioRemitoIn,
        condicionVenta,
        subtotalBrutoIn,
        descuentoPctIn,
        descuentoImporteIn,
        totalFinalIn,
        emailAEnviar
      });
    } catch (emailErr) {
      emailResult = { sent: false, error: emailErr?.message || "Error inesperado" };
      console.error("⚠️ [Email] Error no capturado:", emailErr?.message || emailErr);
    }

    // ── Ahora sí responder al frontend ──
    let finalMsg = resultados.length > 1
      ? `¡Factura dividida! Se emitieron ${resultados.length} comprobantes con éxito.`
      : `Factura autorizada con éxito.`;

    if (emailResult.sent) {
      finalMsg += ` Email enviado a ${emailAEnviar}.`;
    } else {
      finalMsg += ` ⚠️ Email no pudo enviarse: ${emailResult.error}. Podés descargar el PDF manualmente.`;
    }

    let waText = `Factura de Mercado Limpio\nCliente: ${rec.nombre}\nCUIT: ${cuitCliente}\n\n`;
    resultados.forEach((r, idx) => {
      waText += `Parte ${idx + 1}: Comp. Nro ${pad(r.nroFactura, 8)} | Total: $ ${formatMoneyAR(r.total)} | CAE: ${r.cae}\n`;
      if (r.pdfUrl) waText += `PDF: ${r.pdfUrl}\n`;
      waText += "\n";
    });

    res.json({
      ok: true,
      version: APP_VERSION,
      puntoDeVenta: pv,
      mensaje: finalMsg,
      emailEnviado: emailResult.sent,
      emailError: emailResult.error || "",
      facturas: resultados,
      receptor: {
        cuit: cuitCliente,
        nombre: rec.nombre,
        domicilio: domicilioRemitoIn || rec.domicilioAfip
      },
      waLink: `https://wa.me/?text=${encodeURIComponent(waText)}`
    });

  } catch (err) {
    if (!res.headersSent) {
      res.status(500).json({ message: err.message, detail: err?.data || null });
    }
    console.error("❌ [/facturar]", err?.message || err);
  }
});

// ================================================================
// 📅 REPORTE MENSUAL AUTOMÁTICO — se ejecuta el 1° de cada mes
// ================================================================
async function generarYEnviarReporteMensual() {
  if (!supabase) {
    console.error("❌ [Reporte Mensual] Sin conexión a Supabase");
    return;
  }
  if (!resendClient) {
    console.error("❌ [Reporte Mensual] Sin Resend configurado");
    return;
  }

  const emailDestino = process.env.ANALYTICS_REPORT_EMAIL || "santamariapablodaniel@gmail.com";
  const ahora = new Date();

  // Calculamos el mes anterior (el que acaba de terminar)
  const mesAnterior   = ahora.getMonth() === 0 ? 12 : ahora.getMonth();
  const anioAnterior  = ahora.getMonth() === 0 ? ahora.getFullYear() - 1 : ahora.getFullYear();
  const inicioPrevMes = new Date(anioAnterior, mesAnterior - 1, 1).toISOString();
  const finPrevMes    = new Date(ahora.getFullYear(), ahora.getMonth(), 1).toISOString();

  // Mes anterior al anterior (para comparar)
  const mes2Anterior  = mesAnterior === 1 ? 12 : mesAnterior - 1;
  const anio2Anterior = mesAnterior === 1 ? anioAnterior - 1 : anioAnterior;
  const inicioMes2    = new Date(anio2Anterior, mes2Anterior - 1, 1).toISOString();

  const MESES = ["Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];
  const nombreMes = MESES[mesAnterior - 1];

  try {
    console.log(`📊 [Reporte Mensual] Generando reporte de ${nombreMes} ${anioAnterior}...`);

    // Pedidos del mes anterior
    const { data: pedidosMes } = await supabase
      .from("pedidos")
      .select("id, estado, vendedor, created_at")
      .gte("created_at", inicioPrevMes)
      .lt("created_at", finPrevMes);

    // Pedidos del mes previo (para comparar)
    const { data: pedidosMes2 } = await supabase
      .from("pedidos")
      .select("id, estado, vendedor")
      .gte("created_at", inicioMes2)
      .lt("created_at", inicioPrevMes);

    // Faltantes del mes anterior
    const idsPedidosMes = (pedidosMes || []).map(p => p.id);
    let faltantesMes = [];
    if (idsPedidosMes.length) {
      const { data: faltData } = await supabase
        .from("items_pedido")
        .select("descripcion, pedido_id")
        .eq("es_faltante", true)
        .in("pedido_id", idsPedidosMes.slice(0, 400)); // límite seguro
      faltantesMes = faltData || [];
    }

    const totalPed  = (pedidosMes  || []).length;
    const totalPed2 = (pedidosMes2 || []).length;
    const totalFact = (pedidosMes  || []).filter(p => p.estado === "facturado").length;
    const totalFalt = faltantesMes.length;
    const difPed    = totalPed - totalPed2;
    const tasaFact  = totalPed > 0 ? Math.round((totalFact / totalPed) * 100) : 0;

    // Top faltantes
    const faltCounts = {};
    faltantesMes.forEach(i => {
      const k = (i.descripcion || "").trim().toLowerCase();
      if (k) faltCounts[k] = (faltCounts[k] || 0) + 1;
    });
    const topFalt = Object.entries(faltCounts).sort((a, b) => b[1] - a[1]).slice(0, 8);

    // Por vendedor
    const porVend = {};
    (pedidosMes || []).forEach(p => {
      const v = (p.vendedor || "Sin asignar").trim();
      if (!porVend[v]) porVend[v] = { pedidos: 0, facturados: 0 };
      porVend[v].pedidos++;
      if (p.estado === "facturado") porVend[v].facturados++;
    });
    const vendedoresRanking = Object.entries(porVend)
      .sort((a, b) => b[1].pedidos - a[1].pedidos);

    // Veredicto ventas
    const medallas = ["🥇", "🥈", "🥉"];
    const vVentas = difPed > 0
      ? `✅ Vendiste MÁS que en ${MESES[mes2Anterior - 1]} (${difPed} pedidos más)`
      : difPed < 0
        ? `⚠️ Vendiste MENOS que en ${MESES[mes2Anterior - 1]} (${Math.abs(difPed)} pedidos menos)`
        : `➡️ Igual que en ${MESES[mes2Anterior - 1]}`;

    const vendFilas = vendedoresRanking.map(([nombre, d], i) => {
      const tasaV = d.pedidos > 0 ? Math.round((d.facturados / d.pedidos) * 100) : 0;
      return `
        <tr style="border-bottom:1px solid #f1f5f9">
          <td style="padding:10px 12px;font-weight:700;color:#0f172a">${medallas[i] ?? (i + 1) + "°"} ${nombre}</td>
          <td style="padding:10px 12px;text-align:center;font-weight:800;font-size:15px">${d.pedidos}</td>
          <td style="padding:10px 12px;text-align:center;color:#64748b">${d.facturados} (${tasaV}%)</td>
        </tr>`;
    }).join("");

    const faltFilas = topFalt.map(([desc, n], i) => `
      <tr style="border-bottom:1px solid #f1f5f9">
        <td style="padding:8px 12px;color:#64748b">${i + 1}</td>
        <td style="padding:8px 12px;color:#0f172a;font-weight:500">${desc.slice(0, 55)}</td>
        <td style="padding:8px 12px;font-weight:700;color:#ef4444">${n}x</td>
      </tr>`).join("");

    const htmlEmail = `
      <div style="font-family:'Segoe UI',Helvetica,Arial,sans-serif;color:#1a1a1a;max-width:600px;margin:20px auto;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden">

        <div style="background:linear-gradient(135deg,#6366f1,#8b5cf6);padding:32px;text-align:center">
          <div style="font-size:40px;margin-bottom:8px">📊</div>
          <h1 style="color:#fff;margin:0;font-size:22px;font-weight:900">Reporte de ${nombreMes} ${anioAnterior}</h1>
          <p style="color:rgba(255,255,255,.75);margin:6px 0 0;font-size:13px">Mercado Limpio · Resumen mensual automático</p>
        </div>

        <div style="padding:28px">

          <!-- RESUMEN EJECUTIVO -->
          <div style="background:#f8fafc;border-radius:10px;padding:20px;margin-bottom:24px;border-left:4px solid #6366f1">
            <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px">Resumen del mes</div>
            <div style="font-size:15px;color:#0f172a;line-height:1.7">
              <div>📦 <strong>${totalPed} pedidos</strong> recibidos en ${nombreMes}</div>
              <div>✅ <strong>${totalFact} facturados</strong> (${tasaFact}% del total)</div>
              <div>⚠️ <strong>${totalFalt} artículos faltantes</strong> en el mes</div>
              <div style="margin-top:10px;font-size:14px;color:${difPed >= 0 ? '#16a34a' : '#dc2626'};font-weight:700">${vVentas}</div>
            </div>
          </div>

          <!-- VENDEDORES -->
          <h3 style="font-size:14px;font-weight:700;color:#0f172a;text-transform:uppercase;letter-spacing:.04em;margin:0 0 10px">Ranking de vendedores</h3>
          <table style="width:100%;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-bottom:24px">
            <thead>
              <tr style="background:#f8fafc">
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Vendedor</th>
                <th style="padding:10px 12px;text-align:center;color:#64748b;font-size:11px;text-transform:uppercase">Pedidos</th>
                <th style="padding:10px 12px;text-align:center;color:#64748b;font-size:11px;text-transform:uppercase">Facturados</th>
              </tr>
            </thead>
            <tbody>${vendFilas || "<tr><td colspan='3' style='padding:12px;text-align:center;color:#94a3b8'>Sin datos</td></tr>"}</tbody>
          </table>

          <!-- FALTANTES -->
          ${topFalt.length ? `
          <h3 style="font-size:14px;font-weight:700;color:#0f172a;text-transform:uppercase;letter-spacing:.04em;margin:0 0 10px">Artículos que más faltaron</h3>
          <table style="width:100%;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-bottom:24px">
            <thead>
              <tr style="background:#f8fafc">
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px">#</th>
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Artículo</th>
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Veces</th>
              </tr>
            </thead>
            <tbody>${faltFilas}</tbody>
          </table>` : ""}

          <p style="font-size:12px;color:#94a3b8;text-align:center;margin-top:16px">
            Este reporte fue generado automáticamente el 1° de ${MESES[ahora.getMonth()]} · Mercado Limpio
          </p>
        </div>
      </div>`;

    const { error: repError } = await resendClient.emails.send({
      from: `"Mercado Limpio" <ventas@mercadolimpio.ar>`,
      to: emailDestino,
      reply_to: GMAIL_USER,
      subject: `📊 Resumen de ${nombreMes} ${anioAnterior} — Mercado Limpio`,
      html: htmlEmail
    });
    if (repError) throw new Error(`Resend error: ${repError.message || JSON.stringify(repError)}`);

    console.log(`✅ [Reporte Mensual] Enviado a ${emailDestino} — ${totalPed} pedidos, ${totalFalt} faltantes`);
  } catch (err) {
    console.error("❌ [Reporte Mensual] Error:", err?.message || err);
  }
}

// Cron: todos los días a las 8:00 AM Argentina (UTC-3 = 11:00 UTC)
// Solo ejecuta la lógica si es el 1° del mes
cron.schedule("0 11 * * *", () => {
  const hoy = new Date();
  if (hoy.getDate() === 1) {
    console.log("📅 [Cron] Hoy es 1° del mes — generando reporte mensual...");
    generarYEnviarReporteMensual();
  }
}, { timezone: "UTC" });

// Endpoint para disparar el reporte manualmente (para testear)
app.get("/reporte-mensual-manual", async (req, res) => {
  try {
    await generarYEnviarReporteMensual();
    res.json({ ok: true, mensaje: "Reporte mensual enviado" });
  } catch (err) {
    res.status(500).json({ ok: false, message: err?.message });
  }
});

// ================================================================
// 📊 REPORTE ANALÍTICO POR EMAIL
// ================================================================
app.post("/enviar-reporte-analitica", async (req, res) => {
  try {
    const {
      periodo = "semana",
      comparativa_semana = {},
      comparativa_mes = {},
      proyeccion_semanal = [],
      vendedores = [],
      top_faltantes = [],
      fecha = new Date().toLocaleDateString("es-AR")
    } = req.body || {};

    const emailDestino = process.env.ANALYTICS_REPORT_EMAIL || "santamariapablodaniel@gmail.com";

    const cs = comparativa_semana;
    const cm = comparativa_mes;

    function varBadge(val, invert = false) {
      if (val === null || val === undefined) return "";
      const sign = val > 0 ? "+" : "";
      const color = invert
        ? (val > 0 ? "#ef4444" : val < 0 ? "#22c55e" : "#94a3b8")
        : (val > 0 ? "#22c55e" : val < 0 ? "#ef4444" : "#94a3b8");
      const arrow = val > 0 ? "↑" : val < 0 ? "↓" : "→";
      return `<span style="color:${color};font-weight:700;margin-left:6px">${arrow} ${sign}${val}%</span>`;
    }

    function metricRow(label, actual, anterior, varPct, invert = false) {
      return `
        <tr style="border-bottom:1px solid #e2e8f0">
          <td style="padding:10px 12px;color:#475569;font-size:13px">${label}</td>
          <td style="padding:10px 12px;font-weight:800;font-size:16px;color:#0f172a">${actual}</td>
          <td style="padding:10px 12px;color:#64748b;font-size:13px">${anterior} ${varBadge(varPct, invert)}</td>
        </tr>`;
    }

    const tasaSemActual  = cs.items_actual  > 0 ? Math.round((cs.faltantes_actual  / cs.items_actual)  * 100) : 0;
    const tasaSemAnterior= cs.items_anterior > 0 ? Math.round((cs.faltantes_anterior / cs.items_anterior) * 100) : 0;
    const tasaMesActual  = cm.items_actual  > 0 ? Math.round((cm.faltantes_actual  / cm.items_actual)  * 100) : 0;
    const tasaMesAnterior= cm.items_anterior > 0 ? Math.round((cm.faltantes_anterior / cm.items_anterior) * 100) : 0;
    const varTasaSem = tasaSemAnterior > 0 ? +(((tasaSemActual - tasaSemAnterior)/tasaSemAnterior)*100).toFixed(1) : null;
    const varTasMes  = tasaMesAnterior > 0 ? +(((tasaMesActual  - tasaMesAnterior) /tasaMesAnterior) *100).toFixed(1) : null;

    // Proyección
    let proyTitle = "Sin datos de proyección";
    if (proyeccion_semanal.length >= 2) {
      const rec = proyeccion_semanal.slice(-3);
      const avgRec = rec.reduce((s, r) => s + r.pedidos, 0) / rec.length;
      const hoy = new Date();
      const diasEnMes = new Date(hoy.getFullYear(), hoy.getMonth() + 1, 0).getDate();
      const diasRestantes = diasEnMes - hoy.getDate();
      const tasaDiaria = avgRec / 5;
      const pedidosMes = cm.pedidos_actual || 0;
      const proy = Math.round(pedidosMes + tasaDiaria * diasRestantes);
      proyTitle = `Proyección fin de mes: <strong>${proy} pedidos</strong> (quedan ${diasRestantes} días)`;
    }

    // Top faltantes rows
    const faltantesRows = top_faltantes.map(([desc, n], i) =>
      `<tr style="border-bottom:1px solid #f1f5f9">
         <td style="padding:8px 12px;color:#64748b;font-size:12px">${i+1}</td>
         <td style="padding:8px 12px;color:#0f172a;font-size:13px;font-weight:500">${String(desc).slice(0,50)}</td>
         <td style="padding:8px 12px;font-weight:700;color:#ef4444;font-size:13px">${n}x</td>
       </tr>`
    ).join("");

    // Vendedores rows
    const vendedoresRows = vendedores.slice(0, 6).map(v =>
      `<tr style="border-bottom:1px solid #f1f5f9">
         <td style="padding:8px 12px;font-weight:600;color:#0f172a;font-size:13px">${String(v.vendedor || "—")}</td>
         <td style="padding:8px 12px;text-align:center;font-weight:700;font-size:14px">${v.pedidos_semana_actual}</td>
         <td style="padding:8px 12px;text-align:center;color:#64748b;font-size:13px">${v.pedidos_semana_anterior}</td>
         <td style="padding:8px 12px;text-align:center;font-weight:700;font-size:14px">${v.pedidos_mes_actual}</td>
         <td style="padding:8px 12px;text-align:center;font-weight:700;color:#ef4444;font-size:13px">${v.faltantes_semana}</td>
       </tr>`
    ).join("");

    const htmlReporte = `
      <div style="font-family:'Segoe UI',Helvetica,Arial,sans-serif;color:#1a1a1a;max-width:640px;margin:20px auto;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;box-shadow:0 4px 6px -1px rgba(0,0,0,0.1)">

        <div style="background:linear-gradient(135deg,#6366f1,#8b5cf6);padding:32px;text-align:center">
          <div style="font-size:36px;margin-bottom:8px">📊</div>
          <h1 style="color:#fff;margin:0;font-size:22px;font-weight:900">Reporte Analítico</h1>
          <p style="color:rgba(255,255,255,.75);margin:6px 0 0;font-size:13px">Mercado Limpio · ${fecha}</p>
        </div>

        <div style="padding:28px">

          <p style="color:#475569;font-size:14px;margin:0 0 20px">
            Resumen ejecutivo de ventas, faltantes y proyección del negocio.
          </p>

          <!-- PROYECCIÓN -->
          <div style="background:#0f172a;border-radius:10px;padding:18px 20px;margin-bottom:24px">
            <div style="font-size:11px;color:rgba(255,255,255,.5);text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px">Proyección</div>
            <p style="color:#fff;font-size:15px;margin:0">${proyTitle}</p>
          </div>

          <!-- SEMANA ACTUAL VS ANTERIOR -->
          <h3 style="font-size:14px;font-weight:700;color:#0f172a;text-transform:uppercase;letter-spacing:.04em;margin:0 0 10px">Semana actual vs anterior</h3>
          <table style="width:100%;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-bottom:24px;font-size:13px">
            <thead>
              <tr style="background:#f8fafc">
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Métrica</th>
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Esta sem.</th>
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Sem. ant.</th>
              </tr>
            </thead>
            <tbody>
              ${metricRow("Pedidos",    cs.pedidos_actual   ?? "—", cs.pedidos_anterior   ?? "—", cs.var_pedidos_pct)}
              ${metricRow("Facturados", cs.facturados_actual ?? "—", cs.facturados_anterior ?? "—", cs.var_facturados_pct)}
              ${metricRow("Faltantes",  cs.faltantes_actual  ?? "—", cs.faltantes_anterior  ?? "—", cs.var_faltantes_pct,  true)}
              ${metricRow("Tasa falt.", `${tasaSemActual}%`, `${tasaSemAnterior}%`, varTasaSem, true)}
            </tbody>
          </table>

          <!-- MES ACTUAL VS ANTERIOR -->
          <h3 style="font-size:14px;font-weight:700;color:#0f172a;text-transform:uppercase;letter-spacing:.04em;margin:0 0 10px">Mes actual vs anterior</h3>
          <table style="width:100%;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-bottom:24px;font-size:13px">
            <thead>
              <tr style="background:#f8fafc">
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Métrica</th>
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Este mes</th>
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Mes ant.</th>
              </tr>
            </thead>
            <tbody>
              ${metricRow("Pedidos",    cm.pedidos_actual   ?? "—", cm.pedidos_anterior   ?? "—", cm.var_pedidos_pct)}
              ${metricRow("Facturados", cm.facturados_actual ?? "—", cm.facturados_anterior ?? "—", cm.var_facturados_pct)}
              ${metricRow("Faltantes",  cm.faltantes_actual  ?? "—", cm.faltantes_anterior  ?? "—", cm.var_faltantes_pct,  true)}
              ${metricRow("Tasa falt.", `${tasaMesActual}%`, `${tasaMesAnterior}%`, varTasMes, true)}
            </tbody>
          </table>

          <!-- RENDIMIENTO VENDEDORES -->
          ${vendedoresRows ? `
          <h3 style="font-size:14px;font-weight:700;color:#0f172a;text-transform:uppercase;letter-spacing:.04em;margin:0 0 10px">Rendimiento vendedores</h3>
          <table style="width:100%;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-bottom:24px;font-size:13px">
            <thead>
              <tr style="background:#f8fafc">
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Vendedor</th>
                <th style="padding:10px 12px;text-align:center;color:#64748b;font-size:11px;text-transform:uppercase">Sem.</th>
                <th style="padding:10px 12px;text-align:center;color:#64748b;font-size:11px;text-transform:uppercase">Sem. ant.</th>
                <th style="padding:10px 12px;text-align:center;color:#64748b;font-size:11px;text-transform:uppercase">Mes</th>
                <th style="padding:10px 12px;text-align:center;color:#64748b;font-size:11px;text-transform:uppercase">Faltantes</th>
              </tr>
            </thead>
            <tbody>${vendedoresRows}</tbody>
          </table>` : ""}

          <!-- TOP FALTANTES -->
          ${faltantesRows ? `
          <h3 style="font-size:14px;font-weight:700;color:#0f172a;text-transform:uppercase;letter-spacing:.04em;margin:0 0 10px">Top artículos faltantes (30 días)</h3>
          <table style="width:100%;border-collapse:collapse;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;margin-bottom:24px;font-size:13px">
            <thead>
              <tr style="background:#f8fafc">
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px">#</th>
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Artículo</th>
                <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px;text-transform:uppercase">Veces</th>
              </tr>
            </thead>
            <tbody>${faltantesRows}</tbody>
          </table>` : ""}

          <p style="font-size:13px;color:#64748b;text-align:center;background:#f8fafc;padding:12px;border-radius:8px;margin-top:8px">
            Reporte generado automáticamente desde Analytics · Mercado Limpio
          </p>
        </div>

        <div style="background:#f1f5f9;padding:16px;text-align:center;font-size:11px;color:#94a3b8;border-top:1px solid #e2e8f0">
          <p style="margin:0">Mercado Limpio · Buenos Aires, Argentina</p>
        </div>
      </div>`;

    if (!resendClient) throw new Error("Resend no configurado — revisar RESEND_API_KEY");

    const { error: analyticsError } = await resendClient.emails.send({
      from: `"Mercado Limpio Analytics" <ventas@mercadolimpio.ar>`,
      to: emailDestino,
      reply_to: GMAIL_USER,
      subject: `📊 Reporte Analítico — ${fecha}`,
      html: htmlReporte
    });
    if (analyticsError) throw new Error(`Resend error: ${analyticsError.message || JSON.stringify(analyticsError)}`);

    console.log(`✅ [Analytics] Reporte enviado a ${emailDestino}`);
    return res.json({ ok: true, enviado_a: emailDestino });

  } catch (err) {
    console.error("❌ [/enviar-reporte-analitica]", err?.message || err);
    return res.status(500).json({ ok: false, message: err?.message || "Error al enviar reporte" });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Motor listo en puerto ${PORT} | v: ${APP_VERSION}`);
  console.log(`   PRODUCTION=${PRODUCTION} | PV_ENV=${PUNTO_VENTA_ENV || "(auto)"}`);
  console.log(`   DEBUG=${DEBUG ? "1" : "0"} | ITEMS_POR_FACTURA=${ITEMS_POR_FACTURA}`);
  console.log(`   Uploads: ${uploadDir} | Public PDFs: ${publicPdfDir}`);
  console.log(`   PUBLIC_URL: ${PUBLIC_URL}`);
  console.log(`   Logo: ${LOGO_PATH || "(no encontrado)"}`);
  console.log(`   PADRON10: ${ENABLE_PADRON_10 ? "YES" : "NO"}`);
  console.log(`   Supabase: ${supabase ? "✅ CONECTADO" : "⚠️  SOLO ARCHIVO LOCAL"}`);
});

process.on("unhandledRejection", (reason) => { console.error("❌ unhandledRejection:", reason); });
process.on("uncaughtException", (err) => { console.error("❌ uncaughtException:", err); });

// ================================================================
// ✅ BASE DE DATOS — Supabase (primario) + archivo local (backup)
// ================================================================
const DB_FACTURAS = path.join(process.cwd(), "facturas_db.jsonl");

async function guardarFacturaEnDB({
  cuitCliente,
  rec,
  nro,
  pv,
  cae,
  impTotal,
  pdfPublicUrl,
  condicionVenta,
  fecha,
  chunkItems,
  emailAEnviar = "",
  emailStatus = "pending",
  emailError = ""
}) {
  const registro = {
    timestamp: new Date().toISOString(),
    fecha,
    anio: Number(String(fecha).split("-")[0]),
    mes: Number(String(fecha).split("-")[1]),
    comprobante: `A-${String(pv).padStart(5, "0")}-${String(nro).padStart(8, "0")}`,
    nro_factura: nro,
    punto_venta: pv,
    cae: String(cae),
    cuit_cliente: String(cuitCliente),
    nombre_cliente: rec?.nombre || `CUIT ${cuitCliente}`,
    domicilio: rec?.domicilioAfip || "",
    condicion_venta: String(condicionVenta || ""),
    total: impTotal,
    pdf_url: pdfPublicUrl || "",
    email_to: String(emailAEnviar || ""),
    email_status: String(emailStatus || "pending"),
    email_error: String(emailError || ""),
    items: JSON.stringify((Array.isArray(chunkItems) ? chunkItems : []).map(it => ({
      descripcion: it.descripcion,
      cantidad: it.cantidad,
      precio_con_iva: it.precioConIva,
      subtotal_con_iva: it.subtotalConIva
    })))
  };

  if (supabase) {
    try {
      const { error } = await supabase.from("facturas").upsert([registro], {
        onConflict: "comprobante"
      });
      if (error) throw error;
      if (DEBUG) log("✅ [Supabase] Factura guardada:", registro.comprobante);
    } catch (e) {
      console.error("❌ [Supabase] Error guardando factura:", e?.message || e);
      _guardarEnArchivoLocal(registro);
    }
  } else {
    _guardarEnArchivoLocal(registro);
  }

  return registro.comprobante;
}

function _guardarEnArchivoLocal(registro) {
  try {
    const local = {
      ...registro,
      nroFactura: registro.nro_factura,
      puntoVenta: registro.punto_venta,
      cuitCliente: registro.cuit_cliente,
      nombreCliente: registro.nombre_cliente,
      condicionVenta: registro.condicion_venta,
      pdfUrl: registro.pdf_url,
      items: typeof registro.items === "string" ? JSON.parse(registro.items) : registro.items
    };
    fs.appendFileSync(DB_FACTURAS, JSON.stringify(local) + "\n", "utf-8");
  } catch (e) {
    console.error("❌ [DB Local] Error guardando:", e?.message || e);
  }
}

async function actualizarEstadoEmail(comprobante, status, errorMsg = "", emailTo = "") {
  if (!supabase) return;

  try {
    const payload = {
      email_status: String(status || ""),
      email_error: String(errorMsg || "")
    };

    if (emailTo) payload.email_to = String(emailTo);

    const { error } = await supabase
      .from("facturas")
      .update(payload)
      .eq("comprobante", comprobante);

    if (error) throw error;
  } catch (e) {
    console.error("❌ [Supabase] Error actualizando email_status:", e?.message || e);
  }
}

async function leerFacturasDelMes(anio, mes) {
  if (supabase) {
    try {
      const { data, error } = await supabase
        .from("facturas")
        .select("*")
        .eq("anio", anio)
        .eq("mes", mes)
        .order("fecha", { ascending: true });

      if (error) throw error;
      if (DEBUG) log(`✅ [Supabase] ${data.length} facturas leídas para ${mes}/${anio}`);

      return data.map(f => ({
        ...f,
        nroFactura: f.nro_factura,
        puntoVenta: f.punto_venta,
        cuitCliente: f.cuit_cliente,
        nombreCliente: f.nombre_cliente,
        condicionVenta: f.condicion_venta,
        pdfUrl: f.pdf_url,
        items: typeof f.items === "string" ? JSON.parse(f.items || "[]") : (f.items || [])
      }));
    } catch (e) {
      console.error("❌ [Supabase] Error leyendo facturas, usando archivo local:", e?.message || e);
    }
  }

  if (!fs.existsSync(DB_FACTURAS)) return [];
  return fs.readFileSync(DB_FACTURAS, "utf-8")
    .split("\n")
    .filter(Boolean)
    .map(l => { try { return JSON.parse(l); } catch { return null; } })
    .filter(f => f && f.anio === anio && f.mes === mes);
}

// ================================================================
// ✅ API RESUMEN MES PARA APP / PWA
// ================================================================
app.get("/admin/facturas-mes", async (req, res) => {
  try {
    const anio = Number(req.query.anio);
    const mes = Number(req.query.mes);

    if (!anio || !mes || mes < 1 || mes > 12) {
      return res.status(400).json({
        ok: false,
        message: "Parámetros inválidos. Debés enviar anio y mes."
      });
    }

    const facturasRaw = await leerFacturasDelMes(anio, mes);

    const facturas = (Array.isArray(facturasRaw) ? facturasRaw : []).map(f => {
      const puntoVenta = Number(f.puntoVenta || f.punto_venta || 0);
      const nroFactura = Number(f.nroFactura || f.nro_factura || 0);
      const cbteTipo = Number(f.cbteTipo || f.cbte_tipo || inferCbteTipoFromComprobante(f.comprobante || "") || 0);

      const comprobante =
        f.comprobante ||
        buildComprobanteLabelByTipo(cbteTipo || CBTE_TIPO_REAL, puntoVenta, nroFactura);

      const tipoCbte =
        String(comprobante).startsWith("NC-") || cbteTipo === 3 || cbteTipo === 8 || cbteTipo === 13 || cbteTipo === 53
          ? "NC"
          : "FA";

      const emailError = String(f.email_error || "");
      const anulado =
        /ANULADA POR/i.test(emailError) ||
        Boolean(f.anulado);

      return {
        fecha: f.fecha || "",
        comprobante,
        tipoCbte,
        puntoVenta,
        nroFactura,
        nro: nroFactura,
        cae: String(f.cae || ""),
        cuit: String(f.cuitCliente || f.cuit_cliente || ""),
        nombre: String(f.nombreCliente || f.nombre_cliente || ""),
        total: Number(f.total || 0),
        pdfUrl: String(f.pdfUrl || f.pdf_url || ""),
        emailTo: String(f.email_to || ""),
        emailStatus: String(f.email_status || ""),
        anulado
      };
    });

    const total = round2(facturas.reduce((acc, f) => acc + Number(f.total || 0), 0));

    return res.json({
      ok: true,
      anio,
      mes,
      cantidad: facturas.length,
      total,
      facturas
    });
  } catch (err) {
    console.error("❌ [/admin/facturas-mes]", err?.message || err);
    return res.status(500).json({
      ok: false,
      message: err?.message || "Error al leer facturas del mes"
    });
  }
});

// ================================================================
// ✅ REENVIAR EMAIL DE FACTURA
// ================================================================
app.post("/reenviar-email", async (req, res) => {
  try {
    const comprobante = String(req.body.comprobante || "").trim();
    const emailDestino = String(req.body.emailDestino || "").trim();

    if (!comprobante) return res.status(400).json({ ok: false, message: "Falta comprobante" });
    if (!emailDestino) return res.status(400).json({ ok: false, message: "Falta email de destino" });
    if (!resendClient) return res.status(500).json({ ok: false, message: "Resend no configurado — revisar RESEND_API_KEY" });

    // Buscar la factura en Supabase
    if (!supabase) return res.status(500).json({ ok: false, message: "Supabase no configurado" });
    const { data: rows, error: dbErr } = await supabase
      .from("facturas")
      .select("*")
      .eq("comprobante", comprobante)
      .limit(1);

    if (dbErr) throw new Error(dbErr.message);
    if (!rows || rows.length === 0) return res.status(404).json({ ok: false, message: `Comprobante ${comprobante} no encontrado` });

    const f = rows[0];
    const pdfUrl = String(f.pdfUrl || f.pdf_url || "");
    const nombre = String(f.nombreCliente || f.nombre_cliente || "Cliente");
    const cuit = String(f.cuitCliente || f.cuit_cliente || "");
    const totalNum = Number(f.total || 0);
    const cae = String(f.cae || "");

    // Descargar el PDF
    let pdfBuffer = null;
    if (pdfUrl) {
      try {
        const pdfResp = await fetch(pdfUrl);
        if (pdfResp.ok) {
          const arrayBuf = await pdfResp.arrayBuffer();
          pdfBuffer = Buffer.from(arrayBuf);
          console.log(`✅ [Reenvio] PDF descargado | bytes=${pdfBuffer.length}`);
        } else {
          console.warn(`⚠️ [Reenvio] PDF no disponible (${pdfResp.status})`);
        }
      } catch (e) {
        console.warn(`⚠️ [Reenvio] Error descargando PDF:`, e?.message);
      }
    }

    // Construir email HTML
    const subject = `Factura ${comprobante} - ${EMISOR.nombreVisible} (reenvío)`;
    const mailHtml = `
      <div style="font-family:'Segoe UI',Helvetica,Arial,sans-serif;color:#1a1a1a;max-width:600px;margin:20px auto;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;box-shadow:0 4px 6px -1px rgba(0,0,0,0.1);">
        <div style="background-color:#0f172a;padding:30px;text-align:center;">
          <h1 style="color:#ffffff;margin:0;font-size:24px;letter-spacing:1px;font-weight:900;">${safeText(EMISOR.nombreVisible)}</h1>
          <p style="color:#94a3b8;margin:5px 0 0 0;font-size:12px;text-transform:uppercase;letter-spacing:0.5px;">Comprobante Electrónico · Reenvío</p>
        </div>
        <div style="padding:30px;">
          <p style="font-size:15px;line-height:1.6;color:#334155;margin-top:0;">Estimado/a <strong>${safeText(nombre)}</strong>,</p>
          <p style="font-size:15px;line-height:1.6;color:#334155;">Le reenviamos el comprobante oficial correspondiente a su última operación.</p>
          <div style="background-color:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:20px;margin:25px 0;border-left:4px solid #3b82f6;">
            <h3 style="margin:0 0 10px 0;font-size:14px;color:#0f172a;text-transform:uppercase;">Datos del Comprobante</h3>
            <div style="font-size:13px;color:#475569;line-height:1.8;">
              <div><strong>Comprobante:</strong> ${safeText(comprobante)}</div>
              <div><strong>CUIT:</strong> ${safeText(cuit)}</div>
              <div><strong>CAE:</strong> ${safeText(cae)}</div>
              <div style="margin-top:10px;padding-top:10px;border-top:1px dashed #cbd5e1;font-size:15px;color:#0f172a;font-weight:bold;">
                Total: $ ${formatMoneyAR(totalNum)}
              </div>
            </div>
          </div>
          <p style="font-size:14px;color:#64748b;line-height:1.6;background:#f1f5f9;padding:12px;border-radius:6px;text-align:center;margin-top:25px;">
            📎 <strong>Importante:</strong> Adjunto a este correo encontrará el archivo PDF oficial autorizado por ARCA/AFIP.
          </p>
          <p style="font-size:15px;line-height:1.6;color:#334155;margin-top:30px;">Atentamente,<br><strong>Administración Mercado Limpio</strong></p>
        </div>
        <div style="background-color:#f1f5f9;padding:20px;text-align:center;font-size:11px;color:#64748b;border-top:1px solid #e2e8f0;">
          <p style="margin:0;">Este es un reenvío automático desde el sistema de facturación.</p>
          <p style="margin:5px 0 0 0;">Buenos Aires, Argentina</p>
        </div>
      </div>`;

    const attachments = pdfBuffer ? [{
      filename: `${comprobante.replace(/[^A-Za-z0-9_-]/g, "_")}.pdf`,
      content: pdfBuffer
    }] : [];

    console.log(`🚀 [Reenvio] Enviando ${comprobante} a ${emailDestino}...`);
    const { data: sendData, error: sendError } = await resendClient.emails.send({
      from: `"${EMISOR.nombreVisible}" <ventas@mercadolimpio.ar>`,
      to: emailDestino,
      reply_to: GMAIL_USER,
      subject,
      html: mailHtml,
      attachments
    });

    if (sendError) throw new Error(`Resend error: ${sendError.message || JSON.stringify(sendError)}`);
    console.log(`✅ [Reenvio] Email enviado a ${emailDestino} | id=${sendData?.id}`);

    await actualizarEstadoEmail(comprobante, "sent", "", emailDestino);

    return res.json({ ok: true, message: `Email reenviado a ${emailDestino}` });
  } catch (err) {
    console.error("❌ [/reenviar-email]", err?.message || err);
    return res.status(500).json({ ok: false, message: err?.message || "Error al reenviar email" });
  }
});

// ================================================================
// ✅ RESUMEN MENSUAL (igual que antes, ahora con datos de Supabase)
// ================================================================
function buildResumenHTMLProfesional(anio, mes, facturas) {
  const MESES = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];
  const nombreMes = MESES[mes] || `Mes ${mes}`;
  const totalGeneral = facturas.reduce((a, f) => a + Number(f.total || 0), 0);
  const porCliente = {};
  for (const f of facturas) {
    const k = f.cuitCliente || f.cuit_cliente;
    if (!porCliente[k]) porCliente[k] = { nombre: f.nombreCliente || f.nombre_cliente, cuit: k, total: 0, cant: 0 };
    porCliente[k].total += Number(f.total || 0);
    porCliente[k].cant++;
  }
  const clientes = Object.values(porCliente).sort((a, b) => b.total - a.total);
  const fmtAR = n => new Intl.NumberFormat("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(n || 0));
  const safe  = s => String(s || "").replace(/[<>]/g, "");

  const filasFacturas = facturas
    .sort((a, b) => String(a.fecha).localeCompare(String(b.fecha)))
    .map((f, i) => {
      const comp   = f.comprobante || `A-${String(f.puntoVenta||f.punto_venta||"").padStart(5,"0")}-${String(f.nroFactura||f.nro_factura||"").padStart(8,"0")}`;
      const nombre = f.nombreCliente || f.nombre_cliente || "";
      const cuit   = f.cuitCliente   || f.cuit_cliente   || "";
      const pdfUrl = f.pdfUrl        || f.pdf_url        || "";
      return `<tr style="background:${i % 2 === 0 ? "#ffffff" : "#f8fafc"};">
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#3b82f6;">${safe(f.fecha)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;font-weight:900;font-family:monospace;">${safe(comp)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;font-weight:700;">${safe(nombre)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;color:#64748b;font-size:12px;">${safe(cuit)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;color:#64748b;font-size:12px;font-family:monospace;">${safe(f.cae)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;text-align:right;font-weight:900;font-size:15px;color:#0f172a;">$ ${fmtAR(f.total)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;text-align:center;">${pdfUrl ? `<a href="${safe(pdfUrl)}" style="background:#3b82f6;color:#fff;padding:5px 12px;border-radius:6px;text-decoration:none;font-size:12px;font-weight:900;">📄 Ver</a>` : '<span style="color:#cbd5e1;">—</span>'}</td>
      </tr>`;
    }).join("");

  const filasClientes = clientes.map(c => `
    <tr>
      <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;font-weight:800;">${safe(c.nombre)}</td>
      <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;color:#64748b;font-size:12px;font-family:monospace;">${safe(c.cuit)}</td>
      <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;text-align:center;font-weight:700;">${c.cant}</td>
      <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;text-align:right;font-weight:900;color:#10b981;">$ ${fmtAR(c.total)}</td>
    </tr>`).join("");

  return `<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"/><title>Resumen ${nombreMes} ${anio}</title></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Segoe UI',Arial,sans-serif;">
<div style="max-width:900px;margin:0 auto;padding:30px 20px;">
  <div style="background:linear-gradient(135deg,#1e3a5f 0%,#0f172a 100%);border-radius:16px 16px 0 0;padding:32px 36px;border-bottom:3px solid #3b82f6;">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;">
      <div>
        <div style="font-size:11px;font-weight:900;color:#3b82f6;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px;">Reporte Interno</div>
        <div style="font-size:26px;font-weight:900;color:#ffffff;">MERCADO LIMPIO</div>
        <div style="font-size:13px;color:#94a3b8;font-weight:700;margin-top:3px;">Distribuidora · Facturación Electrónica ARCA/AFIP</div>
      </div>
      <div style="text-align:right;">
        <div style="font-size:13px;color:#64748b;font-weight:700;">Período</div>
        <div style="font-size:22px;font-weight:900;color:#3b82f6;">${nombreMes}</div>
        <div style="font-size:18px;font-weight:900;color:#94a3b8;">${anio}</div>
      </div>
    </div>
  </div>
  <div style="display:flex;background:#1e293b;border-left:1px solid #334155;border-right:1px solid #334155;">
    <div style="flex:1;padding:22px 20px;text-align:center;border-right:1px solid #334155;"><div style="font-size:36px;font-weight:900;color:#3b82f6;">${facturas.length}</div><div style="font-size:12px;color:#94a3b8;font-weight:700;margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Facturas emitidas</div></div>
    <div style="flex:2;padding:22px 20px;text-align:center;border-right:1px solid #334155;"><div style="font-size:32px;font-weight:900;color:#10b981;">$ ${fmtAR(totalGeneral)}</div><div style="font-size:12px;color:#94a3b8;font-weight:700;margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Total facturado del mes</div></div>
    <div style="flex:1;padding:22px 20px;text-align:center;"><div style="font-size:36px;font-weight:900;color:#f59e0b;">${clientes.length}</div><div style="font-size:12px;color:#94a3b8;font-weight:700;margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Clientes distintos</div></div>
  </div>
  <div style="background:#ffffff;border-left:1px solid #e2e8f0;border-right:1px solid #e2e8f0;">
    <div style="background:#1e293b;padding:16px 20px;"><div style="font-size:13px;font-weight:900;color:#ffffff;letter-spacing:0.5px;">📋 DETALLE COMPLETO DE FACTURAS</div></div>
    <div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:13px;color:#334155;">
      <thead><tr style="background:#f1f5f9;"><th style="padding:12px 14px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;color:#475569;">Fecha</th><th style="padding:12px 14px;text-align:left;font-size:11px;text-transform:uppercase;color:#475569;">Comprobante</th><th style="padding:12px 14px;text-align:left;font-size:11px;text-transform:uppercase;color:#475569;">Cliente</th><th style="padding:12px 14px;text-align:left;font-size:11px;text-transform:uppercase;color:#475569;">CUIT</th><th style="padding:12px 14px;text-align:left;font-size:11px;text-transform:uppercase;color:#475569;">CAE</th><th style="padding:12px 14px;text-align:right;font-size:11px;text-transform:uppercase;color:#475569;">Total</th><th style="padding:12px 14px;text-align:center;font-size:11px;text-transform:uppercase;color:#475569;">PDF</th></tr></thead>
      <tbody>${filasFacturas || `<tr><td colspan="7" style="padding:30px;text-align:center;color:#94a3b8;font-style:italic;">Sin facturas registradas este mes.</td></tr>`}</tbody>
      <tfoot><tr style="background:#0f172a;"><td colspan="5" style="padding:14px 18px;font-weight:900;color:#fff;font-size:13px;text-align:right;text-transform:uppercase;">TOTAL DEL MES</td><td style="padding:14px 18px;font-weight:900;color:#10b981;font-size:17px;text-align:right;">$ ${fmtAR(totalGeneral)}</td><td></td></tr></tfoot>
    </table></div>
  </div>
  <div style="background:#ffffff;border-left:1px solid #e2e8f0;border-right:1px solid #e2e8f0;border-top:4px solid #f1f5f9;">
    <div style="background:#1e293b;padding:16px 20px;"><div style="font-size:13px;font-weight:900;color:#ffffff;letter-spacing:0.5px;">👥 RESUMEN POR CLIENTE</div></div>
    <table style="width:100%;border-collapse:collapse;font-size:13px;color:#334155;">
      <thead><tr style="background:#f1f5f9;"><th style="padding:12px 14px;text-align:left;font-size:11px;text-transform:uppercase;color:#475569;">Cliente</th><th style="padding:12px 14px;text-align:left;font-size:11px;text-transform:uppercase;color:#475569;">CUIT</th><th style="padding:12px 14px;text-align:center;font-size:11px;text-transform:uppercase;color:#475569;">Facturas</th><th style="padding:12px 14px;text-align:right;font-size:11px;text-transform:uppercase;color:#475569;">Total</th></tr></thead>
      <tbody>${filasClientes || `<tr><td colspan="4" style="padding:20px;text-align:center;color:#94a3b8;">Sin datos.</td></tr>`}</tbody>
    </table>
  </div>
  <div style="background:#1e293b;border-radius:0 0 16px 16px;padding:18px 28px;text-align:center;border:1px solid #334155;border-top:none;">
    <div style="font-size:11px;color:#475569;font-weight:700;">Reporte generado automáticamente · Sistema Mercado Limpio · ARCA/AFIP</div>
    <div style="font-size:11px;color:#334155;margin-top:4px;">Solo para uso interno — distribuidoramercadolimpio@gmail.com</div>
  </div>
</div></body></html>`;
}

async function enviarResumenMensual(anioForzar, mesForzar) {
  const hoy  = new Date(Date.now() - 3 * 60 * 60 * 1000);
  const anio = anioForzar || (hoy.getUTCMonth() === 0 ? hoy.getUTCFullYear() - 1 : hoy.getUTCFullYear());
  const mes  = mesForzar  || (hoy.getUTCMonth() === 0 ? 12 : hoy.getUTCMonth());
  const MESES = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];
  const facturas     = await leerFacturasDelMes(anio, mes);
  const totalGeneral = facturas.reduce((a, f) => a + Number(f.total || 0), 0);
  const fmtAR        = n => new Intl.NumberFormat("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(n || 0));
  console.log(`📊 [Resumen] ${MESES[mes]} ${anio}: ${facturas.length} facturas | $ ${fmtAR(totalGeneral)}`);
  const htmlMail = buildResumenHTMLProfesional(anio, mes, facturas);
  const subject  = `📊 Resumen ${MESES[mes]} ${anio} — ${facturas.length} facturas | $ ${fmtAR(totalGeneral)}`;
  const toAddress = process.env.RESEND_API_KEY ? "santamariapablodaniel@gmail.com" : "distribuidoramercadolimpio@gmail.com";
  if (!resendClient) throw new Error("Resend no configurado — revisar RESEND_API_KEY en variables de entorno");
  const { error: resumenError } = await resendClient.emails.send({ from: "Mercado Limpio <onboarding@resend.dev>", to: toAddress, subject, html: htmlMail });
  if (resumenError) throw new Error(`Resend error: ${resumenError.message || JSON.stringify(resumenError)}`);
  console.log(`✅ [Resumen] Enviado vía Resend a ${toAddress}`);
}

let _ultimoResumenEnviado = null;
setInterval(async () => {
  try {
    const ahora = new Date(Date.now() - 3 * 60 * 60 * 1000);
    const dia = ahora.getUTCDate(), hora = ahora.getUTCHours();
    const clave = `${ahora.getUTCFullYear()}-${String(ahora.getUTCMonth() + 1).padStart(2, "0")}`;
    if (dia === 1 && hora === 8 && _ultimoResumenEnviado !== clave) {
      _ultimoResumenEnviado = clave;
      console.log(`🗓️  [Cron] Enviando resumen mensual...`);
      await enviarResumenMensual();
    }
  } catch (e) { console.error("❌ [Cron]", e?.message || e); }
}, 30 * 60 * 1000).unref?.();

console.log("🗓️  [Cron] Resumen mensual activado → 1° de cada mes a las 08:00 AR");

app.get("/admin/test-resumen", async (req, res) => {
  if (req.query.token !== "mercadolimpio") return res.status(401).send("No autorizado.");
  try {
    await enviarResumenMensual(req.query.anio ? Number(req.query.anio) : null, req.query.mes ? Number(req.query.mes) : null);
    res.send("✅ Resumen enviado");
  } catch (e) { res.status(500).send("❌ Error: " + (e?.message || e)); }
});

// ================================================================
// ✅ ANULACIÓN CON NOTA DE CRÉDITO ASOCIADA
// ================================================================

const NC_TIPO_MAP = {
  1: 3,    // Factura A  -> Nota de Crédito A
  6: 8,    // Factura B  -> Nota de Crédito B
  11: 13,  // Factura C  -> Nota de Crédito C
  51: 53   // Factura M  -> Nota de Crédito M
};

function inferCbteTipoFromComprobante(comp = "") {
  const s = String(comp || "").toUpperCase();
  if (s.startsWith("NC-")) return CBTE_TIPO_REAL === 51 ? 53 : 3;
  
  // Si el texto dice "A-" pero tu motor está en modo 51 (Factura M), forzamos a que use el 51.
  if (s.startsWith("A-")) return CBTE_TIPO_REAL === 51 ? 51 : 1;
  
  if (s.startsWith("B-")) return 6;
  if (s.startsWith("C-")) return 11;
  if (s.startsWith("M-")) return 51;
  return CBTE_TIPO_REAL;
} 

function inferNcTipoFromOriginal(originalCbteTipo) {
  return NC_TIPO_MAP[Number(originalCbteTipo)] || 53;
}

function buildComprobanteLabelByTipo(cbteTipo, pv, nro) {
  const map = {
    1: "FA-A",
    3: "NC-A",
    6: "FA-B",
    8: "NC-B",
    11: "FA-C",
    13: "NC-C",
    51: "FA-M",
    53: "NC-M"
  };
  const pref = map[Number(cbteTipo)] || "CBTE";
  return `${pref}-${pad(pv, 5)}-${pad(nro, 8)}`;
}

async function buscarComprobanteGuardado({ comprobante, pv, nro }) {
  const compNorm = String(comprobante || "").trim();

  if (supabase) {
    try {
      let data = null;
      let error = null;

      if (compNorm) {
        const q = await supabase
          .from("facturas")
          .select("*")
          .eq("comprobante", compNorm)
          .limit(1)
          .maybeSingle();
        data = q.data;
        error = q.error;
      } else if (pv && nro) {
        const q = await supabase
          .from("facturas")
          .select("*")
          .eq("punto_venta", Number(pv))
          .eq("nro_factura", Number(nro))
          .limit(1)
          .maybeSingle();
        data = q.data;
        error = q.error;
      }

      if (error) throw error;
      if (data) {
        return {
          ...data,
          nroFactura: data.nro_factura,
          puntoVenta: data.punto_venta,
          cuitCliente: data.cuit_cliente,
          nombreCliente: data.nombre_cliente,
          condicionVenta: data.condicion_venta,
          pdfUrl: data.pdf_url,
          cbteTipo: Number(data.cbte_tipo || inferCbteTipoFromComprobante(data.comprobante))
        };
      }
    } catch (e) {
      console.error("❌ [NC] Error buscando comprobante en Supabase:", e?.message || e);
    }
  }

  if (!fs.existsSync(DB_FACTURAS)) return null;

  const rows = fs.readFileSync(DB_FACTURAS, "utf-8")
    .split("\n")
    .filter(Boolean)
    .map(l => { try { return JSON.parse(l); } catch { return null; } })
    .filter(Boolean);

  let found = null;

  if (compNorm) {
    found = rows.find(r => String(r.comprobante || "").trim() === compNorm) || null;
  } else if (pv && nro) {
    found = rows.find(r =>
      Number(r.puntoVenta || r.punto_venta) === Number(pv) &&
      Number(r.nroFactura || r.nro_factura) === Number(nro)
    ) || null;
  }

  if (!found) return null;

  return {
    ...found,
    nroFactura: found.nroFactura || found.nro_factura,
    puntoVenta: found.puntoVenta || found.punto_venta,
    cuitCliente: found.cuitCliente || found.cuit_cliente,
    nombreCliente: found.nombreCliente || found.nombre_cliente,
    condicionVenta: found.condicionVenta || found.condicion_venta,
    pdfUrl: found.pdfUrl || found.pdf_url,
    cbteTipo: Number(found.cbteTipo || found.cbte_tipo || inferCbteTipoFromComprobante(found.comprobante))
  };
}

async function guardarComprobanteGeneralEnDB({
  comprobante,
  cbteTipo,
  cuitCliente,
  nombreCliente,
  domicilio,
  nro,
  pv,
  cae,
  impTotal,
  pdfPublicUrl,
  condicionVenta,
  fecha,
  items,
  emailAEnviar = "",
  emailStatus = "pending",
  emailError = ""
}) {
  const registro = {
    timestamp: new Date().toISOString(),
    fecha,
    anio: Number(String(fecha).split("-")[0]),
    mes: Number(String(fecha).split("-")[1]),
    comprobante: String(comprobante || ""),
    cbte_tipo: Number(cbteTipo || 0),
    nro_factura: Number(nro || 0),
    punto_venta: Number(pv || 0),
    cae: String(cae || ""),
    cuit_cliente: String(cuitCliente || ""),
    nombre_cliente: String(nombreCliente || ""),
    domicilio: String(domicilio || ""),
    condicion_venta: String(condicionVenta || ""),
    total: Number(impTotal || 0),
    pdf_url: String(pdfPublicUrl || ""),
    email_to: String(emailAEnviar || ""),
    email_status: String(emailStatus || "pending"),
    email_error: String(emailError || ""),
    items: JSON.stringify(Array.isArray(items) ? items : [])
  };

  if (supabase) {
    try {
      const { error } = await supabase
        .from("facturas")
        .upsert([registro], { onConflict: "comprobante" });

      if (error) throw error;
      if (DEBUG) log("✅ [Supabase] Comprobante guardado:", registro.comprobante);
    } catch (e) {
      console.error("❌ [Supabase] Error guardando comprobante general:", e?.message || e);
      _guardarEnArchivoLocal(registro);
    }
  } else {
    _guardarEnArchivoLocal(registro);
  }

  return registro.comprobante;
}

async function marcarComprobanteComoAnulado(comprobanteOriginal, ncComprobante, ncCae) {
  if (!supabase) return;

  try {
    const payload = {
      email_error: `ANULADA POR ${ncComprobante} | CAE ${ncCae}`
    };

    const { error } = await supabase
      .from("facturas")
      .update(payload)
      .eq("comprobante", comprobanteOriginal);

    if (error) throw error;
  } catch (e) {
    console.error("❌ [Supabase] Error marcando anulación:", e?.message || e);
  }
}

function buildNotaCreditoHtml({
  emisor,
  receptor,
  fechaISO,
  pv,
  nro,
  cae,
  caeVtoISO,
  qrDataUrl,
  total,
  originalComprobante,
  originalCae,
  motivo
}) {
  const pvStr = pad(pv, 5);
  const nroStr = pad(nro, 8);
  const fechaAR = String(fechaISO || "").split("-").reverse().join("/");
  const caeVtoAR = String(caeVtoISO || "").split("-").reverse().join("/");
  const domMain = String(receptor?.domicilioAfip || receptor?.domicilioRemito || "Domicilio no informado");

  return `<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8"/>
  <title>Nota de Crédito ${pvStr}-${nroStr}</title>
  <style>
    * { box-sizing:border-box; font-family: Arial, Helvetica, sans-serif; }
    body { margin:0; color:#0f172a; background:#fff; }
    .page { width:820px; margin:0 auto; padding:30px; }
    .box { border:2px solid #1e293b; border-radius:8px; overflow:hidden; margin-bottom:18px; }
    .head { display:flex; }
    .col { flex:1; padding:18px 20px; }
    .col + .col { border-left:1px solid #e2e8f0; text-align:right; }
    .title { font-size:26px; font-weight:900; color:#1e293b; margin-bottom:6px; }
    .muted { font-size:11px; color:#475569; margin:3px 0; }
    .strong { color:#0f172a; font-weight:800; }
    .card { border:1px solid #cbd5e1; border-radius:8px; padding:14px; background:#f8fafc; margin-bottom:16px; }
    .card-title { font-size:12px; font-weight:900; text-transform:uppercase; color:#1e293b; margin-bottom:8px; }
    .motivo { background:#fef3c7; border:1px solid #f59e0b; color:#92400e; border-radius:8px; padding:12px; font-size:13px; font-weight:700; margin-top:10px; }
    .totals { display:flex; gap:18px; margin-top:18px; }
    .tot-box { flex:1; border:1px solid #cbd5e1; border-radius:8px; padding:15px; }
    .row { display:flex; justify-content:space-between; margin:8px 0; font-size:13px; }
    .row.final { border-top:2px solid #e2e8f0; padding-top:10px; margin-top:12px; font-size:18px; font-weight:900; }
    .arca { flex:1; border:1px solid #cbd5e1; border-radius:8px; padding:15px; background:#f8fafc; display:flex; align-items:center; justify-content:space-between; }
    .qr { width:110px; height:110px; }
    .footer { text-align:center; font-size:10px; font-weight:700; color:#64748b; margin-top:18px; border-top:1px dashed #cbd5e1; padding-top:10px; }
  </style>
</head>
<body>
  <div class="page">
    <div class="box">
      <div class="head">
        <div class="col">
          <div class="title">NOTA DE CRÉDITO</div>
          <div class="muted">Razón Social: <span class="strong">${safeText(emisor.nombreVisible)}</span></div>
          <div class="muted">Domicilio Comercial: <span class="strong">${safeText(emisor.domicilio)}</span></div>
          <div class="muted">Condición frente al IVA: <span class="strong">${safeText(emisor.condicionIVA)}</span></div>
        </div>
        <div class="col">
          <div class="title">M</div>
          <div class="muted">Punto de Venta: <span class="strong">${pvStr}</span> &nbsp;&nbsp; Comp. Nro: <span class="strong">${nroStr}</span></div>
          <div class="muted">Fecha de Emisión: <span class="strong">${safeText(fechaAR)}</span></div>
          <div class="muted">CUIT Emisor: <span class="strong">${safeText(CUIT_DISTRIBUIDORA)}</span></div>
          <div class="muted">CAE: <span class="strong">${safeText(cae)}</span></div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">Cliente</div>
      <div class="muted">CUIT: <span class="strong">${safeText(receptor.cuit)}</span></div>
      <div class="muted">Razón Social: <span class="strong">${safeText(receptor.nombre)}</span></div>
      <div class="muted">Domicilio: <span class="strong">${safeText(domMain)}</span></div>
      <div class="muted">Condición frente al IVA: <span class="strong">${safeText(receptor.condicionIVA || "IVA Responsable Inscripto")}</span></div>

      <div class="motivo">
        Anulación mediante Nota de Crédito asociada.<br/>
        Comprobante original: <strong>${safeText(originalComprobante)}</strong><br/>
        CAE original: <strong>${safeText(originalCae || "-")}</strong><br/>
        Motivo: <strong>${safeText(motivo || "Anulación por error / duplicación")}</strong>
      </div>
    </div>

    <div class="totals">
      <div class="arca">
        ${qrDataUrl ? `<img class="qr" src="${qrDataUrl}" alt="QR ARCA" />` : `<div class="qr"></div>`}
        <div style="text-align:right; font-size:11px; color:#334155; line-height:1.6;">
          Comprobante Autorizado por ARCA<br><br>
          <strong>CAE Nro:</strong> ${safeText(cae)}<br>
          <strong>Fecha Vto. CAE:</strong> ${safeText(caeVtoAR)}
        </div>
      </div>
      <div class="tot-box">
        <div class="row"><span>Importe Neto Gravado:</span><strong>$ ${formatMoneyAR(round2(Number(total || 0) / 1.21))}</strong></div>
        <div class="row"><span>IVA 21%:</span><strong>$ ${formatMoneyAR(round2(Number(total || 0) - round2(Number(total || 0) / 1.21)))}</strong></div>
        <div class="row final"><span>TOTAL NOTA DE CRÉDITO:</span><span>$ ${formatMoneyAR(total)}</span></div>
      </div>
    </div>

    <div class="footer">${safeText(emisor.leyenda)}</div>
  </div>
</body>
</html>`;
}

app.post("/anular-comprobante", async (req, res) => {
  try {
    const comprobanteOriginal = String(req.body.comprobante || "").trim();
    const pvOriginal = Number(req.body.puntoVenta || 0);
    const nroOriginal = Number(req.body.nroFactura || 0);
    const motivo = String(req.body.motivo || "Anulación por error / comprobante duplicado").trim();

    if (!comprobanteOriginal && (!pvOriginal || !nroOriginal)) {
      return res.status(400).json({
        ok: false,
        message: "Debés informar comprobante o puntoVenta + nroFactura"
      });
    }

    let original = await buscarComprobanteGuardado({
      comprobante: comprobanteOriginal,
      pv: pvOriginal,
      nro: nroOriginal
    });

    // Si no está en Supabase, creamos un objeto mínimo y consultamos padrón ARCA
    if (!original) {
      console.log("⚠️ Comprobante no en base, procediendo con datos manuales e integrando Padrón ARCA...");
      
      const cuitManual = onlyDigits(req.body.cuitCliente);
      let datosPadron = { nombre: "Cliente Externo", domicilioAfip: "" };

      // Buscamos en el padrón de AFIP/ARCA usando el CUIT que pusiste en el modal
      if (cuitManual && cuitManual.length === 11) {
        datosPadron = await getReceptorDesdePadron(cuitManual);
      }

      original = {
        puntoVenta: pvOriginal || 5,
        nroFactura: nroOriginal,
        cuitCliente: cuitManual,
        nombreCliente: datosPadron.nombre, // ¡Acá toma el nombre real de AFIP!
        domicilio: datosPadron.domicilioAfip, // ¡Acá toma el domicilio de AFIP!
        total: req.body.montoTotal || 0,
        cbteTipo: 51 // Asumimos Factura M por defecto
      };

      if (!original.nroFactura || !original.total || !original.cuitCliente) {
        return res.status(400).json({
          ok: false,
          message: "Para facturas antiguas no registradas, debés indicar Nro, CUIT y Monto Total en el modal."
        });
      }
    }

    const cbteTipoOriginal = Number(original.cbteTipo || CBTE_TIPO_REAL);
    const cbteTipoNC = inferNcTipoFromOriginal(cbteTipoOriginal);
    const pvNc = await getPtoVentaSeguro();
    
    // Acá tomamos la fecha que pongas en el modal, sino usa la de hoy
    const fecha = req.body.fechaNC || todayISO(); 
    const cbteFch = yyyymmdd(fecha);

    const totalOriginal = Math.abs(Number(original.total || 0));
    if (!(totalOriginal > 0)) {
      return res.status(400).json({
        ok: false,
        message: "El comprobante original no tiene un total válido para anular"
      });
    }

    const impNeto = round2(totalOriginal / 1.21);
    const impIVA = round2(totalOriginal - impNeto);
    const nroNC = (await afip.ElectronicBilling.getLastVoucher(pvNc, cbteTipoNC)) + 1;

    const voucherData = {
      CantReg: 1,
      PtoVta: pvNc,
      CbteTipo: cbteTipoNC,
      Concepto: 1,
      DocTipo: 80,
      DocNro: Number(original.cuitCliente),
      CbteDesde: nroNC,
      CbteHasta: nroNC,
      CbteFch: cbteFch,
      ImpTotal: totalOriginal,
      ImpTotConc: 0,
      ImpNeto: impNeto,
      ImpOpEx: 0,
      ImpIVA: impIVA,
      ImpTrib: 0,
      MonId: "PES",
      MonCotiz: 1,
      Iva: [{ Id: 5, BaseImp: impNeto, Importe: impIVA }],
      CbtesAsoc: [{
        Tipo: cbteTipoOriginal,
        PtoVta: Number(original.puntoVenta),
        Nro: Number(original.nroFactura)
      }]
    };

    const result = await afip.ElectronicBilling.createVoucher(voucherData);

    const qrPayload = {
      ver: 1,
      fecha,
      cuit: CUIT_DISTRIBUIDORA,
      ptoVta: pvNc,
      tipoCmp: cbteTipoNC,
      nroCmp: nroNC,
      importe: totalOriginal,
      moneda: "PES",
      ctz: 1,
      tipoDocRec: 80,
      nroDocRec: Number(original.cuitCliente),
      tipoCodAut: "E",
      codAut: Number(result.CAE)
    };

    const qrDataUrl = await QRCode.toDataURL(
      `https://www.arca.gob.ar/fe/qr/?p=${Buffer.from(JSON.stringify(qrPayload)).toString("base64")}`,
      { margin: 0, width: 170 }
    );

    const htmlNC = buildNotaCreditoHtml({
      emisor: EMISOR,
      receptor: {
        cuit: original.cuitCliente,
        nombre: original.nombreCliente,
        domicilioAfip: original.domicilio || "",
        condicionIVA: "IVA Responsable Inscripto"
      },
      fechaISO: fecha,
      pv: pvNc,
      nro: nroNC,
      cae: result.CAE,
      caeVtoISO: result.CAEFchVto,
      qrDataUrl,
      total: totalOriginal,
      originalComprobante: original.comprobante || buildComprobanteLabelByTipo(cbteTipoOriginal, original.puntoVenta, original.nroFactura),
      originalCae: original.cae || "",
      motivo
    });

    const pdfRes = await afip.ElectronicBilling.createPDF({
      html: htmlNC,
      file_name: `NC_${pad(pvNc, 5)}-${pad(nroNC, 8)}`,
      options: {
        width: 8.27,
        marginTop: 0.35,
        marginBottom: 0.35,
        marginLeft: 0.35,
        marginRight: 0.35
      }
    });

    let pdfBuffer = null;
    try {
      pdfBuffer = await downloadToBuffer(pdfRes.file);
    } catch (e) {
      console.error("⚠️ [NC] No pude descargar PDF NC:", e?.message || e);
    }

    let pdfPublicUrl = "";
    try {
      if (pdfBuffer?.length) {
        pdfPublicUrl = await savePublicPdf(pdfBuffer, `NC_${pad(pvNc, 5)}-${pad(nroNC, 8)}`);
      } else {
        pdfPublicUrl = String(pdfRes.file || "");
      }
    } catch (e) {
      pdfPublicUrl = String(pdfRes.file || "");
      console.error("⚠️ [NC] Error guardando PDF público:", e?.message || e);
    }

    const ncComprobante = buildComprobanteLabelByTipo(cbteTipoNC, pvNc, nroNC);

    await guardarComprobanteGeneralEnDB({
      comprobante: ncComprobante,
      cbteTipo: cbteTipoNC,
      cuitCliente: original.cuitCliente,
      nombreCliente: original.nombreCliente,
      domicilio: original.domicilio || "",
      nro: nroNC,
      pv: pvNc,
      cae: result.CAE,
      impTotal: -totalOriginal,
      pdfPublicUrl,
      condicionVenta: `ANULACIÓN / NC ASOCIADA A ${original.comprobante || buildComprobanteLabelByTipo(cbteTipoOriginal, original.puntoVenta, original.nroFactura)}`,
      fecha,
      items: [{
        descripcion: `Anulación de ${original.comprobante || buildComprobanteLabelByTipo(cbteTipoOriginal, original.puntoVenta, original.nroFactura)}`,
        cantidad: 1,
        precio_con_iva: totalOriginal,
        subtotal_con_iva: totalOriginal
      }],
      emailAEnviar: original.email_to || DEFAULT_EMAIL
    });

    await marcarComprobanteComoAnulado(
      original.comprobante || buildComprobanteLabelByTipo(cbteTipoOriginal, original.puntoVenta, original.nroFactura),
      ncComprobante,
      result.CAE
    );

    // ── Email NC vía Resend / Gmail (Sincrónico) ──
    const emailDestino = String(original.email_to || DEFAULT_EMAIL).trim();
    let ncEmailSent = false;

    if (emailDestino) {
      try {
        const subjectNC = `Nota de Crédito ${ncComprobante} - ${EMISOR.nombreVisible}`;
        const originalCompText = original.comprobante || buildComprobanteLabelByTipo(cbteTipoOriginal, original.puntoVenta, original.nroFactura);
        
        // NUEVO DISEÑO HTML PROFESIONAL Y LIMPIO (Tonos azules y grises)
        const mailHtmlNC = `
          <div style="font-family: 'Segoe UI', Helvetica, Arial, sans-serif; color: #1a1a1a; max-width: 600px; margin: 20px auto; border: 1px solid #e2e8f0; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);">
            <div style="background-color: #0f172a; padding: 30px; text-align: center;">
              <h1 style="color: #ffffff; margin: 0; font-size: 24px; letter-spacing: 1px; font-weight: 900;">${safeText(EMISOR.nombreVisible)}</h1>
              <p style="color: #94a3b8; margin: 5px 0 0 0; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;">Comprobante de Ajuste (Anulación)</p>
            </div>
            
            <div style="padding: 30px;">
              <p style="font-size: 15px; line-height: 1.6; color: #334155; margin-top: 0;">Estimado/a <strong>${safeText(original.nombreCliente)}</strong>,</p>
              <p style="font-size: 15px; line-height: 1.6; color: #334155;">
                Le informamos que se ha generado una <strong>Nota de Crédito</strong> a su favor. Este documento legal anula o ajusta el comprobante emitido anteriormente.
              </p>
              
              <div style="background-color: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 20px; margin: 25px 0; border-left: 4px solid #3b82f6;">
                <h3 style="margin: 0 0 10px 0; font-size: 14px; color: #0f172a; text-transform: uppercase;">Detalle de la operación</h3>
                <div style="font-size: 13px; color: #475569; line-height: 1.8;">
                  <div><strong>Razón Social:</strong> ${safeText(original.nombreCliente)}</div>
                  <div><strong>CUIT:</strong> ${safeText(original.cuitCliente)}</div>
                  ${original.domicilio ? `<div><strong>Domicilio Fiscal:</strong> ${safeText(original.domicilio)}</div>` : ""}
                  <div><strong>Comprobante Original:</strong> ${safeText(originalCompText)}</div>
                  <div><strong>Nota de Crédito:</strong> <span style="color: #0f172a; font-weight: bold;">${safeText(ncComprobante)}</span></div>
                  <div><strong>Motivo:</strong> ${safeText(motivo)}</div>
                  <div style="margin-top: 10px; padding-top: 10px; border-top: 1px dashed #cbd5e1; font-size: 15px; color: #0f172a;">
                    <strong>Total Ajustado:</strong> $ ${formatMoneyAR(totalOriginal)}
                  </div>
                </div>
              </div>
              
              <p style="font-size: 14px; color: #64748b; line-height: 1.6; background: #f1f5f9; padding: 12px; border-radius: 6px; text-align: center;">
                📎 <strong>Importante:</strong> Adjunto a este correo encontrará el archivo PDF oficial autorizado por ARCA/AFIP.
              </p>
              
              <p style="font-size: 15px; line-height: 1.6; color: #334155; margin-top: 30px;">
                Atentamente,<br>
                <strong>Administración Mercado Limpio</strong>
              </p>
            </div>
            
            <div style="background-color: #f1f5f9; padding: 20px; text-align: center; font-size: 11px; color: #64748b; border-top: 1px solid #e2e8f0;">
              <p style="margin: 0;">Este es un envío automático desde el sistema de facturación.</p>
              <p style="margin: 5px 0 0 0;">Buenos Aires, Argentina</p>
            </div>
          </div>`;

        if (!resendClient) throw new Error("Resend no configurado — revisar RESEND_API_KEY en variables de entorno");
        console.log("🚀 [NC] Enviando vía Resend API...");
        const { data: ncResendData, error: ncResendError } = await resendClient.emails.send({
          from: `"${EMISOR.nombreVisible}" <ventas@mercadolimpio.ar>`,
          to: emailDestino,
          reply_to: GMAIL_USER,
          subject: subjectNC,
          html: mailHtmlNC,
          attachments: pdfBuffer?.length ? [{
            filename: `NC_${ncComprobante}.pdf`,
            content: pdfBuffer
          }] : []
        });
        if (ncResendError) throw new Error(`Resend error: ${ncResendError.message || JSON.stringify(ncResendError)}`);
        ncEmailSent = true;
        await actualizarEstadoEmail(ncComprobante, "sent", "", emailDestino);
        console.log(`✅ [NC] Email enviado a ${emailDestino} | id=${ncResendData?.id}`);
      } catch (mailErr) {
        await actualizarEstadoEmail(ncComprobante, "failed", mailErr?.message, emailDestino);
        console.error("⚠️ [NC] Falló envío de mail:", mailErr?.message);
      }
    }

    return res.json({
      ok: true,
      message: `Nota de Crédito emitida correctamente.${ncEmailSent ? ` Email enviado a ${emailDestino}.` : " ⚠️ Email no enviado."}`,
      original: {
        comprobante: original.comprobante,
        cae: original.cae,
        total: Number(original.total || 0)
      },
      notaCredito: {
        comprobante: ncComprobante,
        puntoVenta: pvNc,
        nroFactura: nroNC,
        cae: result.CAE,
        total: totalOriginal,
        pdfUrl: pdfPublicUrl
      }
    });

  } catch (err) {
    console.error("❌ [/anular-comprobante]", err?.message || err);
    return res.status(500).json({
      ok: false,
      message: err?.message || "Error al anular comprobante"
    });
  }
});

// ================================================================
// MÓDULO EXTRACTO BANCARIO — Catálogo + algoritmo + endpoints
// ================================================================

const CATALOGO_MAKE = [
  { desc: "Make trapo piso blanco profesional", precio: 5200 },
  { desc: "Make trapo piso nido abeja blanco", precio: 5500 },
  { desc: "Make trapo piso gris suave", precio: 4900 },
  { desc: "Make trapo rayado", precio: 4800 },
  { desc: "Make franela naranja", precio: 3200 },
  { desc: "Make paño microfibra", precio: 3600 },
  { desc: "Make paño amarillo x 3 un", precio: 4100 },
  { desc: "Make rejilla americana", precio: 3100 },
  { desc: "Make rejilla cocina", precio: 2900 },
  { desc: "Make rejilla multicolor rayadita", precio: 2600 },
  { desc: "Make rejilla toalla color", precio: 2800 },
  { desc: "Make esponja acero 12 gr", precio: 2100 },
  { desc: "Make esponja acero 15 gr", precio: 2400 },
  { desc: "Make esponja bronce 12 gr", precio: 2300 },
  { desc: "Make fibra esponja c/salvauñas", precio: 2800 },
  { desc: "Make fibra esponja lisa", precio: 2400 },
  { desc: "Make fibra esponja x 3 un", precio: 6500 },
  { desc: "Make esponja fibra extra fuerte rosa", precio: 2600 },
  { desc: "Make balde 13 Lts", precio: 12500 },
  { desc: "Make balde 17 Lts", precio: 15800 },
  { desc: "Make balde traslucido 10 lt", precio: 9200 },
  { desc: "Make balde plastico 11 L", precio: 8800 },
  { desc: "Make bolsa consorcio 60 x 90", precio: 3500 },
  { desc: "Make bolsa consorcio 80 x 110", precio: 4800 },
  { desc: "Make bolsa consorcio 90 x 120", precio: 5800 },
  { desc: "Make bolsa de residuos 45 x 60 en rollo", precio: 2900 },
  { desc: "Make bolsa residuo 50 x 70", precio: 3200 },
  { desc: "Make guante classic M", precio: 4200 },
  { desc: "Make guante classic G", precio: 4500 },
  { desc: "Make guante soft M", precio: 5100 },
  { desc: "Make guante soft G", precio: 5400 },
  { desc: "Make escoba recta exterior", precio: 7800 },
  { desc: "Make escobillon 5 hileras", precio: 8500 },
  { desc: "Make escobillon 6 hileras", precio: 9200 },
  { desc: "Make escobillon Black", precio: 9800 },
  { desc: "Make secador doble goma 34cm", precio: 6800 },
  { desc: "Make secador doble goma 41cm", precio: 7900 },
  { desc: "Make secador Gold", precio: 8200 },
  { desc: "Make lampazo algodon blanco", precio: 9800 },
  { desc: "Make lampazo microfibra bicolor", precio: 12500 },
  { desc: "Make lampazo sintetico", precio: 11200 },
  { desc: "Make pulverizador 1 lts", precio: 5600 },
  { desc: "Make pulverizador 750 cc", precio: 4800 },
  { desc: "Make cepillo mano anatomico", precio: 3800 },
  { desc: "Make vela blanca larga", precio: 2800 },
  { desc: "Make vela roja parafina", precio: 2900 },
  { desc: "Make bolsa ecologica", precio: 1800 },
  { desc: "Make sopapa c/cabo", precio: 3200 },
  { desc: "Make cepillo lava jean", precio: 4200 },
  { desc: "Make escobilla c/base", precio: 3500 },
  { desc: "Make bolsas plasticas c/cierre grande", precio: 2100 },
  { desc: "Make canasto c/tapa 17 lt", precio: 14200 },
];

const CATALOGO_ROMYL = [
  { desc: "Romyl trapo piso blanco", precio: 4900 },
  { desc: "Romyl trapo piso gris", precio: 4600 },
  { desc: "Romyl trapo piso rayado", precio: 4400 },
  { desc: "Romyl rejilla multiuso", precio: 2800 },
  { desc: "Romyl rejilla color nido abeja", precio: 2700 },
  { desc: "Romyl rejilla microfibra", precio: 3100 },
  { desc: "Romyl esponja acero 13 gr", precio: 2200 },
  { desc: "Romyl esponja bronce 13 gr", precio: 2400 },
  { desc: "Romyl fibra esponja", precio: 2500 },
  { desc: "Romyl fibra esponja 3 un", precio: 6800 },
  { desc: "Romyl paño amarillo 1 un", precio: 1800 },
  { desc: "Romyl franela naranja", precio: 3100 },
  { desc: "Romyl balde 12 lt", precio: 11800 },
  { desc: "Romyl balde ovalado 14l c/escurridor", precio: 14500 },
  { desc: "Romyl secador negro 30 cm", precio: 5800 },
  { desc: "Romyl secador negro 40 cm", precio: 6900 },
  { desc: "Romyl lampazo algodon blanco", precio: 9200 },
  { desc: "Romyl lampazo algodon gris", precio: 9200 },
  { desc: "Romyl sopapa roja", precio: 2800 },
  { desc: "Romyl cepillo piso", precio: 5800 },
  { desc: "Romyl vela larga", precio: 2700 },
  { desc: "Romyl escobilla baño palito", precio: 3200 },
];

const CATALOGO_SAMANTHA = [
  { desc: "Samantha balde 10 lts", precio: 10200 },
  { desc: "Samantha balde oval c/escurridor", precio: 13500 },
  { desc: "Samantha escoba Super", precio: 7200 },
  { desc: "Samantha escobillon interior multiuso", precio: 8800 },
  { desc: "Samantha secador plastico 30 cm", precio: 4800 },
  { desc: "Samantha secador plastico 40 cm", precio: 5900 },
  { desc: "Samantha escobillon bicolor con cabo", precio: 9500 },
];

const CATALOGO_TODOESPONJA = [
  { desc: "Todoesponja esponja acero 15 gr", precio: 2400 },
  { desc: "Todoesponja esponja bronce 15 gr", precio: 2600 },
  { desc: "Todoesponja fibra esponja", precio: 2700 },
  { desc: "Todoesponja fibra esponja c/salvauñas", precio: 3100 },
  { desc: "Todoesponja rejilla americana", precio: 3200 },
  { desc: "Todoesponja trapo piso blanco clasico", precio: 4800 },
  { desc: "Todoesponja trapo piso gris clasico", precio: 4600 },
];

// Apellidos chinos más comunes en Argentina
const APELLIDOS_CHINOS = new Set([
  "li","wang","zhang","chen","liu","yang","huang","zhao","wu","zhou",
  "xu","sun","ma","zhu","hu","guo","he","lin","luo","zheng","xie",
  "tang","han","cao","deng","xiao","jiang","cai","peng","lu","ye",
  "su","cheng","wei","feng","dai","yin","dong","yu","qi","qian",
  "rong","fang","wen","hua","ding","yan","gao","shi","liang","jia",
  "mao","cui","qiu","du","yuan","wan","ni","lei","zhong","hao",
  "fan","tao","yao","meng","xiong","kang","long","shen","bao",
  "shao","niu","zeng","qin","gui","zhan","ling","jin","yue","pan",
  "lai","gong","gu","fu","huo","tian","bai","hou","yin","kong",
  "sheng","chai","zou","lv","mu","weng","xun","ren","ming","zuo",
  "piao","bian","cui","chao","nong","duan","fu","lu"
]);

function detectarNombreChino(nombre) {
  const n = String(nombre || "").toLowerCase().replace(/[^a-záéíóúüñ\s]/gi, "");
  return n.split(/\s+/).some(w => APELLIDOS_CHINOS.has(w));
}

function buildItemsParaMonto(montoTotal) {
  const R2 = n => Math.round((n + 1e-9) * 100) / 100;
  const shuffle = arr => {
    const a = [...arr];
    for (let i = a.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [a[i], a[j]] = [a[j], a[i]];
    }
    return a;
  };

  const makePool = shuffle(CATALOGO_MAKE);
  const otroPool = shuffle([...CATALOGO_ROMYL, ...CATALOGO_SAMANTHA, ...CATALOGO_TODOESPONJA]);

  const nMake = 3 + Math.floor(Math.random() * 2); // 3 o 4 items Make
  const nOtro = 2 + Math.floor(Math.random() * 2); // 2 o 3 items otros
  const selectedMake = makePool.slice(0, nMake);
  const selectedOtro = otroPool.slice(0, nOtro);

  const targetMake = R2(montoTotal * 0.60);
  const targetOtro = R2(montoTotal - targetMake);

  function fillItems(products, target) {
    const items = [];
    let remaining = target;
    for (let i = 0; i < products.length; i++) {
      const p = products[i];
      const isLast = i === products.length - 1;
      if (isLast) {
        const qty = Math.max(1, Math.round(remaining / p.precio));
        const precioAjustado = R2(remaining / qty);
        items.push({ descripcion: p.desc, cantidad: qty, precioConIva: precioAjustado, subtotalConIva: R2(qty * precioAjustado) });
      } else {
        const share = target / products.length;
        const qty = Math.max(1, Math.round(share / p.precio));
        const subtotal = R2(qty * p.precio);
        items.push({ descripcion: p.desc, cantidad: qty, precioConIva: p.precio, subtotalConIva: subtotal });
        remaining = R2(remaining - subtotal);
      }
    }
    return items;
  }

  const all = [...fillItems(selectedMake, targetMake), ...fillItems(selectedOtro, targetOtro)];

  // Cierre final: corregir cualquier diferencia residual de redondeo
  const totalCalc = R2(all.reduce((s, x) => s + x.subtotalConIva, 0));
  const diff = R2(montoTotal - totalCalc);
  if (Math.abs(diff) >= 0.01 && all.length > 0) {
    const last = all[all.length - 1];
    last.subtotalConIva = R2(last.subtotalConIva + diff);
    if (last.cantidad > 0) last.precioConIva = R2(last.subtotalConIva / last.cantidad);
  }

  return all;
}

// ── Parseo directo de PDF Santander (sin Gemini) ─────────────────
// Extrae transferencias recibidas del texto plano del PDF exportado
// desde la app Santander. CUIT: todo después del último '/', sin dígitos.
function parseSantanderPdfText(text) {
  const lines = text.split(/\r?\n/).map(l => l.trim()).filter(l => l.length > 0);
  const movimientos = [];
  let currentFecha = "";

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Rastrear fecha actual: DD/MM/YY o DD/MM/YYYY al inicio de línea
    const dateMatch = line.match(/^(\d{2})\/(\d{2})\/(\d{2,4})/);
    if (dateMatch) {
      const day = dateMatch[1], mon = dateMatch[2];
      const yr  = dateMatch[3].length === 2 ? "20" + dateMatch[3] : dateMatch[3];
      currentFecha = `${yr}-${mon}-${day}`;
    }

    // Detectar línea de transferencia recibida (no "realizada")
    const esRecibida = /transf[^a-z]*recibid/i.test(line) && !/realizada/i.test(line);
    if (!esRecibida) continue;

    // La línea siguiente: "De [nombre] / [medio] /[CUIT con posibles espacios]"
    const descLine = lines[i + 1] || "";
    if (!/^\s*de\s+/i.test(descLine)) continue;

    const nombreMatch = descLine.match(/^de\s+(.+?)\s*\//i);
    if (!nombreMatch) continue;
    const nombre = nombreMatch[1].trim().replace(/\s+/g, " ");

    // CUIT: todo lo que hay después del ÚLTIMO '/', quitando no-dígitos
    const lastSlashIdx = descLine.lastIndexOf("/");
    if (lastSlashIdx < 0) continue;
    const cuit = descLine.slice(lastSlashIdx + 1).replace(/\D/g, "");
    if (cuit.length !== 11) continue;

    // Monto: buscar en las 3 líneas siguientes "$ X.XXX,XX" positivo (sin guión previo)
    let monto = 0;
    for (let j = i + 2; j <= i + 4 && j < lines.length; j++) {
      const montoMatch = lines[j].match(/^\$\s*([\d.]+,\d{2})\s*$/);
      if (montoMatch) {
        monto = parseFloat(montoMatch[1].replace(/\./g, "").replace(",", "."));
        break;
      }
      // Formato alternativo sin símbolo: número con coma decimal
      const altMatch = lines[j].match(/^([\d.]+,\d{2})\s*$/);
      if (altMatch && !lines[j].startsWith("-")) {
        monto = parseFloat(altMatch[1].replace(/\./g, "").replace(",", "."));
        break;
      }
    }
    if (!monto || monto <= 0) continue;

    movimientos.push({ fecha: currentFecha, nombre, monto, cuit, descripcion: descLine });
  }
  return movimientos;
}

// ── POST /procesar-extracto ─────────────────────────────────────
// Soporta XLSX (parseo directo, sin Gemini) + PDF/imagen (Gemini).
// Para XLSX de Banco Provincia: extrae fecha, nombre, monto y CUIT
// directamente desde la columna Descripción sin depender de IA.
app.post("/procesar-extracto", upload.single("extracto"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, message: "No se recibió archivo" });

    const filePath    = req.file.path;
    const mimeType    = req.file.mimetype;
    const origName    = (req.file.originalname || "").toLowerCase();
    const isXlsx      = origName.endsWith(".xlsx") || origName.endsWith(".xls") ||
                        mimeType.includes("spreadsheet") || mimeType.includes("excel");
    const isPdf       = mimeType === "application/pdf" || origName.endsWith(".pdf");

    let movimientos = [];

    if (isXlsx) {
      // ── Parseo directo XLSX (Banco Provincia y similares) ────────
      // Formato: Fecha | Sucursal | Descripción | Referencia | Caja de Ahorro | CC | Saldo
      // CUIT siempre al final de Descripción: "De [nombre] / ... /[CUIT]"
      const wb   = XLSX.readFile(filePath);
      const sh   = wb.Sheets[wb.SheetNames[0]];
      const rows = XLSX.utils.sheet_to_json(sh, { header: 1, defval: "" });

      // Encontrar la fila de encabezado (contiene "Fecha" y "Descripción")
      let dataStart = 0;
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i].map(c => String(c).toLowerCase());
        if (row.some(c => c.includes("fecha")) && row.some(c => c.includes("descripci"))) {
          dataStart = i + 1;
          break;
        }
      }

      for (let i = dataStart; i < rows.length; i++) {
        const r    = rows[i];
        const desc = String(r[3] || "");
        // Solo transferencias recibidas con monto positivo
        const descLow = desc.toLowerCase();
        if (!descLow.includes("transf") || !descLow.includes("recibid")) continue;

        // Monto: columna Caja de Ahorro (idx 5)
        // Santander exporta con punto decimal estilo americano ("146645.00"), sin separador de miles.
        // Banco Provincia usa formato argentino ("1.234,56") con coma decimal.
        // → Si tiene coma: formato argentino (quitar puntos, reemplazar coma por punto).
        // → Si no tiene coma: formato americano, parseFloat directo.
        const cellVal  = String(r[5] || "0").trim();
        const monto    = cellVal.includes(",")
          ? parseFloat(cellVal.replace(/\./g, "").replace(",", "."))
          : parseFloat(cellVal);
        if (!monto || monto <= 0) continue;

        // Fecha: columna 1, formato DD/MM/YYYY → YYYY-MM-DD
        const fechaRaw = String(r[1] || "");
        let fecha = "";
        const fm = fechaRaw.match(/(\d{2})\/(\d{2})\/(\d{4})/);
        if (fm) fecha = `${fm[3]}-${fm[2]}-${fm[1]}`;

        // Nombre: después de "De " hasta el primer " /"
        let nombre = "";
        const nm = desc.match(/\bde\s+(.+?)\s*\//i);
        if (nm) nombre = nm[1].trim().replace(/\s+/g, " ");

        // CUIT: último token de solo dígitos al final de la descripción
        let cuit = null;
        const cm = desc.match(/\/\s*(\d{10,11})\s*$/);
        if (cm) cuit = cm[1];

        movimientos.push({ fecha, nombre, monto, descripcion: desc, cuit });
      }

      console.log(`✅ [Extracto XLSX] Movimientos entrantes: ${movimientos.length}`);

    } else if (isPdf) {
      // ── PDF: intento parseo directo (Santander) → fallback Gemini ──
      const fileBuffer = fs.readFileSync(filePath);
      let textoPdf = "";
      try { const p = await pdfParse(fileBuffer); textoPdf = p.text || ""; } catch {}

      // 1° intento: parser estructurado sin tokens
      if (textoPdf.trim().length > 50) {
        const directResult = parseSantanderPdfText(textoPdf);
        if (directResult.length > 0) {
          movimientos = directResult;
          console.log(`✅ [Extracto PDF directo] Movimientos encontrados: ${movimientos.length}`);
        }
      }

      // 2° fallback: Gemini (solo si el parser directo no encontró nada)
      if (movimientos.length === 0) {
        if (!geminiModel) { try { fs.unlinkSync(filePath); } catch {} return res.status(503).json({ ok: false, message: "IA no configurada (GEMINI_API_KEY)" }); }
        const PROMPT = `Sos un asistente contable argentino. Analizá este extracto bancario y devolvé ÚNICAMENTE un JSON válido con todos los MOVIMIENTOS DE CRÉDITO (transferencias entrantes, acreditaciones, depósitos).\n\nPara cada movimiento incluí:\n- fecha: formato YYYY-MM-DD\n- nombre: nombre completo del remitente\n- monto: número positivo sin símbolo ni puntos de miles\n- descripcion: descripción del movimiento\n- cuit: CUIT del remitente si aparece (null si no)\n\nIgnorá débitos, comisiones y movimientos negativos.\nDevolvé SOLO el JSON: {"movimientos":[{"fecha":"...","nombre":"...","monto":0,"descripcion":"...","cuit":null}]}`;

        let result;
        if (textoPdf.trim().length > 50) {
          result = await geminiModel.generateContent(`${PROMPT}\n\nEXTRACTO BANCARIO:\n${textoPdf}`);
        } else {
          const b64 = fileBuffer.toString("base64");
          result = await geminiModel.generateContent([PROMPT, { inlineData: { data: b64, mimeType: "application/pdf" } }]);
        }
        const raw    = result.response.text().trim().replace(/```json|```/gi, "").trim();
        const parsed = JSON.parse(raw);
        movimientos  = Array.isArray(parsed.movimientos) ? parsed.movimientos : [];
        console.log(`✅ [Extracto PDF Gemini] Movimientos encontrados: ${movimientos.length}`);
      }

    } else {
      // ── Imagen (JPG, PNG, WEBP) → Gemini ────────────────────────
      if (!geminiModel) { try { fs.unlinkSync(filePath); } catch {} return res.status(503).json({ ok: false, message: "IA no configurada (GEMINI_API_KEY)" }); }
      const PROMPT = `Sos un asistente contable argentino. Analizá este extracto bancario y devolvé ÚNICAMENTE un JSON válido con todos los MOVIMIENTOS DE CRÉDITO.\n\nPara cada movimiento:\n- fecha: YYYY-MM-DD\n- nombre: nombre del remitente\n- monto: número positivo\n- descripcion: descripción\n- cuit: CUIT si aparece (null si no)\n\nIgnorá débitos. JSON: {"movimientos":[{"fecha":"...","nombre":"...","monto":0,"descripcion":"...","cuit":null}]}`;
      const b64    = fs.readFileSync(filePath).toString("base64");
      const result = await geminiModel.generateContent([PROMPT, { inlineData: { data: b64, mimeType } }]);
      const raw    = result.response.text().trim().replace(/```json|```/gi, "").trim();
      const parsed = JSON.parse(raw);
      movimientos  = Array.isArray(parsed.movimientos) ? parsed.movimientos : [];
    }

    // Limpiar archivo temporal
    try { fs.unlinkSync(filePath); } catch {}

    // Normalizar y filtrar chinos
    const todas = movimientos.map(m => ({
      fecha:       String(m.fecha || ""),
      nombre:      String(m.nombre || ""),
      monto:       Math.abs(Number(String(m.monto || "0").replace(/\./g, "").replace(",", "."))),
      descripcion: String(m.descripcion || ""),
      cuit:        m.cuit ? onlyDigits(String(m.cuit)) : null,
      esChino:     detectarNombreChino(m.nombre)
    })).filter(m => m.monto > 0);

    const chinos = todas.filter(m => m.esChino);

    console.log(`✅ [Extracto] Total: ${todas.length} | Clientes chinos: ${chinos.length}`);

    return res.json({
      ok: true,
      total: todas.length,
      detectados: chinos.length,
      transferencias: chinos,
      todas: DEBUG ? todas : undefined
    });

  } catch (err) {
    console.error("❌ [/procesar-extracto]", err?.message || err);
    return res.status(500).json({ ok: false, message: err?.message || "Error al procesar extracto" });
  }
});

// ── POST /facturar-extracto ─────────────────────────────────────
// Recibe array de transferencias y emite una factura por cada una.
// Genera items automáticamente con buildItemsParaMonto (60% Make / 40% otros).
// Devuelve resumen consolidado y envía email.
app.post("/facturar-extracto", async (req, res) => {
  try {
    const transferencias = Array.isArray(req.body.transferencias) ? req.body.transferencias : [];
    if (transferencias.length === 0) return res.status(400).json({ ok: false, message: "Sin transferencias" });

    const condicionVenta = String(req.body.condicionVenta || "Transferencia Bancaria");
    const emailReporte   = String(req.body.emailReporte || "santamariapablodaniel@gmail.com");
    const fecha = todayISO();
    const cbteFch = yyyymmdd(fecha);
    const pv = await getPtoVentaSeguro();

    const resultados = [];
    let totalFacturado = 0;
    let errores = 0;

    for (const t of transferencias) {
      const cuitCliente = onlyDigits(String(t.cuit || ""));
      const monto = Math.abs(Number(t.monto || 0));
      const nombre = String(t.nombre || "Cliente");

      if (cuitCliente.length !== 11 || monto <= 0) {
        resultados.push({ ok: false, nombre, cuit: cuitCliente, monto, error: "CUIT inválido o monto cero" });
        errores++;
        continue;
      }

      try {
        const items = buildItemsParaMonto(monto);

        // Calcular importes fiscales
        const impTotal = round2(items.reduce((a, x) => a + Number(x.subtotalConIva || 0), 0));
        const impNeto  = round2(impTotal / 1.21);
        const impIVA   = round2(impTotal - impNeto);

        const rec = await getReceptorDesdePadron(cuitCliente);
        const nro = (await afip.ElectronicBilling.getLastVoucher(pv, CBTE_TIPO_REAL)) + 1;

        const voucherData = {
          CantReg: 1, PtoVta: pv, CbteTipo: CBTE_TIPO_REAL, Concepto: 1,
          DocTipo: 80, DocNro: Number(cuitCliente),
          CbteDesde: nro, CbteHasta: nro, CbteFch: cbteFch,
          ImpTotal: impTotal, ImpTotConc: 0, ImpNeto: impNeto,
          ImpOpEx: 0, ImpIVA: impIVA, ImpTrib: 0,
          MonId: "PES", MonCotiz: 1,
          Iva: [{ Id: 5, BaseImp: impNeto, Importe: impIVA }]
        };

        const afipResult = await afip.ElectronicBilling.createVoucher(voucherData);

        const qrPayload = {
          ver: 1, fecha, cuit: CUIT_DISTRIBUIDORA, ptoVta: pv,
          tipoCmp: CBTE_TIPO_REAL, nroCmp: nro, importe: impTotal,
          moneda: "PES", ctz: 1, tipoDocRec: 80, nroDocRec: Number(cuitCliente),
          tipoCodAut: "E", codAut: Number(afipResult.CAE)
        };
        const qrDataUrl = await QRCode.toDataURL(
          `https://www.arca.gob.ar/fe/qr/?p=${Buffer.from(JSON.stringify(qrPayload)).toString("base64")}`,
          { margin: 0, width: 170 }
        );

        const itemsCalc = items.map(it => ({
          ...it,
          subtotalNeto: round2(Number(it.subtotalConIva) / 1.21),
          precioNeto: round2((Number(it.subtotalConIva) / 1.21) / it.cantidad)
        }));

        const htmlPDF = buildFacturaHtml({
          receptor: { cuit: cuitCliente, nombre: rec.nombre, condicionIVA: rec.condicionIVA, domicilioAfip: rec.domicilioAfip, domicilioRemito: "" },
          fechaISO: fecha, pv, nro, items: itemsCalc,
          neto: impNeto, iva: impIVA, total: impTotal,
          cae: afipResult.CAE, caeVtoISO: afipResult.CAEFchVto,
          condicionVenta, qrDataUrl, isPreview: false
        });

        const pdfRes = await afip.ElectronicBilling.createPDF({
          html: htmlPDF, file_name: `FA_${pad(pv, 5)}-${pad(nro, 8)}`,
          options: { width: 8.27, marginTop: 0.35, marginBottom: 0.35, marginLeft: 0.35, marginRight: 0.35 }
        });

        let pdfBuffer = null;
        try {
          await new Promise(r => setTimeout(r, 1500));
          pdfBuffer = await downloadToBuffer(pdfRes.file);
        } catch {}

        let pdfPublicUrl = "";
        try {
          pdfPublicUrl = pdfBuffer?.length
            ? await savePublicPdf(pdfBuffer, `FA_${pad(pv, 5)}-${pad(nro, 8)}`)
            : String(pdfRes.file || "");
        } catch { pdfPublicUrl = String(pdfRes.file || ""); }

        const comprobante = `M-${pad(pv, 5)}-${pad(nro, 8)}`;

        await guardarComprobanteGeneralEnDB({
          comprobante, cbteTipo: CBTE_TIPO_REAL,
          cuitCliente, nombreCliente: rec.nombre, domicilio: rec.domicilioAfip || "",
          nro, pv, cae: afipResult.CAE, impTotal,
          pdfPublicUrl, condicionVenta: `${condicionVenta} · EXTRACTO`,
          fecha, items, emailAEnviar: DEFAULT_EMAIL
        });

        totalFacturado = round2(totalFacturado + impTotal);
        resultados.push({
          ok: true, nombre: rec.nombre, cuit: cuitCliente,
          comprobante, cae: afipResult.CAE, total: impTotal, pdfUrl: pdfPublicUrl
        });
        console.log(`✅ [Extracto] Factura emitida: ${comprobante} | CUIT ${cuitCliente} | $${impTotal}`);

      } catch (err) {
        console.error(`❌ [Extracto] Error en CUIT ${cuitCliente}:`, err?.message);
        resultados.push({ ok: false, nombre, cuit: cuitCliente, monto, error: err?.message || "Error AFIP" });
        errores++;
      }
    }

    // ── Enviar email con resumen consolidado ─────────────────────
    const ok_results = resultados.filter(r => r.ok);
    const htmlReporte = `
      <div style="font-family:'Segoe UI',Helvetica,Arial,sans-serif;max-width:620px;margin:0 auto;color:#1a1a1a;">
        <div style="background:#0f172a;padding:28px 24px;border-radius:12px 12px 0 0;text-align:center;">
          <h1 style="color:#fff;margin:0;font-size:22px;">Mercado Limpio</h1>
          <p style="color:#94a3b8;margin:6px 0 0;font-size:13px;text-transform:uppercase;letter-spacing:1px;">Reporte de Facturación — Extracto Bancario</p>
        </div>
        <div style="background:#f8fafc;padding:24px;border:1px solid #e2e8f0;border-top:none;">
          <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:24px;">
            <div style="background:#fff;border-radius:10px;padding:16px;text-align:center;border:1px solid #e2e8f0;">
              <div style="font-size:28px;font-weight:900;color:#1C1C1E;">${ok_results.length}</div>
              <div style="font-size:11px;color:#8E8E93;font-weight:700;text-transform:uppercase;margin-top:4px;">Facturas</div>
            </div>
            <div style="background:linear-gradient(135deg,#34C759,#248A3D);border-radius:10px;padding:16px;text-align:center;">
              <div style="font-size:18px;font-weight:900;color:#fff;">$${formatMoneyAR(totalFacturado)}</div>
              <div style="font-size:11px;color:rgba(255,255,255,0.8);font-weight:700;text-transform:uppercase;margin-top:4px;">Total</div>
            </div>
            <div style="background:#fff;border-radius:10px;padding:16px;text-align:center;border:1px solid #e2e8f0;">
              <div style="font-size:28px;font-weight:900;color:${errores > 0 ? "#FF3B30" : "#34C759"};">${errores}</div>
              <div style="font-size:11px;color:#8E8E93;font-weight:700;text-transform:uppercase;margin-top:4px;">Errores</div>
            </div>
          </div>
          <h3 style="font-size:13px;color:#64748b;text-transform:uppercase;letter-spacing:0.5px;margin:0 0 12px;">Detalle de comprobantes</h3>
          ${ok_results.map(r => `
            <div style="background:#fff;border-radius:10px;padding:14px 16px;margin-bottom:8px;border:1px solid #e2e8f0;display:flex;justify-content:space-between;align-items:center;">
              <div>
                <div style="font-weight:800;font-size:14px;">${safeText(r.nombre)}</div>
                <div style="font-size:12px;color:#64748b;margin-top:2px;">CUIT: ${r.cuit} · ${r.comprobante}</div>
                <div style="font-size:11px;color:#94a3b8;font-family:monospace;margin-top:1px;">CAE: ${r.cae}</div>
              </div>
              <div style="font-weight:900;font-size:15px;color:#1C1C1E;text-align:right;">
                $${formatMoneyAR(r.total)}
                ${r.pdfUrl ? `<br><a href="${r.pdfUrl}" style="font-size:11px;color:#007AFF;font-weight:600;">PDF ↗</a>` : ""}
              </div>
            </div>`).join("")}
          ${errores > 0 ? `
            <div style="margin-top:16px;padding:12px 16px;background:#FFF1F0;border-radius:10px;border:1px solid #FFD6D2;">
              <strong style="color:#FF3B30;font-size:13px;">Transferencias con error (${errores}):</strong>
              ${resultados.filter(r => !r.ok).map(r => `<div style="font-size:12px;color:#64748b;margin-top:6px;">
                ${safeText(r.nombre)} · CUIT: ${r.cuit} · $${formatMoneyAR(r.monto)} — ${safeText(r.error)}
              </div>`).join("")}
            </div>` : ""}
          <p style="font-size:12px;color:#94a3b8;text-align:center;margin-top:24px;">
            Fecha de procesamiento: ${fecha} · Mercado Limpio Distribuidora
          </p>
        </div>
      </div>`;

    try {
      if (!resendClient) throw new Error("Resend no configurado");
      const { error: extractoError } = await resendClient.emails.send({
        from: `"${EMISOR.nombreVisible}" <ventas@mercadolimpio.ar>`,
        to: emailReporte,
        reply_to: GMAIL_USER,
        subject: `📊 Extracto procesado — ${ok_results.length} facturas · $${formatMoneyAR(totalFacturado)}`,
        html: htmlReporte
      });
      if (extractoError) throw new Error(`Resend error: ${extractoError.message || JSON.stringify(extractoError)}`);
      console.log(`✅ [Extracto] Email reporte enviado a ${emailReporte}`);
    } catch (mailErr) {
      console.error("⚠️ [Extracto] No se pudo enviar email reporte:", mailErr?.message);
    }

    return res.json({
      ok: true,
      totalFacturado,
      facturasEmitidas: ok_results.length,
      errores,
      resultados
    });

  } catch (err) {
    console.error("❌ [/facturar-extracto]", err?.message || err);
    return res.status(500).json({ ok: false, message: err?.message || "Error al facturar extracto" });
  }
});

// ================================================================
// ✉️  MAILER PROVEEDORES
// ================================================================
const MAILER_SECRET      = process.env.MAILER_SECRET || "";
const MAILER_FROM_EMAIL  = process.env.MAILER_FROM_EMAIL  || "ventas@mercadolimpio.ar";
const MAILER_FROM_NAME   = process.env.MAILER_FROM_NAME   || "Mercado Limpio";
const MAILER_REPLY_TO    = process.env.MAILER_REPLY_TO    || GMAIL_USER || "distribuidoramercadolimpio@gmail.com";
const MAILER_CONTACTS_PATH = "mailer/contacts.json";

// Helpers para persistir contactos en Supabase Storage
async function loadMailerContacts() {
  if (!supabase) return [];
  try {
    const { data, error } = await supabase.storage
      .from(SUPABASE_STORAGE_BUCKET)
      .download(MAILER_CONTACTS_PATH);
    if (error || !data) return [];
    const text = await data.text();
    return JSON.parse(text);
  } catch { return []; }
}

async function saveMailerContacts(contacts) {
  if (!supabase) return;
  const blob = new Blob([JSON.stringify(contacts, null, 2)], { type: "application/json" });
  await supabase.storage
    .from(SUPABASE_STORAGE_BUCKET)
    .upload(MAILER_CONTACTS_PATH, blob, { upsert: true, contentType: "application/json" });
}

async function addMailerContact(email) {
  const e = email.trim().toLowerCase();
  let contacts = await loadMailerContacts();
  contacts = [e, ...contacts.filter(c => c !== e)].slice(0, 200);
  await saveMailerContacts(contacts);
}

// Middleware de autenticación del mailer (X-Mailer-Secret header)
function mailerAuth(req, res, next) {
  if (!MAILER_SECRET) return next(); // sin secret configurado = abierto (solo para desarrollo)
  const header = req.headers["x-mailer-secret"] || "";
  if (header !== MAILER_SECRET) {
    return res.status(401).json({ ok: false, error: "No autorizado" });
  }
  next();
}

// GET /mailer/contacts
app.get("/mailer/contacts", mailerAuth, async (_req, res) => {
  try {
    const contacts = await loadMailerContacts();
    res.json(contacts);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// POST /mailer/send
app.post("/mailer/send", mailerAuth, mailerUpload.array("archivos", 20), async (req, res) => {
  try {
    if (!resendClient) return res.status(500).json({ ok: false, error: "Resend no configurado en el worker" });

    const { to, subject, message } = req.body;
    if (!to || !subject || !message) {
      return res.status(400).json({ ok: false, error: "Faltan campos: to, subject, message" });
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const destinatarios = to.split(",").map(e => e.trim()).filter(Boolean);
    for (const email of destinatarios) {
      if (!emailRegex.test(email)) {
        return res.status(400).json({ ok: false, error: `Email inválido: ${email}` });
      }
    }

    const attachments = (req.files || []).map(file => ({
      filename: file.originalname,
      content: file.buffer
    }));

    const htmlBody = `
      <div style="font-family:Arial,sans-serif;font-size:15px;color:#1e293b;line-height:1.6;max-width:680px">
        ${message
          .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;")
          .replace(/\n/g,"<br>")}
        <br><br>
        <hr style="border:none;border-top:1px solid #e2e8f0;margin:24px 0">
        <p style="font-size:12px;color:#94a3b8;margin:0">
          ${MAILER_FROM_NAME} · Buenos Aires, Argentina<br>
          Para responder a este email, usá Responder directamente.
        </p>
      </div>`;

    console.log(`📤 [Mailer] → ${destinatarios.join(", ")} | ${subject} | adjuntos: ${attachments.length}`);

    const { data, error } = await resendClient.emails.send({
      from:        `"${MAILER_FROM_NAME}" <${MAILER_FROM_EMAIL}>`,
      to:          destinatarios,
      reply_to:    MAILER_REPLY_TO,
      subject,
      html:        htmlBody,
      attachments
    });

    if (error) {
      console.error("❌ [Mailer] Resend error:", error);
      return res.status(500).json({ ok: false, error: error.message || JSON.stringify(error) });
    }

    // Guardar contactos de forma asíncrona (no bloquea la respuesta)
    destinatarios.forEach(e => addMailerContact(e).catch(() => {}));

    console.log(`✅ [Mailer] Enviado | id=${data?.id}`);
    res.json({ ok: true, id: data?.id });

  } catch (err) {
    console.error("❌ [Mailer]", err?.message || err);
    res.status(500).json({ ok: false, error: err.message });
  }
});
