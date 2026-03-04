"use strict";

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

require("dotenv").config();

const app = express();
app.use(express.json({ limit: "50mb" }));
app.use(cors());

const APP_VERSION = "2026-02-26.GEMINI-MOTOR-DOBLE-SMART-LOGS.DOMFIX.DESC.FINAL";
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

// ✅ Pediste 25 por factura:
const ITEMS_POR_FACTURA = 25;

// ✅ URL pública base (para links de WhatsApp)
const PUBLIC_URL = String(process.env.PUBLIC_URL || "https://api-mercadolimpio.onrender.com").replace(/\/+$/, "");

// ✅ (Opcional) Si algún día habilitás A10, poné ENABLE_PADRON_10=true
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
  secure: false,
  auth: { user: GMAIL_USER, pass: GMAIL_APP_PASS },
});
const uploadDir = path.join(process.cwd(), "uploads");
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });

const publicPdfDir = path.join(uploadDir, "public_pdfs");
if (!fs.existsSync(publicPdfDir)) fs.mkdirSync(publicPdfDir, { recursive: true });

// ✅ servimos PDFs como archivos públicos
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

function savePublicPdf(buffer, baseNameNoExt) {
  const stamp = Date.now().toString(36) + "_" + crypto.randomBytes(4).toString("hex");
  const fname = `${safeFileName(baseNameNoExt)}_${stamp}.pdf`;
  const fpath = path.join(publicPdfDir, fname);
  fs.writeFileSync(fpath, buffer);
  return `${PUBLIC_URL}/public_pdfs/${fname}`;
}

// ✅ limpieza de PDFs públicos viejos (por default 7 días)
function cleanupOldPublicPdfs(maxAgeMs = 7 * 24 * 60 * 60 * 1000) {
  try {
    const now = Date.now();
    const files = fs.readdirSync(publicPdfDir);
    for (const f of files) {
      if (!String(f).toLowerCase().endsWith(".pdf")) continue;
      const fp = path.join(publicPdfDir, f);
      const st = fs.statSync(fp);
      if (now - st.mtimeMs > maxAgeMs) {
        try { fs.unlinkSync(fp); } catch {}
      }
    }
  } catch {}
}
setInterval(() => cleanupOldPublicPdfs(), 6 * 60 * 60 * 1000).unref?.();

const upload = multer({ dest: uploadDir, limits: { fileSize: 50 * 1024 * 1024 } });

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
// ✅ PADRÓN: A13 + Constancia (A10 opcional y OFF por default)
// ============================
function buildDomicilioString(df) {
  if (!df) return "";

  const calle = df.calle || df.direccion || df.domicilio || df.nombreCalle || df.street || "";
  const nro = df.numero || df.nro || df.num || df.numeroCalle || "";
  const piso = df.piso || "";
  const dpto = df.departamento || df.dpto || "";
  const loc = df.localidad || df.descripcionLocalidad || "";
  const prov = df.descripcionProvincia || df.provincia || df.descProvincia || "";
  const cp = df.codPostal || df.codigoPostal || df.cp || "";

  const linea1 = [calle, nro].filter(Boolean).join(" ").trim();
  const linea2 = [piso ? `Piso ${piso}` : "", dpto ? `Dto ${dpto}` : ""].filter(Boolean).join(" ").trim();

  const out = [
    linea1,
    linea2,
    loc,
    prov,
    cp ? `CP: ${cp}` : ""
  ].filter(Boolean).join(" - ");

  return String(out || "").replace(/\s{2,}/g, " ").trim();
}

function normalizePadronDetails(padron, cuitCliente) {
  const base = { nombre: `CUIT ${cuitCliente}`, domicilioAfip: "", condicionIVA: "IVA Responsable Inscripto" };
  if (!padron) return base;

  const dg = padron?.datosGenerales || padron;
  const nombre =
    (dg ? [dg.apellido, dg.nombre].filter(Boolean).join(" ").trim() : "") ||
    dg?.razonSocial ||
    dg?.denominacion ||
    dg?.nombre ||
    dg?.razonSocialNombre ||
    "";

  // Domicilio fiscal típico A13
  const df =
    padron?.datosGenerales?.domicilioFiscal ||
    padron?.domicilioFiscal ||
    dg?.domicilioFiscal ||
    dg?.domicilio ||
    padron?.domicilio ||
    null;

  let domicilio = buildDomicilioString(df);

  // Algunos servicios devuelven array de domicilios
  if (!domicilio) {
    const arr = padron?.datosGenerales?.domicilios || padron?.domicilios || dg?.domicilios || [];
    if (Array.isArray(arr) && arr.length) domicilio = buildDomicilioString(arr[0]);
  }

  return {
    nombre: nombre || base.nombre,
    domicilioAfip: domicilio || "",
    condicionIVA: "IVA Responsable Inscripto"
  };
}

// Cache simple por CUIT (evita llamar padrones 10 veces seguidas)
const padronCache = new Map(); // cuit -> {data, exp}
const PADRON_TTL_MS = 12 * 60 * 60 * 1000; // 12h

async function getReceptorDesdePadron(cuitCliente) {
  const cuitStr = onlyDigits(cuitCliente);
  const cuitNum = Number(cuitStr);
  const base = { nombre: `CUIT ${cuitCliente}`, domicilioAfip: "", condicionIVA: "IVA Responsable Inscripto" };

  if (!cuitStr || cuitStr.length !== 11) return base;

  const cached = padronCache.get(cuitStr);
  if (cached && Date.now() < cached.exp) return cached.data;

  // 1) A13
  try {
    const p13 = await afip.RegisterScopeThirteen.getTaxpayerDetails(cuitNum);
    const r13 = normalizePadronDetails(p13, cuitCliente);
    if (r13.nombre && r13.nombre !== base.nombre) base.nombre = r13.nombre;
    if (r13.domicilioAfip) {
      padronCache.set(cuitStr, { data: r13, exp: Date.now() + PADRON_TTL_MS });
      return r13;
    }
  } catch (e) {
    if (DEBUG) errlog("PADRON A13 error:", e?.message || e);
  }

  // 2) A10 (opcional, OFF por default)
  if (ENABLE_PADRON_10 && afip.RegisterScopeTen?.getTaxpayerDetails) {
    try {
      const p10 = await afip.RegisterScopeTen.getTaxpayerDetails(cuitNum);
      const r10 = normalizePadronDetails(p10, cuitCliente);
      if (r10.nombre && r10.nombre !== base.nombre) base.nombre = r10.nombre;
      if (r10.domicilioAfip) {
        padronCache.set(cuitStr, { data: r10, exp: Date.now() + PADRON_TTL_MS });
        return r10;
      }
    } catch (e) {
      if (DEBUG) errlog("PADRON A10 error:", e?.message || e);
    }
  }

  // 3) Constancia de inscripción (último intento)
  // OJO: el SDK puede exponer esto con otro nombre según versión, por eso lo hacemos flexible
  try {
    const svc =
      afip.RegisterInscriptionProof ||
      afip.InscriptionProof ||
      afip.RegistrationProof ||
      null;

    if (svc && typeof svc.getTaxpayerDetails === "function") {
      const pc = await svc.getTaxpayerDetails(cuitNum);
      const rc = normalizePadronDetails(pc, cuitCliente);
      if (rc.nombre && rc.nombre !== base.nombre) base.nombre = rc.nombre;
      if (rc.domicilioAfip) {
        padronCache.set(cuitStr, { data: rc, exp: Date.now() + PADRON_TTL_MS });
        return rc;
      }
    }
  } catch (e) {
    if (DEBUG) errlog("CONSTANCIA error:", e?.message || e);
  }

  padronCache.set(cuitStr, { data: base, exp: Date.now() + PADRON_TTL_MS });
  return base;
}

// === TEXTO PDF ===
function normalizePdfText(texto) {
  return String(texto || "")
    .replace(/\u00A0/g, " ")
    .replace(/\t/g, " ")
    .replace(/\r/g, "")
    .replace(/[ ]{2,}/g, " ")
    .trim();
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
// ✅ DESCUENTO GLOBAL + TOTALES (ROBUSTO)
// ============================
const MONEY_RX = "\\d{1,3}(?:\\.\\d{3})*(?:,\\d{2})";

function extractDocPricingSummary(texto) {
  const lines = String(texto || "").split(/\n/).map(l => l.trim()).filter(Boolean);
  const tail = lines.slice(-180);

  const rxMoneyOnlyLine = new RegExp(`^(${MONEY_RX})$`);

  // 1) Montos “solos” del pie (ej: 426.836,45)
  const moneyOnly = [];
  for (let i = 0; i < tail.length; i++) {
    const ln = tail[i];
    const mm = ln.match(rxMoneyOnlyLine);
    if (mm) moneyOnly.push({ v: parseMoneyArToNumber(mm[1]), pos: i });
  }

  // 2) Descuento global: "7 % 32.127,47" y "32.127,47 7 %"
  let descuentoPct = 0;
  let descuentoImporte = 0;
  let discStrategy = "none";

  const rxPctFirst = new RegExp(`^\\s*(\\d{1,2}(?:[\\.,]\\d{1,2})?)\\s*%\\s*(${MONEY_RX})\\s*$`);
  const rxAmtFirst = new RegExp(`^\\s*(${MONEY_RX})\\s*(\\d{1,2}(?:[\\.,]\\d{1,2})?)\\s*%\\s*$`);

  const discCandidates = [];

  for (const lnRaw of tail) {
    const ln = String(lnRaw || "").trim();
    if (!ln.includes("%")) continue;
    if (ln.includes("(")) continue;          // ignora "(0,00 %)" ítems
    if (/saldo/i.test(ln)) continue;         // ignora saldo
    if (ln.length > 60) continue;

    let m = ln.match(rxPctFirst);
    if (m) {
      const pct = Number(String(m[1]).replace(",", "."));
      const imp = parseMoneyArToNumber(m[2]);
      if (pct > 0 && pct < 80 && imp > 0) discCandidates.push({ pct, imp, why: "pct_first" });
      continue;
    }

    m = ln.match(rxAmtFirst);
    if (m) {
      const imp = parseMoneyArToNumber(m[1]);
      const pct = Number(String(m[2]).replace(",", "."));
      if (pct > 0 && pct < 80 && imp > 0) discCandidates.push({ pct, imp, why: "amt_first" });
      continue;
    }

    // fallback mixto
    const mPct = ln.match(/(\d{1,2}(?:[.,]\d{1,2})?)\s*%/);
    const mAmt = ln.match(new RegExp(`(${MONEY_RX})`));
    if (mPct && mAmt) {
      const pct = Number(String(mPct[1]).replace(",", "."));
      const imp = parseMoneyArToNumber(mAmt[1]);
      if (pct > 0 && pct < 80 && imp > 0) discCandidates.push({ pct, imp, why: "fallback_mixed" });
    }
  }

  if (discCandidates.length) {
    discCandidates.sort((a, b) => b.imp - a.imp);
    descuentoPct = round2(discCandidates[0].pct);
    descuentoImporte = round2(discCandidates[0].imp);
    discStrategy = discCandidates[0].why;
  }

  // 3) Subtotal bruto: último monto “solo”
  let subtotalBruto = moneyOnly.length ? moneyOnly[moneyOnly.length - 1].v : 0;
  let subStrategy = moneyOnly.length ? "tail_last_money_only" : "none";

  // 4) Total final post-desc
  let totalFinal = 0;
  let totStrategy = "none";
  let verified = false;

  const findNear = (arr, target, tol = 0.06) => arr.find(x => Math.abs(x.v - target) <= tol);

  if (subtotalBruto > 0 && descuentoImporte > 0) {
    const expected = round2(subtotalBruto - descuentoImporte);
    const found = findNear(moneyOnly, expected);
    if (found) {
      totalFinal = found.v;
      totStrategy = "subtotal_minus_desc_found_in_tail";
      verified = true;
    } else {
      const maxMoney = moneyOnly.reduce((m, x) => Math.max(m, x.v), 0);
      const expected2 = round2(maxMoney - descuentoImporte);
      const found2 = findNear(moneyOnly, expected2);
      if (found2) {
        subtotalBruto = maxMoney;
        subStrategy = "tail_max_money_only";
        totalFinal = found2.v;
        totStrategy = "max_minus_desc_found_in_tail";
        verified = true;
      } else {
        totalFinal = expected;
        totStrategy = "subtotal_minus_desc_calc_only";
        verified = false;
      }
    }
  } else if (moneyOnly.length) {
    // sin dto: buscamos repetido o último
    const vals = moneyOnly.map(x => x.v).sort((a, b) => a - b);
    const groups = [];
    for (const v of vals) {
      const g = groups[groups.length - 1];
      if (!g || Math.abs(g.v - v) > 0.05) groups.push({ v, n: 1 });
      else g.n++;
    }
    const dup = groups.filter(g => g.n >= 2).sort((a, b) => (b.n - a.n) || (b.v - a.v))[0];
    if (dup) {
      totalFinal = dup.v;
      totStrategy = "dup_money_only";
      verified = true;
    } else {
      totalFinal = subtotalBruto;
      totStrategy = "no_discount_total_equals_subtotal";
      verified = true;
    }
  }

  return {
    subtotalBruto,
    descuentoPct,
    descuentoImporte,
    totalFinal,
    verified,
    strategies: { discStrategy, subStrategy, totStrategy }
  };
}

function sumItemsBruto(items) {
  return round2((items || []).reduce((acc, it) => {
    const sub = Number(it.subtotalConIva || 0);
    if (sub > 0) return acc + sub;
    const c = Number(it.cantidad || 0);
    const p = Number(it.precioConIva || 0);
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
    const fixedSub = round2(Number(last.subtotalConIva || 0) + diff);
    last.subtotalConIva = fixedSub;
    last.precioConIva = round2(fixedSub / qty);
  }

  return out;
}

// ============================
// ✅ SMART DOMICILIO + LOCALIDAD
// ============================
function escapeRegExp(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function extractDomicilioLocalidadSmart(texto) {
  const raw = String(texto || "");
  const lines = raw.split(/\n/).map(l => l.trim()).filter(Boolean);

  const head = lines.slice(0, 40);
  const tail = lines.slice(-160);

  let domicilio = "";
  let domStrategy = "none";

  for (const ln of head) {
    if (/\bCP\s*:/i.test(ln) && /[A-Za-zÁÉÍÓÚÑáéíóúñüÜ]/.test(ln) && /\d/.test(ln)) {
      if (/(Saldo|Subtotal|Total)\b/i.test(ln)) continue;
      domicilio = ln;
      domStrategy = "head_cp";
      break;
    }
  }

  domicilio = String(domicilio || "")
    .replace(/\bCP\s*:\s*\d+\b/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();

  let localidad = "";
  let locStrategy = "none";

  for (let i = tail.length - 1; i >= 0; i--) {
    const ln = tail[i];
    if (/^[A-Za-zÁÉÍÓÚÑáéíóúñüÜ ]{4,}$/.test(ln)) {
      localidad = ln.trim();
      locStrategy = "tail_plain_text";
      break;
    }
  }

  const domicilioRemito = [domicilio, localidad].filter(Boolean).join(", ").trim();

  if (DEBUG) {
    log("🏠 [SMART] domicilio:", { domicilio, domStrategy });
    log("🏙️ [SMART] localidad:", { localidad, locStrategy });
    log("✅ [SMART] domicilioRemito final:", domicilioRemito);
  }

  return { domicilio, localidad, domicilioRemito, domStrategy, locStrategy };
}

// IA 1: cabecera
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

    let domLimpio = String(parsed.domicilioRemito || "")
      .replace(/CP:\s*0/gi, "")
      .replace(/\s{2,}/g, " ")
      .replace(/\s+,/g, ",")
      .trim();

    return { cuitCliente: parsed.cuitCliente || "", domicilioRemito: domLimpio };
  } catch {
    return { cuitCliente: "", domicilioRemito: "" };
  }
}

function splitQtyUnitFromMoneyToken(tokenMoney, subtotal) {
  // Ej: "121459,95" puede ser "12" + "1459,95" y debe dar ~subtotal
  const t = String(tokenMoney || "").replace(/\./g, "").trim();
  const m = t.match(/^(\d+),(\d{2})$/);
  if (!m) return null;

  const intPart = m[1];
  const decPart = m[2];

  let best = null;
  const maxQtyLen = Math.min(4, intPart.length - 1); // qty 1..9999, deja al menos 1 dígito para unit
  for (let qtyLen = 1; qtyLen <= maxQtyLen; qtyLen++) {
    const qtyStr = intPart.slice(0, qtyLen);
    const unitStr = intPart.slice(qtyLen);
    if (!unitStr) continue;

    const qty = Number(qtyStr);
    if (!Number.isInteger(qty) || qty <= 0 || qty > 5000) continue;

    const unit = parseMoneyArToNumber(unitStr + "," + decPart);
    if (!(unit > 0)) continue;

    const calc = round2(qty * unit);
    const diff = Math.abs(calc - subtotal);
    const tol = Math.max(2.0, subtotal * 0.003); // $2 o 0.3%

    if (diff <= tol && (!best || diff < best.diff)) best = { qty, unit, diff };
  }

  return best ? { qty: best.qty, unit: best.unit } : null;
}

function extractItemsSmartRegex(texto) {
  const lines = String(texto || "").split(/\n/).map(l => l.trim()).filter(Boolean);
  const items = [];

  const rxMoney = new RegExp(MONEY_RX, "g");

  for (const ln0 of lines) {
    // Item lines típicas: contienen "(0,00 %)" u otro % y tienen texto
    if (!ln0.includes("%")) continue;
    if (!/[A-Za-zÁÉÍÓÚÑáéíóúñüÜ]/.test(ln0)) continue;

    // excluir footer/header
    if (/(saldo\s+actual|saldo\s+anterior|subtotal\b|total\b|observaciones|firma|fecha\s+vto)/i.test(ln0)) continue;

    // Descripción: preferimos el texto luego del ")"
    let desc = "";
    const idxClose = ln0.lastIndexOf(")");
    if (idxClose >= 0 && idxClose < ln0.length - 1) {
      const after = ln0.slice(idxClose + 1).trim();
      if (/[A-Za-zÁÉÍÓÚÑáéíóúñüÜ]/.test(after)) desc = after;
    }

    if (!desc) {
      // fallback: limpiamos números/moneda/parentesis y dejamos letras
      desc = ln0
        .replace(/\(.*?\)/g, " ")
        .replace(rxMoney, " ")
        .replace(/\b\d+\b/g, " ")
        .replace(/[^\p{L}\s]/gu, " ")
        .replace(/\s{2,}/g, " ")
        .trim();
    }
    if (!desc) continue;

    // Quitamos "(0,00 %)" para no meter 0,00 en los money tokens
    const ln = ln0.replace(/\(.*?\)/g, " ").replace(/\s{2,}/g, " ").trim();

    const moneyTokens = ln.match(rxMoney) || [];
    if (moneyTokens.length < 2) continue;

    const subtotal = parseMoneyArToNumber(moneyTokens[moneyTokens.length - 1]);
    if (!(subtotal > 0)) continue;

    let qty = 0;
    let unit = 0;

    // Caso A: línea normal "24 Cabo madera 800,00 ... 19.200,00"
    const lead = ln.match(/^(\d{1,5})\s/);
    if (lead) {
      const q = Number(lead[1]);
      if (q > 0 && q <= 5000) {
        qty = q;

        // Elegimos unit entre tokens (menos el subtotal) el que mejor cierre qty*unit=subtotal
        let bestU = null;
        for (let j = 0; j < moneyTokens.length - 1; j++) {
          const u = parseMoneyArToNumber(moneyTokens[j]);
          if (!(u > 0)) continue;

          const calc = round2(qty * u);
          const diff = Math.abs(calc - subtotal);
          const tol = Math.max(2.0, subtotal * 0.003);

          if (diff <= tol && (!bestU || diff < bestU.diff)) bestU = { u, diff };
        }
        if (bestU) unit = bestU.u;
      }
    }

    // Caso B: línea "pegada" "121459,9517.519,42 ... )Cabo de metal"
    if (!qty || !unit) {
      const split = splitQtyUnitFromMoneyToken(moneyTokens[0], subtotal);
      if (split) {
        qty = split.qty;
        unit = split.unit;
      }
    }

    // Caso C: inferir qty a partir de unit candidato
    if (!qty || !unit) {
      for (let j = 0; j < moneyTokens.length - 1 && (!qty || !unit); j++) {
        const u = parseMoneyArToNumber(moneyTokens[j]);
        if (!(u > 0)) continue;

        const q = Math.round(subtotal / u);
        const tol = Math.max(2.0, subtotal * 0.003);
        if (q >= 1 && q <= 5000 && Math.abs(round2(q * u) - subtotal) <= tol) {
          qty = q;
          unit = u;
        }
      }
    }

    if (qty > 0 && unit > 0) {
      // Ajuste final: unit por división para que el subtotal quede exacto
      const calc = round2(qty * unit);
      const tol = Math.max(2.0, subtotal * 0.003);
      if (Math.abs(calc - subtotal) > tol) unit = round2(subtotal / qty);

      items.push({
        cantidad: qty,
        descripcion: desc,
        precioConIva: unit,
        subtotalConIva: subtotal
      });
    }
  }

  return items;
}

// IA 2: items
async function extractItemsIA(texto) {
  // ✅ 1) Primero intentamos extractor determinístico (evita que "24800,00" rompa cantidades)
  const smart = extractItemsSmartRegex(texto);
  if (smart.length >= 3) {
    if (DEBUG) log("🧾 ITEMS smartRegex count:", smart.length);
    return { items: smart };
  }

  // ✅ 2) Si no alcanza, fallback a Gemini (mantengo tu lógica)
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
      cantidad: Number(it.cantidad || 0),
      descripcion: String(it.descripcion || "").trim(),
      precioConIva: Number(it.precioConIva || 0),
      subtotalConIva: Number(it.subtotalConIva || 0),
    })).filter((x) => x.descripcion && (x.subtotalConIva > 0 || x.precioConIva > 0));

    // ✅ saneo: si qty está mal (muy grande / no entera), la reconstruimos
    for (const x of clean) {
      if (x.subtotalConIva > 0 && x.precioConIva > 0) {
        const qInfer = Math.round(x.subtotalConIva / x.precioConIva);
        const tol = Math.max(2.0, x.subtotalConIva * 0.003);
        if (
          (!Number.isInteger(x.cantidad) || x.cantidad <= 0 || x.cantidad > 5000) &&
          qInfer >= 1 && qInfer <= 5000 &&
          Math.abs(round2(qInfer * x.precioConIva) - round2(x.subtotalConIva)) <= tol
        ) {
          x.cantidad = qInfer;
        }
      }

      // completar unit/sub si falta
      if (x.subtotalConIva > 0 && (!x.precioConIva || x.precioConIva <= 0) && x.cantidad > 0) {
        x.precioConIva = round2(x.subtotalConIva / x.cantidad);
      }
      if (x.precioConIva > 0 && (!x.subtotalConIva || x.subtotalConIva <= 0) && x.cantidad > 0) {
        x.subtotalConIva = round2(x.precioConIva * x.cantidad);
      }
    }

    const final = clean.filter(x => x.cantidad > 0 && x.precioConIva > 0 && x.subtotalConIva > 0);
    if (DEBUG) log("🧾 ITEMS gemini count:", final.length);
    return { items: final };
  } catch {
    // ✅ 3) Último fallback: si Gemini falla, devolvemos lo smart aunque sea poco
    if (smart.length) return { items: smart };
    return { items: [] };
  }
}

function extractDataRegex(texto) {
  const cuit = pickCuitCliente(texto);
  const textLimpio = String(texto || "").replace(/[\r\n",]/g, " ").replace(/\s{2,}/g, " ");

  let dom = "", loc = "";
  const matchDom = textLimpio.match(/Domicilio:\s*(.*?)\s*(?:CP:|I\.V\.A|Localidad|C\.U\.I\.T)/i);
  if (matchDom && matchDom[1]) dom = matchDom[1].trim();

  const matchLoc = textLimpio.match(/Localidad:\s*(.*?)\s*(?:C\.U\.I\.T|Remito|Condición)/i);
  if (matchLoc && matchLoc[1]) loc = matchLoc[1].trim();

  let domicilioRemito = [dom, loc].filter(Boolean).join(", ").replace(/CP:\s*0/gi, "").trim();
  return { cuit, domicilioRemito, items: [] };
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

    if (DEBUG) {
      log("🤖 IA cabecera:", cabecera);
      log("🤖 IA items count:", Array.isArray(items) ? items.length : 0);
    }
  }

  const fb = extractDataRegex(texto);
  if (!cuit || onlyDigits(cuit).length !== 11) cuit = fb.cuit || pickCuitCliente(texto);
  if (!domicilioRemito || domicilioRemito.length < 5) domicilioRemito = fb.domicilioRemito || smart.domicilioRemito || "";

  const loc = String(smart.localidad || "").trim();
  const smartFull = String(smart.domicilioRemito || "").trim();
  const hasLocComma = loc ? new RegExp(`,\\s*${escapeRegExp(loc)}\\b`, "i").test(domicilioRemito) : false;
  if (smartFull && loc && (!domicilioRemito || !hasLocComma)) domicilioRemito = smartFull;

  domicilioRemito = String(domicilioRemito || "")
    .replace(/CP:\s*0/gi, "")
    .replace(/\s+,/g, ",")
    .replace(/\s{2,}/g, " ")
    .trim();

  cuit = onlyDigits(cuit);
  if (cuit.length !== 11) cuit = pickCuitCliente(texto);

  return { cuit, domicilioRemito, items };
}

// ============================
// ✅ parseMultiplesRemitos (multi-PDF robusto)
// ============================
async function parseMultiplesRemitos(files) {
  let textoCombinado = "";
  const docsPricing = [];
  const docCuitts = new Set();

  for (const file of files) {
    const dataBuffer = fs.readFileSync(file.path);
    const parsed = await pdfParse(dataBuffer);
    const textoDoc = normalizePdfText(parsed?.text || "");

    if (DEBUG) {
      log("📄 DOC:", file.originalname || file.filename, "| chars:", textoDoc.length);
      const arr = textoDoc.split("\n");
      log("📄 DOC head:\n" + arr.slice(0, 10).join("\n"));
      log("📄 DOC tail:\n" + arr.slice(-10).join("\n"));
    }

    // ✅ CUIT por doc (para evitar mezclar clientes)
    const cDoc = onlyDigits(pickCuitCliente(textoDoc));
    if (cDoc && cDoc.length === 11 && String(cDoc) !== String(CUIT_DISTRIBUIDORA || "")) docCuitts.add(cDoc);

    const pricing = extractDocPricingSummary(textoDoc);
    docsPricing.push(pricing);

    textoCombinado += "\n\n--- DOC ---\n\n" + textoDoc;
  }

  if (docCuitts.size > 1) {
    throw new Error(`Se detectaron remitos de distintos CUIT (${[...docCuitts].join(", ")}). Subí remitos del mismo cliente.`);
  }

  const extracted = await extractData(textoCombinado);
  const itemsRaw = extracted.items || [];

  const subtotalItems = sumItemsBruto(itemsRaw);
  const subtotalDocs = round2(docsPricing.reduce((a, x) => a + Number(x.subtotalBruto || 0), 0));
  const totalDocs = round2(docsPricing.reduce((a, x) => a + Number(x.totalFinal || 0), 0));

  const subtotalBruto = (subtotalDocs > 0 && totalDocs > 0) ? subtotalDocs : subtotalItems;
  const totalFinal = (totalDocs > 0) ? totalDocs : subtotalItems;

  // Validación: subtotal ítems vs subtotal docs
  if (subtotalDocs > 0 && subtotalItems > 0) {
    const tol = Math.max(1.0, subtotalDocs * 0.002);
    const delta = Math.abs(subtotalItems - subtotalDocs);
    if (delta > tol) {
      throw new Error(`No coincide subtotal de ítems vs subtotal del remito. Ítems=$${formatMoneyAR(subtotalItems)} | Remito=$${formatMoneyAR(subtotalDocs)}.`);
    }
  }

  // Factor global para que total final coincida EXACTO
  const factor = (subtotalItems > 0 && totalFinal > 0 && totalFinal < subtotalItems - 0.01)
    ? (totalFinal / subtotalItems)
    : 1;

  let items = itemsRaw;
  if (factor > 0 && factor < 1 && totalFinal > 0) {
    items = applyFactorAndReconcile(itemsRaw, factor, totalFinal);
  }

  const descuentoImporte = round2(subtotalBruto - totalFinal);

  let descuentoPct = 0;
  const pcts = docsPricing.map(x => Number(x.descuentoPct || 0)).filter(x => x > 0);
  const pctUnique = pcts.length ? pcts.every(p => Math.abs(p - pcts[0]) <= 0.05) : false;
  if (pctUnique) descuentoPct = round2(pcts[0]);
  else if (subtotalBruto > 0 && descuentoImporte > 0) descuentoPct = round2((descuentoImporte / subtotalBruto) * 100);

  if (DEBUG) {
    log("🧾 PRICING:", {
      subtotalItems,
      subtotalDocs,
      totalDocs,
      factor,
      descuentoPct,
      descuentoImporte
    });
    log("🧾 DOCS pricing strategies:", docsPricing.map(x => x.strategies));
  }

  return {
    texto: textoCombinado,
    cuit: extracted.cuit,
    domicilioRemito: extracted.domicilioRemito,
    items,
    subtotalBruto,
    descuentoPct,
    descuentoImporte,
    totalFinal
  };
}

// ============================
// ✅ LOGO (local) -> DataURL para PDF
// ============================
function findLogoPath() {
  const candidates = [
    "logo.jpeg", "logo.jpg", "logo.png",
    "Logo.jpeg", "Logo.jpg", "Logo.png"
  ].map(f => path.join(process.cwd(), f));

  for (const p of candidates) {
    try { if (fs.existsSync(p)) return p; } catch {}
  }
  return "";
}

function logoPathToDataUrl(p) {
  try {
    if (!p) return "";
    const ext = path.extname(p).toLowerCase();
    const mime = ext === ".png" ? "image/png" : "image/jpeg";
    const buf = fs.readFileSync(p);
    return `data:${mime};base64,${buf.toString("base64")}`;
  } catch {
    return "";
  }
}

const LOGO_PATH = findLogoPath();
const LOGO_DATA_URL = logoPathToDataUrl(LOGO_PATH);

// ============================
// ✅ HTML FACTURA (Preview + PDF) + DOMICILIOS + RESUMEN
// ============================
function buildFacturaHtml({
  receptor, fechaISO, pv, nro,
  items, neto, iva, total,
  cae, caeVtoISO, condicionVenta,
  qrDataUrl, isPreview = false,
  notaFactura = "",
  subtotalBruto = 0,
  descuentoPct = 0,
  descuentoImporte = 0,
  totalFinal = 0,
}) {
  const pvStr = pad(pv, 5);
  const nroStr = pad(nro, 8);
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
    </tr>
  `).join("");

  const pageStyle = isPreview
    ? `width: 100%; padding: 10px; zoom: 0.8;`
    : `width: 820px; margin: 0 auto; padding: 30px;`;

  // DOMICILIOS (Remito vs AFIP)
  const domRemito = String(receptor?.domicilioRemito || "").trim();
  const domAfip = String(receptor?.domicilioAfip || "").trim();

  const normAddr = (s) => String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[.,;]+/g, "")
    .trim();

  const domMain = domRemito || domAfip || "Domicilio no informado";
  const showBoth = domRemito && domAfip && normAddr(domRemito) !== normAddr(domAfip);

  const domicilioHtml = showBoth
    ? `
        <div class="muted">Domicilio (Entrega/Remito): <strong>${safeText(domRemito)}</strong></div>
        <div class="muted">Domicilio Fiscal (AFIP): <strong>${safeText(domAfip)}</strong></div>
      `
    : `
        <div class="muted">Domicilio: <strong>${safeText(domMain)}</strong></div>
      `;

  const showDesc = Number(descuentoImporte || 0) > 0 && Number(subtotalBruto || 0) > 0;

  return `<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8" />
<title>Factura A ${pvStr}-${nroStr}</title>
<style>
  * { box-sizing: border-box; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; }
  body { margin: 0; color: #0f172a; background: #ffffff; }
  .page { ${pageStyle} }
  .header-box { display: flex; justify-content: space-between; border: 2px solid #1e293b; border-radius: 8px; position: relative; margin-bottom: 20px; }
  .header-left { flex: 1; padding: 20px; border-right: 1px solid #e2e8f0; }
  .header-right { flex: 1; padding: 20px; position: relative; text-align: right; }
  .letter-box { position: absolute; top: -2px; left: 50%; transform: translateX(-50%); width: 50px; height: 50px; border: 2px solid #1e293b; border-bottom-left-radius: 8px; border-bottom-right-radius: 8px; border-top: none; background: #fff; display: flex; align-items: center; justify-content: center; }
  .letter { font-size: 32px; font-weight: 900; color: #1e293b; margin: 0; }
  .title { font-weight: 900; font-size: 26px; color: #1e293b; margin-bottom: 5px; text-transform: uppercase; letter-spacing: 0.5px; }
  .muted { color: #475569; font-size: 11px; margin: 3px 0; }
  .muted strong { color: #1e293b; }
  .client-box { border: 1px solid #cbd5e1; border-radius: 8px; padding: 15px; margin-bottom: 20px; background: #f8fafc; page-break-inside: avoid; position: relative; }
  .nota-fraccion {
    position: absolute; top: 10px; right: 15px;
    background: #fef08a; color: #a16207;
    font-size: 10px; font-weight: bold;
    padding: 4px 8px; border-radius: 4px;
    white-space: pre-line;
    text-align: right;
  }
  table { width: 100%; border-collapse: collapse; margin-top: 10px; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; }
  th { background: #1e293b; color: #ffffff; padding: 12px 10px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; text-align: left; }
  th.r { text-align: right; }
  .r { text-align: right; }
  .footer-grid { display: flex; gap: 20px; margin-top: 20px; page-break-inside: avoid; }
  .totals-box { flex: 1; border: 1px solid #cbd5e1; border-radius: 8px; padding: 15px; }
  .total-row { display: flex; justify-content: space-between; font-size: 12px; margin-bottom: 8px; color: #475569; }
  .total-row.final { font-size: 16px; font-weight: 900; color: #0f172a; margin-top: 10px; padding-top: 10px; border-top: 2px solid #e2e8f0; }
  .arca-box { flex: 1; display: flex; align-items: center; justify-content: space-between; border: 1px solid #cbd5e1; border-radius: 8px; padding: 15px; background: #f8fafc; }
  .arca-info { font-size: 11px; color: #334155; line-height: 1.6; }
  .qr { width: 110px; height: 110px; }
  .leyenda { text-align: center; font-size: 10px; font-weight: bold; color: #64748b; margin-top: 20px; border-top: 1px dashed #cbd5e1; padding-top: 10px; page-break-inside: avoid; }
</style>
</head>
<body>
<div class="page">
  ${isPreview ? `<div style="background:#fef08a; padding:10px; text-align:center; font-weight:bold; color:#a16207; margin-bottom:15px; border-radius:8px;">MODO VISTA PREVIA (AÚN NO AUTORIZADO POR ARCA)</div>` : ""}
  <div class="header-box">
    <div class="letter-box"><div class="letter">A</div></div>
    <div class="header-left">
  ${LOGO_DATA_URL ? `
    <div style="display:flex; align-items:center; gap:14px; margin-bottom:10px;">
      <img src="${LOGO_DATA_URL}" alt="Logo" style="height:64px; width:auto; object-fit:contain; display:block;" />
      <div style="border-left:1px solid #e2e8f0; height:42px;"></div>
      <div style="display:flex; flex-direction:column; gap:2px;"></div>
    </div>
  ` : ``}

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
    <table style="border: none; margin: 0;">
      <tr style="background: transparent;">
        <td style="padding: 0; width: 60%; vertical-align: top;">
          <div class="muted">CUIT: <strong>${safeText(receptor.cuit)}</strong></div>
          <div class="muted">Condición frente al IVA: <strong>${safeText(receptor.condicionIVA)}</strong></div>
          <div class="muted">Condición de Venta: <strong>${safeText(condicionVenta)}</strong></div>
        </td>
        <td style="padding: 0; width: 40%; vertical-align: top;">
          <div class="muted">Apellido y Nombre / Razón Social: <strong>${safeText(receptor.nombre)}</strong></div>
          ${domicilioHtml}
        </td>
      </tr>
    </table>
  </div>

  <table>
    <thead>
      <tr>
        <th>Producto / Servicio</th>
        <th class="r" style="width:50px;">Cant</th>
        <th class="r" style="width:90px;">Precio Unit.</th>
        <th class="r" style="width:90px;">Subtotal Neto</th>
        <th class="r" style="width:50px;">Alic. IVA</th>
        <th class="r" style="width:100px;">Subtotal c/IVA</th>
      </tr>
    </thead>
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
        <div style="height:10px;"></div>
      ` : ""}

      <div class="total-row"><span>Importe Neto Gravado:</span><strong>$ ${formatMoneyAR(neto)}</strong></div>
      <div class="total-row"><span>IVA 21%:</span><strong>$ ${formatMoneyAR(iva)}</strong></div>
      <div class="total-row"><span>Importe Otros Tributos:</span><strong>$ 0,00</strong></div>
      <div class="total-row final"><span>IMPORTE TOTAL:</span><span>$ ${formatMoneyAR(total)}</span></div>
    </div>
  </div>

  <div class="leyenda">${safeText(EMISOR.leyenda)}</div>
</div>
</body>
</html>`;
}

// ============================
// ROUTES
// ============================
app.get("/health", (req, res) => res.json({ ok: true, version: APP_VERSION, iaIntegrada: !!geminiModel }));

app.get("/tipos", async (req, res) => {
  try {
    res.json({ pv: await getPtoVentaSeguro(), tipos: [{ id: CBTE_TIPO_REAL, name: "Factura (WS)", habilitado: true }] });
  } catch (e) {
    res.status(500).json({ message: e.message });
  }
});

app.post("/leer-remito", upload.array("remito", 10), async (req, res) => {
  const started = Date.now();
  try {
    const files = req.files || (req.file ? [req.file] : []);
    if (!files.length) return res.status(400).json({ message: "No llegó archivo PDF" });

    const parsed = await parseMultiplesRemitos(files);
    files.forEach(f => cleanupTempFile(f.path));

    res.json({
      cuit: parsed.cuit,
      domicilioRemito: parsed.domicilioRemito,
      items: parsed.items,
      total: parsed.totalFinal,
      subtotalBruto: parsed.subtotalBruto,
      descuentoPct: parsed.descuentoPct,
      descuentoImporte: parsed.descuentoImporte,
      ms: Date.now() - started,
      version: APP_VERSION
    });
  } catch (err) {
    (req.files || []).forEach(f => cleanupTempFile(f.path));
    res.status(500).json({ message: "Error", detail: String(err?.message || err) });
  }
});

// ============================
// ✅ PREVIEW: coherente por PARTE (totales parte) + nota con totales por parte
// ============================
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
      const cantidad = Number(x.cantidad || 0);
      const descripcion = String(x.descripcion || "").trim();
      const precioConIva = Number(x.precioConIva || 0);
      const subtotalConIva = Number(x.subtotalConIva || 0);
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

    if (factor > 0 && factor < 1 && totalFinalIn > 0) {
      allItems = applyFactorAndReconcile(allItems, factor, totalFinalIn);
    }

    const chunks = [];
    for (let i = 0; i < allItems.length; i += ITEMS_POR_FACTURA) chunks.push(allItems.slice(i, i + ITEMS_POR_FACTURA));

    const partes = chunks.map(ch => round2(ch.reduce((a, it) => a + Number(it.subtotalConIva || 0), 0)));
    const totalSumPartes = round2(partes.reduce((a, x) => a + x, 0));
    const totalRemitoGlobal = totalFinalIn > 0 ? round2(totalFinalIn) : totalSumPartes;

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

        let subBrutoParte = 0, descParte = 0, pctParte = 0;
        const factorUsado = (factor > 0 && factor < 1) ? factor : 1;
        if (factorUsado < 1 && totalParte > 0) {
          subBrutoParte = round2(totalParte / factorUsado);
          descParte = round2(subBrutoParte - totalParte);
          pctParte = descuentoPctIn > 0 ? round2(descuentoPctIn) : 0;
        }

        let impTotal = 0, impNeto = 0, impIVA = 0;
        const itemsCalc = itemsParte.map((it) => {
          const subConIva = Number(it.subtotalConIva || 0);
          const subNeto = round2(subConIva / 1.21);
          const subIva = round2(subConIva - subNeto);
          impTotal += subConIva; impNeto += subNeto; impIVA += subIva;
          return { ...it, subtotalNeto: subNeto, precioNeto: it.cantidad > 0 ? round2(subNeto / it.cantidad) : 0 };
        });
        impTotal = round2(impTotal); impNeto = round2(impNeto); impIVA = round2(impIVA);

        let rec = { nombre: "Completá el CUIT para ver al cliente...", domicilioAfip: "", condicionIVA: "-" };
        if (cuitCliente.length === 11) rec = await getReceptorDesdePadron(cuitCliente);

        let nota = "";
        if (chunks.length > 1) {
          nota = `FACTURA PARTE ${p} DE ${chunks.length}\n`;
          nota += `Total remito: $ ${formatMoneyAR(totalRemitoGlobal)}\n`;
          partes.forEach((t, idx) => nota += `Parte ${idx + 1}: $ ${formatMoneyAR(t)}\n`);
          nota = nota.trim();
        }

        const htmlParte = buildFacturaHtml({
          receptor: { cuit: cuitCliente || "00000000000", nombre: rec.nombre, condicionIVA: rec.condicionIVA, domicilioAfip: rec.domicilioAfip, domicilioRemito: domicilioRemitoIn },
          fechaISO: todayISO(),
          pv,
          nro: 0,
          items: itemsCalc,
          neto: impNeto, iva: impIVA, total: impTotal,
          cae: null, caeVtoISO: null,
          condicionVenta,
          qrDataUrl: null,
          isPreview: true,
          notaFactura: nota,

          subtotalBruto: subBrutoParte,
          descuentoPct: pctParte,
          descuentoImporte: descParte,
          totalFinal: totalParte
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

    const idxParte = Math.max(0, Number(previewParteReq || 1) - 1);
    const itemsParte = chunks[idxParte] || [];
    const totalParte = round2(itemsParte.reduce((a, it) => a + Number(it.subtotalConIva || 0), 0));

    let subBrutoParte = 0, descParte = 0, pctParte = 0;
    const factorUsado = (factor > 0 && factor < 1) ? factor : 1;

    if (factorUsado < 1 && totalParte > 0) {
      subBrutoParte = round2(totalParte / factorUsado);
      descParte = round2(subBrutoParte - totalParte);
      pctParte = descuentoPctIn > 0 ? round2(descuentoPctIn) : 0;
    }

    let impTotal = 0, impNeto = 0, impIVA = 0;
    const itemsCalc = itemsParte.map((it) => {
      const subConIva = Number(it.subtotalConIva || 0);
      const subNeto = round2(subConIva / 1.21);
      const subIva = round2(subConIva - subNeto);
      impTotal += subConIva; impNeto += subNeto; impIVA += subIva;
      return { ...it, subtotalNeto: subNeto, precioNeto: it.cantidad > 0 ? round2(subNeto / it.cantidad) : 0 };
    });
    impTotal = round2(impTotal); impNeto = round2(impNeto); impIVA = round2(impIVA);

    let rec = { nombre: "Completá el CUIT para ver al cliente...", domicilioAfip: "", condicionIVA: "-" };
    if (cuitCliente.length === 11) rec = await getReceptorDesdePadron(cuitCliente);

    let nota = "";
    if (chunks.length > 1) {
      nota = `FACTURA PARTE ${idxParte + 1} DE ${chunks.length}\n`;
      nota += `Total remito: $ ${formatMoneyAR(totalRemitoGlobal)}\n`;
      partes.forEach((t, i) => nota += `Parte ${i + 1}: $ ${formatMoneyAR(t)}\n`);
      nota = nota.trim();
    }

    const html = buildFacturaHtml({
      receptor: { cuit: cuitCliente || "00000000000", nombre: rec.nombre, condicionIVA: rec.condicionIVA, domicilioAfip: rec.domicilioAfip, domicilioRemito: domicilioRemitoIn },
      fechaISO: todayISO(),
      pv,
      nro: 0,
      items: itemsCalc,
      neto: impNeto, iva: impIVA, total: impTotal,
      cae: null, caeVtoISO: null,
      condicionVenta,
      qrDataUrl: null,
      isPreview: true,
      notaFactura: nota,

      subtotalBruto: subBrutoParte,
      descuentoPct: pctParte,
      descuentoImporte: descParte,
      totalFinal: totalParte
    });

    res.send(html);
  } catch (err) {
    res.status(500).send("Error generando vista previa: " + err.message);
  }
});

// ============================
// ✅ FACTURAR (PDF + MAIL) + WhatsApp con URLs públicas
// ============================
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

      return { cantidad, descripcion, precioConIva: unit, subtotalConIva: sub };
    }).filter(Boolean);

    if (!allItems.length) return res.status(400).json({ message: "Ítems inválidos" });

    // ✅ padrón (A13 + constancia)
    const rec = await getReceptorDesdePadron(cuitCliente);

    // factor global para que total post-desc coincida EXACTO
    const subtotalCalc = sumItemsBruto(allItems);
    let factor = 1;

    if (totalFinalIn > 0 && subtotalCalc > 0) factor = totalFinalIn / subtotalCalc;
    else if (descuentoPctIn > 0) factor = 1 - (descuentoPctIn / 100);
    else if (descuentoImporteIn > 0 && subtotalCalc > 0) factor = (subtotalCalc - descuentoImporteIn) / subtotalCalc;

    if (factor > 0 && factor < 1 && totalFinalIn > 0) {
      allItems = applyFactorAndReconcile(allItems, factor, totalFinalIn);
    }

    // split por ITEMS_POR_FACTURA
    const chunks = [];
    for (let i = 0; i < allItems.length; i += ITEMS_POR_FACTURA) {
      chunks.push(allItems.slice(i, i + ITEMS_POR_FACTURA));
    }

    const partes = chunks.map(ch => round2(ch.reduce((a, it) => a + Number(it.subtotalConIva || 0), 0)));
    const totalSumPartes = round2(partes.reduce((a, x) => a + x, 0));
    const totalRemitoGlobal = totalFinalIn > 0 ? round2(totalFinalIn) : totalSumPartes;

    const resultados = [];
    const fecha = todayISO();
    const cbteFch = yyyymmdd(fecha);

    const factorUsado = (factor > 0 && factor < 1) ? factor : 1;
    let acumuladoSubBruto = 0;

    const emailEnabled = !!(GMAIL_USER && GMAIL_APP_PASS);
    const mailParts = [];
    const mailAttachments = []; // ✅ SOLO PDFs, sin logo inline

    for (let i = 0; i < chunks.length; i++) {
      const chunkItems = chunks[i];

      let notaFactura = "";
      if (chunks.length > 1) {
        notaFactura = `FACTURA PARTE ${i + 1} DE ${chunks.length}\n`;
        notaFactura += `Total remito: $ ${formatMoneyAR(totalRemitoGlobal)}\n`;
        partes.forEach((t, idx) => (notaFactura += `Parte ${idx + 1}: $ ${formatMoneyAR(t)}\n`));
        notaFactura = notaFactura.trim();
      }

      let impTotal = 0, impNeto = 0, impIVA = 0;
      const itemsCalc = chunkItems.map((it) => {
        const subConIva = Number(it.subtotalConIva || 0);
        const subNeto = round2(subConIva / 1.21);
        const subIva = round2(subConIva - subNeto);
        impTotal += subConIva; impNeto += subNeto; impIVA += subIva;
        return { ...it, subtotalNeto: subNeto, precioNeto: it.cantidad > 0 ? round2(subNeto / it.cantidad) : 0 };
      });
      impTotal = round2(impTotal); impNeto = round2(impNeto); impIVA = round2(impIVA);

      let subBrutoParte = 0, descParte = 0, pctParte = 0;
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
        neto: impNeto, iva: impIVA, total: impTotal,
        cae: result.CAE, caeVtoISO: result.CAEFchVto,
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
        options: { width: 8.27, marginTop: 0.35, marginBottom: 0.35, marginLeft: 0.35, marginRight: 0.35 }
      });

      // ✅ descargamos el PDF una sola vez
      let pdfBuffer = null;
      try {
        pdfBuffer = await downloadToBuffer(pdfRes.file);
      } catch (e) {
        if (DEBUG) errlog("No pude bajar PDF para public/email:", e?.message || e);
      }

      // ✅ URL pública estable (servida por TU server)
      let pdfPublicUrl = "";
      try {
        if (pdfBuffer && pdfBuffer.length) {
          pdfPublicUrl = savePublicPdf(pdfBuffer, `FA_${pad(pv, 5)}-${pad(nro, 8)}`);
        } else {
          // fallback: usamos la URL que devuelve el SDK
          pdfPublicUrl = String(pdfRes.file || "");
        }
      } catch (e) {
        pdfPublicUrl = String(pdfRes.file || "");
      }

      mailParts.push({
        parte: i + 1,
        totalPartes: chunks.length,
        pv,
        nro,
        cae: result.CAE,
        total: impTotal,
        pdfUrl: pdfPublicUrl
      });

      // ✅ Adjuntos para email (sin logo)
      if (emailEnabled) {
        if (!pdfBuffer) {
          // si no pudimos descargar antes, reintentamos
          pdfBuffer = await downloadToBuffer(pdfRes.file);
        }
        mailAttachments.push({
          filename: `FA_${pad(pv, 5)}-${pad(nro, 8)}.pdf`,
          content: pdfBuffer,
          contentType: "application/pdf"
        });
      }

      resultados.push({ nroFactura: nro, cae: result.CAE, total: impTotal, pdfUrl: pdfPublicUrl });
      guardarFacturaEnDB({ cuitCliente, rec, nro, pv, cae: result.CAE, impTotal, pdfPublicUrl, condicionVenta, fecha, chunkItems });
    }

    // ✅ Envío 1 SOLO EMAIL con todos los PDFs adjuntos (SIN LOGO)
    if (emailEnabled && mailParts.length && mailAttachments.some(a => a.contentType === "application/pdf")) {
      const domRemitoMail = String(domicilioRemitoIn || "").trim();
      const domAfipMail = String(rec.domicilioAfip || "").trim();

      const normAddr = (s) => String(s || "")
        .toLowerCase()
        .replace(/\s+/g, " ")
        .replace(/[.,;]+/g, "")
        .trim();

      const showBothDom = domRemitoMail && domAfipMail && normAddr(domRemitoMail) !== normAddr(domAfipMail);

      const domicilioMailHtml = showBothDom
        ? `
          <div style="margin-top:6px;">
            <div><strong>Domicilio (Entrega/Remito):</strong> ${safeText(domRemitoMail)}</div>
            <div><strong>Domicilio Fiscal (AFIP):</strong> ${safeText(domAfipMail)}</div>
          </div>
        `
        : `
          <div style="margin-top:6px;"><strong>Domicilio:</strong> ${safeText(domRemitoMail || domAfipMail || "Domicilio no informado")}</div>
        `;

      const showDescGlobal = (Number(descuentoImporteIn || 0) > 0 && Number(subtotalBrutoIn || 0) > 0 && Number(totalFinalIn || 0) > 0);

      const partsRows = mailParts.map(p => `
        <tr>
          <td style="padding:10px;border-bottom:1px solid #e2e8f0;">Parte ${p.parte} / ${p.totalPartes}</td>
          <td style="padding:10px;border-bottom:1px solid #e2e8f0;">A-${pad(p.pv, 5)}-${pad(p.nro, 8)}</td>
          <td style="padding:10px;border-bottom:1px solid #e2e8f0;">CAE ${safeText(p.cae)}</td>
          <td style="padding:10px;border-bottom:1px solid #e2e8f0;text-align:right;font-weight:900;">$ ${formatMoneyAR(p.total)}</td>
        </tr>
      `).join("");

      const totalMail = round2(mailParts.reduce((a, x) => a + Number(x.total || 0), 0));

      const subject = (mailParts.length > 1)
        ? `Facturas A (${mailParts.length} partes) - ${EMISOR.nombreVisible} - ${safeText(rec.nombre)}`
        : `Factura A ${pad(mailParts[0].pv, 5)}-${pad(mailParts[0].nro, 8)} - ${EMISOR.nombreVisible}`;

      const mailHtml = `
        <div style="font-family:Arial,sans-serif;background:#f6f7fb;padding:30px;">
          <div style="max-width:720px;margin:0 auto;background:#fff;border-radius:14px;overflow:hidden;box-shadow:0 8px 22px rgba(0,0,0,0.08);">
            <div style="background:#0f172a;color:#fff;padding:18px 24px;">
              <div style="font-size:18px;font-weight:900;letter-spacing:0.4px;">${safeText(EMISOR.nombreVisible)}</div>
              <div style="opacity:0.85;font-weight:700;font-size:12px;margin-top:3px;">Factura Electrónica</div>
            </div>

            <div style="padding:24px;">
              <div style="font-size:14px;margin-bottom:10px;">Estimado/a <strong>${safeText(rec.nombre)}</strong>,</div>
              <div style="color:#475569;font-size:13px;line-height:1.55;">
                Adjuntamos el/los comprobante(s) electrónico(s) correspondiente(s) a su compra.
              </div>

              <div style="margin-top:16px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:14px;">
                <div><strong>CUIT:</strong> ${safeText(cuitCliente)}</div>
                ${domicilioMailHtml}
                <div style="margin-top:8px;"><strong>Condición de Venta:</strong> ${safeText(condicionVenta)}</div>

                ${showDescGlobal ? `
                  <div style="margin-top:10px;">
                    <div><strong>Subtotal (Remito):</strong> $ ${formatMoneyAR(subtotalBrutoIn)}</div>
                    <div><strong>Descuento (${formatMoneyAR(descuentoPctIn)}%):</strong> -$ ${formatMoneyAR(descuentoImporteIn)}</div>
                    <div><strong>Total Remito (post-desc):</strong> $ ${formatMoneyAR(totalFinalIn)}</div>
                  </div>
                ` : ``}
              </div>

              <div style="margin-top:16px;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;">
                <div style="background:#1e293b;color:#fff;padding:12px 14px;font-weight:900;">Detalle de comprobantes</div>
                <table style="width:100%;border-collapse:collapse;font-size:13px;">
                  <thead>
                    <tr style="background:#f1f5f9;color:#334155;">
                      <th style="padding:10px;text-align:left;">Parte</th>
                      <th style="padding:10px;text-align:left;">Comprobante</th>
                      <th style="padding:10px;text-align:left;">CAE</th>
                      <th style="padding:10px;text-align:right;">Total</th>
                    </tr>
                  </thead>
                  <tbody>${partsRows}</tbody>
                  <tfoot>
                    <tr>
                      <td colspan="3" style="padding:12px;text-align:right;font-weight:900;">TOTAL FACTURADO</td>
                      <td style="padding:12px;text-align:right;font-weight:900;">$ ${formatMoneyAR(totalMail)}</td>
                    </tr>
                  </tfoot>
                </table>
              </div>

              <div style="margin-top:18px;text-align:center;color:#64748b;font-size:12px;">
                Comprobantes autorizados por ARCA. Ante cualquier duda, respondé este email.
              </div>
            </div>
          </div>
        </div>
      `;

      if (resendClient) {
  await resendClient.emails.send({
    from: "Mercado Limpio <onboarding@resend.dev>",
    to: "distribuidoramercadolimpio@gmail.com",
    subject: `📊 Resumen ${MESES[mes]} ${anio} — ${facturas.length} facturas | $ ${fmtAR(totalGeneral)}`,
    html: htmlMail,
  });
} else {
  await transporter.sendMail({
    from: `"Mercado Limpio" <${GMAIL_USER}>`,
    to: "distribuidoramercadolimpio@gmail.com",
    subject: `📊 Resumen ${MESES[mes]} ${anio} — ${facturas.length} facturas | $ ${fmtAR(totalGeneral)}`,
    html: htmlMail,
  });
}

    let finalMsg = `Factura autorizada con éxito.`;
    if (resultados.length > 1) finalMsg = `¡Factura dividida! Se emitieron ${resultados.length} comprobantes con éxito.`;

    // ✅ WhatsApp con URL pública(s)
    let waText = `Factura de Mercado Limpio\nCliente: ${rec.nombre}\nCUIT: ${cuitCliente}\n\n`;
    resultados.forEach((r, idx) => {
      waText += `Parte ${idx + 1}: Comp. Nro ${pad(r.nroFactura, 8)} | Total: $ ${formatMoneyAR(r.total)} | CAE: ${r.cae}\n`;
      if (r.pdfUrl) waText += `PDF: ${r.pdfUrl}\n`;
      waText += `\n`;
    });

    res.json({
      ok: true,
      version: APP_VERSION,
      puntoDeVenta: pv,
      mensaje: finalMsg,
      facturas: resultados,
      receptor: { cuit: cuitCliente, nombre: rec.nombre, domicilio: domicilioRemitoIn || rec.domicilioAfip },
      waLink: `https://wa.me/?text=${encodeURIComponent(waText)}`
    });

  } catch (err) {
    res.status(500).json({ message: err.message, detail: err?.data || null });
  }
});

// --- RUTA DE PRUEBA: /admin/test-resumen?token=mercadolimpio ---
// Abrí esta URL en el navegador para probar el email sin esperar el cron
app.get("/admin/test-resumen", async (req, res) => {
  if (req.query.token !== "mercadolimpio") return res.status(401).send("No autorizado.");
  try {
    const mes  = req.query.mes  ? Number(req.query.mes)  : null;
    const anio = req.query.anio ? Number(req.query.anio) : null;
    await enviarResumenMensual(anio, mes);
    res.send("✅ Resumen enviado a distribuidoramercadolimpio@gmail.com");
  } catch(e) {
    res.status(500).send("❌ Error: " + (e?.message || e));
  }
});

// ============================
// ✅ START SERVER
// ============================
app.listen(PORT, () => {
  console.log(`🚀 Motor listo en puerto ${PORT} | v: ${APP_VERSION}`);
  console.log(`   PRODUCTION=${PRODUCTION} | PV_ENV=${PUNTO_VENTA_ENV || "(auto)"}`);
  console.log(`   DEBUG=${DEBUG ? "1" : "0"} | ITEMS_POR_FACTURA=${ITEMS_POR_FACTURA}`);
  console.log(`   Uploads: ${uploadDir}`);
  console.log(`   Public PDFs: ${publicPdfDir}`);
  console.log(`   PUBLIC_URL: ${PUBLIC_URL}`);
  console.log(`   Logo: ${LOGO_PATH ? LOGO_PATH : "(no encontrado en " + process.cwd() + ")"}`);
  console.log(`   PADRON10 enabled: ${ENABLE_PADRON_10 ? "YES" : "NO"}`);
});

// ============================
// ✅ Hardening: logs de errores no capturados
// ============================
process.on("unhandledRejection", (reason) => {
  console.error("❌ unhandledRejection:", reason);
});

process.on("uncaughtException", (err) => {
  console.error("❌ uncaughtException:", err);
});



// ================================================================
// ✅ MÓDULO: GUARDAR FACTURAS + RESUMEN MENSUAL AUTOMÁTICO
// Pegá este bloque entero AL FINAL de tu index.js
// El resumen llega a: distribuidoramercadolimpio@gmail.com
// Se envía automáticamente el 1° de cada mes a las 08:00 (Argentina)
// ================================================================

// --- BASE DE DATOS LOCAL (archivo facturas_db.jsonl en la raíz del proyecto) ---
const DB_FACTURAS = path.join(process.cwd(), "facturas_db.jsonl");

function guardarFacturaEnDB({ cuitCliente, rec, nro, pv, cae, impTotal, pdfPublicUrl, condicionVenta, fecha, chunkItems }) {
  try {
    const registro = {
      timestamp:     new Date().toISOString(),
      fecha,
      anio:          Number(String(fecha).split("-")[0]),
      mes:           Number(String(fecha).split("-")[1]),
      comprobante:   `A-${String(pv).padStart(5,"0")}-${String(nro).padStart(8,"0")}`,
      nroFactura:    nro,
      puntoVenta:    pv,
      cae:           String(cae),
      cuitCliente:   String(cuitCliente),
      nombreCliente: rec?.nombre || `CUIT ${cuitCliente}`,
      domicilio:     rec?.domicilioAfip || "",
      condicionVenta: String(condicionVenta || ""),
      total:         impTotal,
      pdfUrl:        pdfPublicUrl || "",
      items: (Array.isArray(chunkItems) ? chunkItems : []).map(it => ({
        descripcion:    it.descripcion,
        cantidad:       it.cantidad,
        precioConIva:   it.precioConIva,
        subtotalConIva: it.subtotalConIva,
      })),
    };
    fs.appendFileSync(DB_FACTURAS, JSON.stringify(registro) + "\n", "utf-8");
  } catch (e) {
    console.error("❌ [DB] Error guardando factura:", e?.message || e);
  }
}

function leerFacturasDelMes(anio, mes) {
  if (!fs.existsSync(DB_FACTURAS)) return [];
  return fs.readFileSync(DB_FACTURAS, "utf-8")
    .split("\n").filter(Boolean)
    .map(l => { try { return JSON.parse(l); } catch { return null; } })
    .filter(f => f && f.anio === anio && f.mes === mes);
}

// --- TEMPLATE HTML DEL RESUMEN MENSUAL ---
function buildResumenHTMLProfesional(anio, mes, facturas) {
  const MESES = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];
  const nombreMes = MESES[mes] || `Mes ${mes}`;
  const totalGeneral = facturas.reduce((a, f) => a + Number(f.total || 0), 0);

  // Agrupar por cliente
  const porCliente = {};
  for (const f of facturas) {
    if (!porCliente[f.cuitCliente]) porCliente[f.cuitCliente] = { nombre: f.nombreCliente, cuit: f.cuitCliente, total: 0, cant: 0 };
    porCliente[f.cuitCliente].total += Number(f.total || 0);
    porCliente[f.cuitCliente].cant++;
  }
  const clientes = Object.values(porCliente).sort((a,b) => b.total - a.total);

  const fmtAR = n => new Intl.NumberFormat("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(n||0));
  const safe  = s => String(s||"").replace(/[<>]/g,"");

  const filasFacturas = facturas
    .sort((a,b) => String(a.fecha).localeCompare(String(b.fecha)))
    .map((f, i) => `
      <tr style="background:${i%2===0?'#ffffff':'#f8fafc'};">
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;font-weight:700;color:#3b82f6;">${safe(f.fecha)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;font-weight:900;font-family:monospace;">${safe(f.comprobante)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;font-weight:700;">${safe(f.nombreCliente)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;color:#64748b;font-size:12px;">${safe(f.cuitCliente)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;color:#64748b;font-size:12px;font-family:monospace;">${safe(f.cae)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;text-align:right;font-weight:900;font-size:15px;color:#0f172a;">$ ${fmtAR(f.total)}</td>
        <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;text-align:center;">
          ${f.pdfUrl ? `<a href="${safe(f.pdfUrl)}" style="background:#3b82f6;color:#fff;padding:5px 12px;border-radius:6px;text-decoration:none;font-size:12px;font-weight:900;">📄 Ver</a>` : '<span style="color:#cbd5e1;">—</span>'}
        </td>
      </tr>`).join("");

  const filasClientes = clientes.map(c => `
    <tr>
      <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;font-weight:800;">${safe(c.nombre)}</td>
      <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;color:#64748b;font-size:12px;font-family:monospace;">${safe(c.cuit)}</td>
      <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;text-align:center;font-weight:700;">${c.cant}</td>
      <td style="padding:11px 14px;border-bottom:1px solid #e2e8f0;text-align:right;font-weight:900;color:#10b981;">$ ${fmtAR(c.total)}</td>
    </tr>`).join("");

  return `<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"/><title>Resumen ${nombreMes} ${anio}</title></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Segoe UI',Arial,sans-serif;">

<div style="max-width:900px;margin:0 auto;padding:30px 20px;">

  <!-- HEADER -->
  <div style="background:linear-gradient(135deg,#1e3a5f 0%,#0f172a 100%);border-radius:16px 16px 0 0;padding:32px 36px;border-bottom:3px solid #3b82f6;">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;">
      <div>
        <div style="font-size:11px;font-weight:900;color:#3b82f6;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px;">Reporte Interno</div>
        <div style="font-size:26px;font-weight:900;color:#ffffff;letter-spacing:0.3px;">MERCADO LIMPIO</div>
        <div style="font-size:13px;color:#94a3b8;font-weight:700;margin-top:3px;">Distribuidora · Facturación Electrónica ARCA/AFIP</div>
      </div>
      <div style="text-align:right;">
        <div style="font-size:13px;color:#64748b;font-weight:700;">Período</div>
        <div style="font-size:22px;font-weight:900;color:#3b82f6;">${nombreMes}</div>
        <div style="font-size:18px;font-weight:900;color:#94a3b8;">${anio}</div>
      </div>
    </div>
  </div>

  <!-- KPIs -->
  <div style="display:flex;background:#1e293b;border-left:1px solid #334155;border-right:1px solid #334155;">
    <div style="flex:1;padding:22px 20px;text-align:center;border-right:1px solid #334155;">
      <div style="font-size:36px;font-weight:900;color:#3b82f6;">${facturas.length}</div>
      <div style="font-size:12px;color:#94a3b8;font-weight:700;margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Facturas emitidas</div>
    </div>
    <div style="flex:2;padding:22px 20px;text-align:center;border-right:1px solid #334155;">
      <div style="font-size:32px;font-weight:900;color:#10b981;">$ ${fmtAR(totalGeneral)}</div>
      <div style="font-size:12px;color:#94a3b8;font-weight:700;margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Total facturado del mes</div>
    </div>
    <div style="flex:1;padding:22px 20px;text-align:center;">
      <div style="font-size:36px;font-weight:900;color:#f59e0b;">${clientes.length}</div>
      <div style="font-size:12px;color:#94a3b8;font-weight:700;margin-top:4px;text-transform:uppercase;letter-spacing:1px;">Clientes distintos</div>
    </div>
  </div>

  <!-- DETALLE FACTURAS -->
  <div style="background:#ffffff;border-left:1px solid #e2e8f0;border-right:1px solid #e2e8f0;">
    <div style="background:#1e293b;padding:16px 20px;">
      <div style="font-size:13px;font-weight:900;color:#ffffff;letter-spacing:0.5px;">📋 DETALLE COMPLETO DE FACTURAS</div>
    </div>
    <div style="overflow-x:auto;">
      <table style="width:100%;border-collapse:collapse;font-size:13px;color:#334155;">
        <thead>
          <tr style="background:#f1f5f9;">
            <th style="padding:12px 14px;text-align:left;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">Fecha</th>
            <th style="padding:12px 14px;text-align:left;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">Comprobante</th>
            <th style="padding:12px 14px;text-align:left;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">Cliente</th>
            <th style="padding:12px 14px;text-align:left;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">CUIT</th>
            <th style="padding:12px 14px;text-align:left;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">CAE</th>
            <th style="padding:12px 14px;text-align:right;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">Total</th>
            <th style="padding:12px 14px;text-align:center;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">PDF</th>
          </tr>
        </thead>
        <tbody>
          ${filasFacturas || `<tr><td colspan="7" style="padding:30px;text-align:center;color:#94a3b8;font-style:italic;">Sin facturas registradas este mes.</td></tr>`}
        </tbody>
        <tfoot>
          <tr style="background:#0f172a;">
            <td colspan="5" style="padding:14px 18px;font-weight:900;color:#fff;font-size:13px;text-align:right;text-transform:uppercase;letter-spacing:0.5px;">TOTAL DEL MES</td>
            <td style="padding:14px 18px;font-weight:900;color:#10b981;font-size:17px;text-align:right;">$ ${fmtAR(totalGeneral)}</td>
            <td></td>
          </tr>
        </tfoot>
      </table>
    </div>
  </div>

  <!-- RESUMEN POR CLIENTE -->
  <div style="background:#ffffff;border-left:1px solid #e2e8f0;border-right:1px solid #e2e8f0;border-top:4px solid #f1f5f9;">
    <div style="background:#1e293b;padding:16px 20px;">
      <div style="font-size:13px;font-weight:900;color:#ffffff;letter-spacing:0.5px;">👥 RESUMEN POR CLIENTE</div>
    </div>
    <table style="width:100%;border-collapse:collapse;font-size:13px;color:#334155;">
      <thead>
        <tr style="background:#f1f5f9;">
          <th style="padding:12px 14px;text-align:left;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">Cliente</th>
          <th style="padding:12px 14px;text-align:left;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">CUIT</th>
          <th style="padding:12px 14px;text-align:center;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">Facturas</th>
          <th style="padding:12px 14px;text-align:right;font-weight:900;color:#475569;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">Total</th>
        </tr>
      </thead>
      <tbody>${filasClientes || `<tr><td colspan="4" style="padding:20px;text-align:center;color:#94a3b8;">Sin datos.</td></tr>`}</tbody>
    </table>
  </div>

  <!-- FOOTER -->
  <div style="background:#1e293b;border-radius:0 0 16px 16px;padding:18px 28px;text-align:center;border:1px solid #334155;border-top:none;">
    <div style="font-size:11px;color:#475569;font-weight:700;">
      Reporte generado automáticamente el 1° de cada mes · Sistema Mercado Limpio · Facturación ARCA/AFIP
    </div>
    <div style="font-size:11px;color:#334155;margin-top:4px;">Solo para uso interno — distribuidoramercadolimpio@gmail.com</div>
  </div>

</div>
</body>
</html>`;
}

// --- ENVÍO DEL RESUMEN ---
async function enviarResumenMensual(anioForzar, mesForzar) {
  const hoy   = new Date(Date.now() - 3 * 60 * 60 * 1000); // hora AR
  const anio  = anioForzar || (hoy.getUTCMonth() === 0 ? hoy.getUTCFullYear() - 1 : hoy.getUTCFullYear());
  const mes   = mesForzar  || (hoy.getUTCMonth() === 0 ? 12 : hoy.getUTCMonth());
  const MESES = ["","Enero","Febrero","Marzo","Abril","Mayo","Junio","Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"];

  const facturas     = leerFacturasDelMes(anio, mes);
  const totalGeneral = facturas.reduce((a, f) => a + Number(f.total || 0), 0);
  const fmtAR        = n => new Intl.NumberFormat("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(n||0));

  console.log(`📊 [Resumen] ${MESES[mes]} ${anio}: ${facturas.length} facturas | $ ${fmtAR(totalGeneral)}`);

  const htmlMail = buildResumenHTMLProfesional(anio, mes, facturas);

  await transporter.sendMail({
    from:    `"Mercado Limpio" <${GMAIL_USER}>`,
    to:      "distribuidoramercadolimpio@gmail.com",
    subject: `📊 Resumen ${MESES[mes]} ${anio} — ${facturas.length} facturas | $ ${fmtAR(totalGeneral)}`,
    html:    htmlMail,
  });

  console.log(`✅ [Resumen] Email enviado a distribuidoramercadolimpio@gmail.com`);
}

// --- CRON: SE EJECUTA EL 1° DE CADA MES A LAS 08:00 (ARGENTINA) ---
let _ultimoResumenEnviado = null;
setInterval(async () => {
  try {
    const ahora = new Date(Date.now() - 3 * 60 * 60 * 1000); // UTC-3 Argentina
    const dia   = ahora.getUTCDate();
    const hora  = ahora.getUTCHours();
    const clave = `${ahora.getUTCFullYear()}-${String(ahora.getUTCMonth()+1).padStart(2,"0")}`;
    if (dia === 1 && hora === 8 && _ultimoResumenEnviado !== clave) {
      _ultimoResumenEnviado = clave;
      console.log(`🗓️  [Cron] Enviando resumen mensual automático...`);
      await enviarResumenMensual();
    }
  } catch (e) {
    console.error("❌ [Cron]", e?.message || e);
  }
}, 30 * 60 * 1000).unref?.(); // revisa cada 30 minutos

console.log("🗓️  [Cron] Resumen mensual activado → el 1° de cada mes a las 08:00 (AR) llega a distribuidoramercadolimpio@gmail.com");



// --- GUARDADO AUTOMÁTICO: se llama desde el endpoint /facturar ---
// YA INTEGRADO: buscá en el endpoint /facturar la línea:
//   resultados.push({ nroFactura: nro, cae: result.CAE, total: impTotal, pdfUrl: pdfPublicUrl });
// Y PEGÁ ESTO JUSTO ABAJO:
//
//   guardarFacturaEnDB({ cuitCliente, rec, nro, pv, cae: result.CAE, impTotal, pdfPublicUrl, condicionVenta, fecha, chunkItems });
//
// ================================================================
// FIN DEL MÓDULO
// ================================================================
