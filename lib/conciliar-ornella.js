/* ═══════════════════════════════════════════════════════════════════
   NO FACTURAR LO QUE YA FACTURÓ ORNELLA (24/11/2026)

   La cuenta única de Santander es compartida: entran los cobros de Mercado Limpio y los de los alquileres de Ornella
   (Tiempo Libre, monotributo CUIT 27-33589911-9, Factura C por cada pago, base Supabase `yckoaokosdoculwxuysd`).
   Al subir un extracto, toda transferencia que corresponda a un pago de Ornella se marca y NO se factura acá.

   Cómo se empareja (estudiado con los datos reales de octubre y noviembre 2026: 18 de 18 sin ambigüedad):
     · importe exacto (los alquileres repiten importes: $308.000, $336.000… de huéspedes distintos el mismo día);
     · fecha: la transferencia entre 7 días antes y 3 después de la fecha que Ornella cargó el pago;
     · documento: el DNI (o CUIT) del pagador que cargó Ornella está DENTRO del CUIT de quien transfiere.
     Un pago de Ornella se empareja con UNA sola transferencia (la del documento, la de fecha más cercana).
     Pago sin documento cargado y un único candidato por importe y fecha → «posible»: no se factura solo.

   Acceso: una función de SOLO LECTURA en la base de Ornella (`ml_pagos_para_conciliar`, ver sql/2026-11-24-ornella-*.sql),
   protegida con una clave (ORNELLA_CLAVE en Render). Devuelve solo fecha, importe, documento y comprobante de cada pago.
   Si no se puede consultar, quien llama decide: el análisis lo avisa y la facturación NO emite (nunca duplicar).
   ═══════════════════════════════════════════════════════════════════ */
"use strict";

const URL_ORNELLA = process.env.ORNELLA_SUPABASE_URL || "https://yckoaokosdoculwxuysd.supabase.co";
/* Clave PÚBLICA (publishable) del proyecto de Ornella: es la misma que lleva su app; sola no da acceso a nada (RLS). */
const KEY_ORNELLA = process.env.ORNELLA_SUPABASE_KEY || "sb_publishable_-pBMzF5VWVF1l8itYWqy8w_QhIy6lEz";
const ANTES = 7, DESPUES = 3;   // ventana de fecha: transferencia ∈ [pago − 7 días, pago + 3 días]

const soloDig = v => String(v || "").replace(/\D/g, "");
const diaMs = 86400000;
const dias = (a, b) => Math.round((Date.parse(a + "T12:00:00Z") - Date.parse(b + "T12:00:00Z")) / diaMs);
const sumarDias = (iso, n) => new Date(Date.parse(iso + "T12:00:00Z") + n * diaMs).toISOString().slice(0, 10);

/* El documento del pagador (DNI de 7-8 dígitos o CUIT de 11) ¿corresponde al CUIT de quien transfirió? */
function docCoincide(doc, cuit) {
  const d = soloDig(doc), c = soloDig(cuit);
  if (!d || c.length !== 11) return false;
  if (d.length === 11) return d === c;
  if (d.length >= 6 && d.length <= 8) return c.slice(2, 10).replace(/^0+/, "") === d.replace(/^0+/, "");
  return false;
}

/* Lectura (solo los pagos no-efectivo del rango, con sus facturas C). */
async function pagosOrnella(desde, hasta, opts = {}) {
  const clave = opts.clave !== undefined ? opts.clave : process.env.ORNELLA_CLAVE;
  if (!clave) throw new Error("falta configurar ORNELLA_CLAVE (conexión con la facturación de Ornella)");
  const f = opts.fetch || fetch;
  const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), opts.timeoutMs || 20000);
  try {
    const r = await f(`${URL_ORNELLA}/rest/v1/rpc/ml_pagos_para_conciliar`, {
      method: "POST", signal: ctl.signal,
      headers: { apikey: KEY_ORNELLA, Authorization: `Bearer ${KEY_ORNELLA}`, "Content-Type": "application/json" },
      body: JSON.stringify({ p_desde: desde, p_hasta: hasta, p_clave: clave }),
    });
    const j = await r.json().catch(() => null);
    if (!r.ok || !Array.isArray(j)) throw new Error((j && (j.message || j.hint)) || `HTTP ${r.status}`);
    return j.map(p => ({ id: p.pago_id, fecha: String(p.fecha).slice(0, 10), importe: Math.round(Number(p.importe) * 100) / 100,
      doc: soloDig(p.doc_nro), comprobantes: p.comprobantes || "", estados: p.estados || "" }));
  } finally { clearTimeout(tm); }
}

/* Empareja transferencias (con fecha, monto, cuit) con pagos de Ornella. Devuelve un arreglo paralelo: null o
   { tipo: "ornella" | "posible", comprobante, estado, pagoFecha, importe }. */
function emparejar(transferencias, pagos) {
  const res = transferencias.map(() => null);
  const usados = new Set();
  const cand = p => transferencias.map((t, i) => ({ t, i })).filter(({ t, i }) => res[i] === null && t.fecha && Math.abs(Math.abs(Number(t.monto)) - p.importe) < 0.01
    && dias(String(t.fecha).slice(0, 10), p.fecha) >= -ANTES && dias(String(t.fecha).slice(0, 10), p.fecha) <= DESPUES);
  const marca = (p, i, tipo) => { usados.add(p.id); res[i] = { tipo, comprobante: p.comprobantes || "", estado: p.estados || (p.comprobantes ? "" : "sin factura"), pagoFecha: p.fecha, importe: p.importe }; };
  /* 1) con documento: el que coincide, el de fecha más cercana */
  for (const p of pagos.filter(x => x.doc).sort((a, b) => a.fecha.localeCompare(b.fecha))) {
    const c = cand(p).filter(({ t }) => docCoincide(p.doc, t.cuit)).sort((a, b) => Math.abs(dias(a.t.fecha, p.fecha)) - Math.abs(dias(b.t.fecha, p.fecha)));
    if (c.length) marca(p, c[0].i, "ornella");
  }
  /* 2) sin documento: solo si el candidato es único → «posible» (no se factura sola) */
  for (const p of pagos.filter(x => !x.doc && !usados.has(x.id))) {
    const c = cand(p); if (c.length === 1) marca(p, c[0].i, "posible");
  }
  const sinTransferencia = pagos.filter(p => !usados.has(p.id));
  return { marcas: res, sinTransferencia };
}

/* Para una lista de transferencias: consulta el rango que cubren y empareja. Lanza si no se pudo consultar. */
async function verificar(transferencias, opts = {}) {
  const fechas = transferencias.map(t => String(t.fecha || "").slice(0, 10)).filter(f => /^\d{4}-\d{2}-\d{2}$/.test(f)).sort();
  if (!fechas.length) return { marcas: transferencias.map(() => null), sinTransferencia: [], consultado: null };
  const desde = sumarDias(fechas[0], -DESPUES), hasta = sumarDias(fechas[fechas.length - 1], ANTES);
  const pagos = await pagosOrnella(desde, hasta, opts);
  return Object.assign(emparejar(transferencias, pagos), { consultado: { desde, hasta, pagos: pagos.length } });
}

/* Qué hacer con una transferencia al FACTURAR (lo usa el trabajo del extracto antes de emitir). */
function decidir(t, marca, error) {
  if (error) return { emitir: false, resultado: { ok: false, error: "No se pudo verificar contra la facturación de Ornella (" + error + "). No se emitió: se puede reintentar." } };
  if (marca && marca.tipo === "ornella") return { emitir: false, resultado: { ok: true, skipped: true, ornella: true, comprobante: "Ornella " + (marca.comprobante || "(pago sin factura)"), motivo: "Ingreso de Ornella: lo factura ella" } };
  if (marca && marca.tipo === "posible" && !t.forzarOrnella) return { emitir: false, resultado: { ok: true, skipped: true, ornella: true, posible: true, comprobante: "¿Ornella?", motivo: "Posible ingreso de Ornella (pago sin documento): no se emitió; revisar" } };
  return { emitir: true };
}

module.exports = { docCoincide, emparejar, verificar, pagosOrnella, decidir, ANTES, DESPUES };
