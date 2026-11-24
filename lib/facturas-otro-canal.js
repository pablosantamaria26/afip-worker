/* ═══════════════════════════════════════════════════════════════════
   NO VOLVER A FACTURAR LO QUE YA SE FACTURÓ DESDE LA CAJA (o a mano) — 24/11/2026

   La Caja de Queruclor emite facturas en ARCA por remito (origen 'caja', con `remito_ref`) y también hay facturas manuales y
   por remito PDF. Todas quedan en la tabla `facturas` de Mercado Limpio y se ven en Facturación ML. Cuando después el cliente
   transfiere, esa transferencia aparece en el extracto SIN referencia bancaria en la factura: la deduplicación de siempre solo
   la reconoce si viene del mismo CUIT y por el importe exacto. Este módulo cubre lo que falta:

     · «caja» / «manual» (seguro, no se factura): varias transferencias del MISMO CUIT que juntas suman el total de una factura
       (pago en partes), además del importe exacto que ya cubría la deduplicación.
     · «posible» (no se factura sola; Pablo puede tildarla): el CUIT tiene una factura de la Caja/manual en el período pero el
       importe no cierra; o quien transfiere no es el CUIT facturado, pero su nombre coincide con el EMAIL de la factura (el caso
       real: FRAGANCIAS facturada por la Caja con el email de Carlos Giménez, que paga desde su propio CUIT).

   Fuente de verdad: SOLO facturas emitidas en ARCA (tabla `facturas`), nunca cobros ni recibos de la Caja (pedido de Pablo).
   Si la tabla no se puede consultar, quien llama no emite (nunca duplicar).
   ═══════════════════════════════════════════════════════════════════ */
"use strict";

const DIAS_ANTES = 60;    // una factura de cuenta corriente se paga hasta semanas DESPUÉS de emitida
const DIAS_DESPUES = 45;  // o el cliente paga ANTES y la Caja factura después (caso real: pago el 09/11, factura el 20/11)
const TOL = 2;           // tolerancia en pesos (igual que la deduplicación existente)
const MAX_SUBCONJUNTO = 12;

const r2 = n => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
const soloDig = v => String(v || "").replace(/\D/g, "");
const diaMs = 86400000;
const sumarDias = (iso, n) => new Date(Date.parse(iso + "T12:00:00Z") + n * diaMs).toISOString().slice(0, 10);
const quitarAcentos = s => String(s || "").normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase();

/* Facturas que NO vinieron del extracto, vigentes, del rango de fechas. */
async function cargarFacturasOtroCanal(supabase, desde, hasta) {
  const { data, error } = await supabase.from("facturas")
    .select("comprobante, cuit_cliente, nombre_cliente, total, fecha, origen, remito_ref, condicion_venta, email_to, email_error, referencia_bancaria")
    .gte("fecha", desde).lte("fecha", hasta).gt("total", 0).order("fecha");
  if (error) throw new Error("No se pudo consultar las facturas de la Caja/manuales: " + (error.message || error));
  return (data || [])
    .filter(f => !String(f.referencia_bancaria || "").trim() && !/EXTRACTO/i.test(String(f.condicion_venta || "")) && !/ANULADA POR/i.test(String(f.email_error || "")))
    .map(f => ({ comprobante: f.comprobante, cuit: soloDig(f.cuit_cliente), nombre: String(f.nombre_cliente || ""), total: r2(f.total), fecha: String(f.fecha).slice(0, 10),
      origen: f.origen === "caja" ? "caja" : "manual", remito: f.remito_ref || "", email: String(f.email_to || "").trim().toLowerCase() }));
}

/* ¿El nombre de quien transfiere está en el email de la factura? (dos palabras de 3+ letras, o apellido+nombre pegados) */
function nombreEnEmail(nombre, email) {
  const local = quitarAcentos(email).split("@")[0].replace(/[^a-z]/g, "");
  if (local.length < 6) return false;
  const palabras = quitarAcentos(nombre).replace(/[^a-z\s]/g, " ").split(/\s+/).filter(p => p.length >= 3 && !/^(del|los|las|von|van|der|srl|sa)$/.test(p));
  const presentes = palabras.filter(p => local.includes(p));
  return presentes.length >= 2 || (palabras.length === 1 && palabras[0].length >= 6 && local.includes(palabras[0]));
}

/* Subconjunto de importes que suma `objetivo` (±TOL). Devuelve índices o null. */
function subconjuntoQueSuma(importes, objetivo) {
  const n = Math.min(importes.length, MAX_SUBCONJUNTO);
  for (let mask = 1; mask < (1 << n); mask++) {
    let s = 0, k = 0; const idx = [];
    for (let i = 0; i < n; i++) if (mask & (1 << i)) { s += importes[i]; idx.push(i); k++; }
    if (k >= 2 && Math.abs(s - objetivo) <= TOL) return idx;
  }
  return null;
}

/* marcas[i] = null | { tipo: "caja"|"manual"|"posible", comprobante, total, origen, motivo } */
function emparejar(transferencias, facturas) {
  const marcas = transferencias.map(() => null);
  const enVentana = (t, f) => { const d = (Date.parse(String(t.fecha).slice(0, 10) + "T12:00:00Z") - Date.parse(f.fecha + "T12:00:00Z")) / diaMs; return d >= -DIAS_DESPUES && d <= DIAS_ANTES; };
  const libres = f => transferencias.map((t, i) => ({ t, i })).filter(({ t, i }) => marcas[i] === null && t.fecha && soloDig(t.cuit) === f.cuit && enVentana(t, f));
  const marcar = (f, i, tipo, motivo) => { marcas[i] = { tipo, comprobante: f.comprobante, total: f.total, origen: f.origen, remito: f.remito, motivo }; };
  const usadas = new Set();
  /* 1) importe exacto (mismo CUIT) */
  for (const f of facturas) { const c = libres(f).find(({ t }) => Math.abs(Math.abs(Number(t.monto)) - f.total) <= TOL); if (c) { marcar(f, c.i, f.origen, "mismo CUIT e importe"); usadas.add(f.comprobante); } }
  /* 2) pago en partes: varias del mismo CUIT suman el total */
  for (const f of facturas.filter(x => !usadas.has(x.comprobante))) {
    const c = libres(f); const idx = subconjuntoQueSuma(c.map(({ t }) => Math.abs(Number(t.monto))), f.total);
    if (idx) { idx.forEach(k => marcar(f, c[k].i, f.origen, "pago en partes: suman el total de la factura")); usadas.add(f.comprobante); }
  }
  /* 3) mismo CUIT, importe que no cierra → posible */
  for (const f of facturas.filter(x => !usadas.has(x.comprobante))) for (const { i } of libres(f)) marcar(f, i, "posible", "mismo CUIT que una factura de " + (f.origen === "caja" ? "la Caja" : "otro canal") + " del período, pero el importe no coincide");
  /* 4) otro CUIT, pero el nombre de quien transfiere está en el email de la factura → posible */
  for (const f of facturas.filter(x => x.email)) transferencias.forEach((t, i) => { if (marcas[i] === null && t.fecha && enVentana(t, f) && soloDig(t.cuit) !== f.cuit && nombreEnEmail(t.nombre, f.email)) marcar(f, i, "posible", "quien transfiere coincide con el email de la factura " + f.comprobante + " (" + f.nombre + ")"); });
  return { marcas, facturas: facturas.length };
}

async function verificar(supabase, transferencias) {
  const fechas = transferencias.map(t => String(t.fecha || "").slice(0, 10)).filter(f => /^\d{4}-\d{2}-\d{2}$/.test(f)).sort();
  if (!fechas.length || !supabase) return { marcas: transferencias.map(() => null), facturas: 0, consultado: null };
  const desde = sumarDias(fechas[0], -DIAS_ANTES), hasta = sumarDias(fechas[fechas.length - 1], DIAS_DESPUES);
  const facturas = await cargarFacturasOtroCanal(supabase, desde, hasta);
  return Object.assign(emparejar(transferencias, facturas), { consultado: { desde, hasta } });
}

/* Qué hacer al FACTURAR con una transferencia. */
function decidir(t, marca, error) {
  if (error) return { emitir: false, resultado: { ok: false, error: "No se pudo verificar contra las facturas de la Caja (" + error + "). No se emitió: se puede reintentar." } };
  if (marca && marca.tipo !== "posible") return { emitir: false, resultado: { ok: true, skipped: true, otroCanal: true, comprobante: marca.comprobante, total: marca.total, motivo: "Ya facturada desde " + (marca.tipo === "caja" ? "la Caja" : "otro canal") + " (" + marca.motivo + ")" } };
  if (marca && marca.tipo === "posible" && !t.forzarCaja) return { emitir: false, resultado: { ok: true, skipped: true, otroCanal: true, posible: true, comprobante: "¿" + marca.comprobante + "?", total: marca.total, motivo: "Posible pago de una factura ya emitida: " + marca.motivo + ". No se emitió; revisar" } };
  return { emitir: true };
}

module.exports = { cargarFacturasOtroCanal, emparejar, verificar, decidir, nombreEnEmail, subconjuntoQueSuma, DIAS_ANTES, DIAS_DESPUES, TOL };
