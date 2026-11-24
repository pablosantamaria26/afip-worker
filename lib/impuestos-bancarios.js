/* ═══════════════════════════════════════════════════════════════════
   RETENCIONES, PERCEPCIONES E IMPUESTOS BANCARIOS — 24/11/2026

   En cada corrida del extracto, además de lo facturado, el contador recibe una hoja «Retenciones e Impuestos Banco» con todo lo
   que el banco retuvo, percibió o cobró (y los pagos a ARBA y AFIP hechos desde la cuenta), para deducir en las DJ de ARBA y ARCA.

     · Lectura: del MISMO Excel «Últimos movimientos» de Santander que ya se sube (columnas: [1] fecha, [3] descripción,
       [4] referencia, [5] Caja de Ahorro / [6] Cuenta Corriente). Sin IA: catálogo fijo de conceptos. Lo que tiene forma de
       impuesto pero no está en el catálogo se informa aparte («a revisar»), nunca se pierde.
     · Sin duplicados entre corridas (la del 15 y la del 30, o un mismo archivo subido dos veces): cada movimiento tiene una
       huella (fecha | referencia del banco | importe | régimen) y se registra como ENVIADO recién cuando el email salió bien.
       El registro vive en un almacenamiento PRIVADO de Supabase (bucket «registros-internos», un archivo por mes).
     · Arranque: solo movimientos desde IMPUESTOS_DESDE (por defecto 2026-11-01, decisión de Pablo).
   ═══════════════════════════════════════════════════════════════════ */
"use strict";

const CUIT_TITULAR = "23332382314";
const DESDE = process.env.IMPUESTOS_DESDE || "2026-11-01";
const BUCKET = "registros-internos";
const CARPETA = "impuestos-bancarios";

/* Catálogo. El orden importa: lo más específico primero. tipo: «retencion» (deducible) o «pago» (informativo). */
const REGIMENES = [
  { id: "ARBA_REC", tipo: "retencion", nombre: "ARBA · Recaudación bancaria IIBB", corto: "ARBA recaudación", re: /retenci[oó]n\s+arba|recaudaci[oó]n\s+arba/i,
    color: "FF1F5FA8", fondo: "FFDCE8F7", destino: "DJ Ingresos Brutos ARBA: se deduce como recaudación bancaria sufrida del período." },
  { id: "SIRCREB", tipo: "retencion", nombre: "SIRCREB · Recaudación IIBB (Convenio Multilateral)", corto: "SIRCREB", re: /sircreb/i,
    color: "FF6B3FA0", fondo: "FFEAE1F5", destino: "DJ Convenio Multilateral (SIFERE): se deduce como recaudación bancaria SIRCREB." },
  { id: "PERC_IVA", tipo: "retencion", nombre: "Percepciones de IVA", corto: "Percepción IVA", re: /percep\w*\s+(de\s+)?i\.?v\.?a|i\.?v\.?a\.?\s+percep|rg\s*2408/i,
    color: "FF2E7D4F", fondo: "FFDDF1E4", destino: "DJ IVA (ARCA): se computa como percepción sufrida." },
  { id: "LEY25413_CR", tipo: "retencion", nombre: "Impuesto Ley 25.413 · créditos", corto: "Ley 25.413 créd.", re: /25\.?413.*cr[eé]d/i,
    color: "FFB4651E", fondo: "FFFBE7D3", destino: "Pago a cuenta de Ganancias: el porcentaje computable lo define el contador según la categoría MiPyME." },
  { id: "LEY25413_DB", tipo: "retencion", nombre: "Impuesto Ley 25.413 · débitos", corto: "Ley 25.413 déb.", re: /25\.?413/i,
    color: "FFC97A3A", fondo: "FFFDF0E2", destino: "Pago a cuenta de Ganancias: el porcentaje computable lo define el contador según la categoría MiPyME." },
  { id: "PAGO_AFIP", tipo: "pago", nombre: "Pagos a AFIP / ARCA desde la cuenta", corto: "Pago AFIP", re: /imp\.?\s*afip|d[eé]bito\s+autom[aá]tico\s+afip|\bvep\b|pago\s+.*\barca\b/i,
    color: "FF0E6E78", fondo: "FFDDF2F3", destino: "Informativo: obligaciones pagadas a AFIP/ARCA. Se indica el CUIT de la obligación (puede no ser el del titular)." },
  { id: "PAGO_ARBA", tipo: "pago", nombre: "Pagos a ARBA desde la cuenta", corto: "Pago ARBA", re: /pago\s+de\s+servicios\s+arba|d[eé]bito\s+autom[aá]tico\s+arba/i,
    color: "FF5B6B7C", fondo: "FFEEF1F4", destino: "Informativo: anticipos/obligaciones pagados a ARBA (débito automático). No es una retención." },
];
const RG = Object.fromEntries(REGIMENES.map(r => [r.id, r]));
const PARECE_IMPUESTO = /impuesto|reten|percep|recaudaci|sellos|ganancias|\biibb\b|\bi\.?v\.?a\b|agip|afip|arca\b|arba|\brg\s*\d/i;
const NO_ES = /transferencia|transf\b|pago con transferencia|compra con tarjeta/i;

const r2 = n => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
const num = v => { const s = String(v ?? "").trim(); if (!s) return null; const n = s.includes(",") ? parseFloat(s.replace(/\./g, "").replace(",", ".")) : parseFloat(s); return Number.isFinite(n) ? n : null; };
const claveDe = m => [m.fecha, m.referencia, r2(m.importe).toFixed(2), m.regimen].join("|");

/* ── Lectura: filas de sheet_to_json(header:1) del extracto ── */
function leerImpuestosExtracto(rows) {
  let cuenta = "", periodo = "", inicio = -1;
  rows.forEach((r, i) => {
    const a = String(r[1] ?? "").trim().toLowerCase();
    if (a === "cuenta") cuenta = String(r[2] ?? "").replace(/�/g, "ú").trim();
    if (a === "fecha" && / - /.test(String(r[2] ?? ""))) periodo = String(r[2]).trim();
    if (inicio < 0 && a === "fecha" && String(r[3] ?? "").toLowerCase().startsWith("descripci")) inicio = i + 1;
  });
  const movimientos = [], aRevisar = [];
  let anteriores = 0;
  for (let i = Math.max(inicio, 0); i < rows.length; i++) {
    const r = rows[i]; const fm = String(r[1] ?? "").match(/(\d{2})\/(\d{2})\/(\d{4})/); if (!fm) continue;
    const desc = String(r[3] ?? "").replace(/\s+/g, " ").trim(), ref = String(r[4] ?? "").trim();
    const crudo = num(r[5]) ?? num(r[6]); if (crudo == null) continue;
    const fecha = `${fm[3]}-${fm[2]}-${fm[1]}`;
    const rg = REGIMENES.find(x => x.re.test(desc));
    if (!rg) { if (PARECE_IMPUESTO.test(desc) && !NO_ES.test(desc)) aRevisar.push({ fecha, descripcion: desc, referencia: ref, importe: r2(-crudo) }); continue; }
    if (fecha < DESDE) { anteriores++; continue; }
    const m = desc.match(/([\d,]+)\s*%\s*sobre\s*\$\s*([\d.,]+)/i);
    const alicuota = m ? num(m[1]) : null, base = m ? num(m[2]) : null;
    const importe = r2(-crudo);                      // en positivo: lo que el banco retuvo / se pagó (una devolución queda negativa)
    const mov = { fecha, periodo: fecha.slice(0, 7), regimen: rg.id, descripcion: desc, referencia: ref, base, alicuota, importe,
      control: base != null && alicuota != null ? r2(r2(base * alicuota / 100) - importe) : null };
    if (rg.id === "PAGO_AFIP") { const c = (desc.match(/(\d{11})/) || [])[1] || ""; mov.cuitPagado = c; mov.propio = c === CUIT_TITULAR; }
    if (rg.id === "PAGO_ARBA") mov.objeto = (desc.match(/(\d{8,})/) || [])[1] || "";
    mov.clave = claveDe(mov);
    movimientos.push(mov);
  }
  /* El mismo movimiento no puede venir dos veces en un archivo; si viniera, se cuenta una. */
  const vistos = new Set(); const unicos = movimientos.filter(m => !vistos.has(m.clave) && vistos.add(m.clave));
  return { cuenta, periodoExtracto: periodo, movimientos: unicos, aRevisar, anteriores, desde: DESDE };
}

/* La app devuelve los movimientos al pedir facturar: se validan y se vuelve a calcular la huella (nada viene «armado» de afuera). */
function normalizarMovimientos(lista) {
  const out = [];
  for (const x of Array.isArray(lista) ? lista : []) {
    if (!x || !RG[x.regimen] || !/^\d{4}-\d{2}-\d{2}$/.test(String(x.fecha)) || String(x.fecha) < DESDE) continue;
    const importe = r2(Number(x.importe)); if (!Number.isFinite(importe) || importe === 0) continue;
    const m = { fecha: x.fecha, periodo: String(x.fecha).slice(0, 7), regimen: x.regimen, descripcion: String(x.descripcion || "").slice(0, 300),
      referencia: String(x.referencia || "").slice(0, 40), base: x.base == null ? null : r2(x.base), alicuota: x.alicuota == null ? null : Number(x.alicuota), importe,
      control: x.control == null ? null : r2(x.control) };
    if (x.regimen === "PAGO_AFIP") { m.cuitPagado = String(x.cuitPagado || "").replace(/\D/g, "").slice(0, 11); m.propio = m.cuitPagado === CUIT_TITULAR; }
    if (x.regimen === "PAGO_ARBA") m.objeto = String(x.objeto || "").replace(/\D/g, "").slice(0, 20);
    m.clave = claveDe(m); out.push(m);
  }
  const vistos = new Set(); return out.filter(m => !vistos.has(m.clave) && vistos.add(m.clave));
}

/* ── Registro de lo ENVIADO (privado) ── */
async function almacen(sb) {
  const st = sb.storage;
  const { data } = await st.getBucket(BUCKET);
  if (!data) { const { error } = await st.createBucket(BUCKET, { public: false }); if (error && !/exist/i.test(error.message || "")) throw new Error("No se pudo crear el registro privado: " + error.message); }
  return st.from(BUCKET);
}
async function leerMes(b, periodo) {
  const { data, error } = await b.download(`${CARPETA}/${periodo}.json`);
  if (error || !data) return { periodo, movimientos: {} };
  return JSON.parse(await data.text());
}
/* Todo lo enviado (todos los meses registrados): para separar nuevos / ya enviados y para el resumen por período. */
async function cargarRegistro(sb) {
  const b = await almacen(sb);
  const { data, error } = await b.list(CARPETA, { limit: 1000 });
  if (error) throw new Error("No se pudo leer el registro de lo enviado: " + error.message);
  const enviados = new Map();
  for (const o of data || []) { const m = /^(\d{4}-\d{2})\.json$/.exec(o.name || ""); if (!m) continue; const mes = await leerMes(b, m[1]); for (const [k, v] of Object.entries(mes.movimientos || {})) enviados.set(k, v); }
  return enviados;
}
async function registrarEnviados(sb, movs, corrida) {
  if (!movs.length) return 0;
  const b = await almacen(sb);
  const porMes = {}; movs.forEach(m => (porMes[m.periodo] = porMes[m.periodo] || []).push(m));
  for (const [periodo, lista] of Object.entries(porMes)) {
    const mes = await leerMes(b, periodo);
    for (const m of lista) if (!mes.movimientos[m.clave]) mes.movimientos[m.clave] = Object.assign({}, m, { enviado_en: corrida.en, corrida: corrida.id });
    const { error } = await b.upload(`${CARPETA}/${periodo}.json`, Buffer.from(JSON.stringify(mes)), { contentType: "application/json", upsert: true });
    if (error) throw new Error("No se pudo registrar lo enviado de " + periodo + ": " + error.message);
  }
  return movs.length;
}

/* Separa lo nuevo de lo ya informado. Si el registro no se pudo leer, todo va como «sin verificar» (se avisa en la hoja). */
async function clasificar(sb, movimientos) {
  try {
    const enviados = sb ? await cargarRegistro(sb) : new Map();
    const nuevos = movimientos.filter(m => !enviados.has(m.clave));
    const yaEnviados = movimientos.filter(m => enviados.has(m.clave)).map(m => Object.assign({}, m, { enviado_en: enviados.get(m.clave).enviado_en }));
    return { nuevos, yaEnviados, historial: [...enviados.values(), ...nuevos], aviso: sb ? "" : "Sin base de datos: no se pudo verificar lo ya enviado." };
  } catch (e) {
    return { nuevos: movimientos, yaEnviados: [], historial: movimientos, aviso: "No se pudo verificar qué se informó en corridas anteriores (" + e.message + "). Puede repetir movimientos ya enviados." };
  }
}

/* ── Hoja del Excel ── */
const MESES = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"];
const nomPer = p => MESES[Number(p.slice(5, 7))] + " " + p.slice(0, 4);
const fAR = iso => String(iso).slice(0, 10).split("-").reverse().join("/");
const suma = (arr, f) => r2(arr.filter(f || (() => true)).reduce((s, x) => s + x.importe, 0));
const fmtCuit = c => (c && c.length === 11 ? `${c.slice(0, 2)}-${c.slice(2, 10)}-${c.slice(10)}` : c || "");
const C = { navy: "FF1E3A5F", navyDark: "FF142A47", amber: "FFC97A3A", white: "FFFFFFFF", text: "FF1A2530", muted: "FF6B7F94", alt: "FFF4F7FA", line: "FFD8E3EE",
  okBg: "FFE2F0D9", okFg: "FF1F4E2B", warnBg: "FFFFF2CC", warnFg: "FF7F6000", gray: "FF9AA7B4", grayBg: "FFF3F5F7", nuevoBg: "FFDCE8F7", nuevoFg: "FF1F5FA8" };
const MONEY = '"$" #,##0.00;[Red]-"$" #,##0.00;"—"';

/* datos = { cuenta, periodoExtracto, fechaCorrida, nuevos, yaEnviados, historial, aRevisar, anteriores, aviso } */
function agregarHojaImpuestos(wb, datos) {
  const { nuevos = [], yaEnviados = [], historial = [], aRevisar = [], anteriores = 0, aviso = "" } = datos;
  const leido = [...nuevos, ...yaEnviados];
  const periodoAct = (leido.map(m => m.periodo).sort().pop()) || String(datos.fechaCorrida || "").slice(0, 7);
  const fill = argb => ({ type: "pattern", pattern: "solid", fgColor: { argb } });
  const borde = { top: { style: "thin", color: { argb: C.line } }, bottom: { style: "thin", color: { argb: C.line } }, left: { style: "thin", color: { argb: C.line } }, right: { style: "thin", color: { argb: C.line } } };
  const ws = wb.addWorksheet("Retenciones e Impuestos Banco", { properties: { tabColor: { argb: "FF1F5FA8" } }, views: [{ state: "frozen", ySplit: 3, showGridLines: false }],
    pageSetup: { orientation: "landscape", fitToPage: true, fitToWidth: 1, fitToHeight: 0, paperSize: 9, margins: { left: 0.4, right: 0.4, top: 0.5, bottom: 0.5, header: 0.2, footer: 0.2 } } });
  ws.columns = [{ width: 16 }, { width: 17 }, { width: 22 }, { width: 38 }, { width: 15 }, { width: 17 }, { width: 17 }, { width: 17 }, { width: 18 }, { width: 16 }];
  const N = 10; let fila = 4;
  const vacia = h => { ws.getRow(fila).height = h || 8; fila++; };
  const linea = (texto, font, h) => { ws.mergeCells(fila, 1, fila, N); const c = ws.getCell(fila, 1); c.value = texto; c.font = font; c.alignment = { indent: 1, wrapText: true, vertical: "middle" }; ws.getRow(fila).height = h || 18; fila++; return c; };
  const seccion = (n, texto, sub) => { vacia(14); const c = linea(`${n} · ${texto}`, { bold: true, size: 12, color: { argb: C.white } }, 24); c.fill = fill(C.navyDark); if (sub) linea(sub, { italic: true, size: 9, color: { argb: C.muted } }, 18); };
  const cabecera = (textos, colores) => { const r = ws.getRow(fila); textos.forEach((t, i) => { const c = r.getCell(i + 1); c.value = t; c.font = { bold: true, size: 9, color: { argb: C.white } }; c.fill = fill((colores && colores[i]) || C.navy); c.alignment = { horizontal: i >= 5 ? "center" : "left", vertical: "middle", wrapText: true }; c.border = borde; }); r.height = 30; fila++; };

  /* Encabezado */
  ws.mergeCells(1, 1, 1, N); const t = ws.getCell(1, 1); t.value = "RETENCIONES, PERCEPCIONES E IMPUESTOS BANCARIOS"; t.fill = fill(C.navy); t.font = { bold: true, size: 18, color: { argb: C.white } }; t.alignment = { vertical: "middle", indent: 1 }; ws.getRow(1).height = 40;
  ws.mergeCells(2, 1, 2, N); const s2 = ws.getCell(2, 1);
  s2.value = `Santander · ${datos.cuenta || "cuenta bancaria"} · Titular CUIT ${fmtCuit(CUIT_TITULAR)} · Corrida del ${fAR(datos.fechaCorrida)} · Extracto leído: ${datos.periodoExtracto || "—"}`;
  s2.fill = fill(C.navy); s2.font = { size: 10, color: { argb: "FFC9D6E3" } }; s2.alignment = { indent: 1, vertical: "middle" }; ws.getRow(2).height = 20;
  ws.mergeCells(3, 1, 3, N); ws.getCell(3, 1).fill = fill(C.amber); ws.getRow(3).height = 4;
  if (aviso) { vacia(6); const c = linea("⚠ " + aviso, { bold: true, size: 10, color: { argb: C.warnFg } }, 26); c.fill = fill(C.warnBg); }

  /* Indicadores */
  vacia(10);
  const delMes = historial.filter(m => m.periodo === periodoAct);
  const kpis = [
    ["NUEVO EN ESTA CORRIDA", suma(nuevos, m => RG[m.regimen].tipo === "retencion"), `${nuevos.length} movimientos nuevos`, C.nuevoFg],
    ["RETENIDO · " + nomPer(periodoAct).toUpperCase(), suma(delMes, m => RG[m.regimen].tipo === "retencion"), "retenciones, percepciones e impuestos", C.navy],
    ["ARBA RECAUDACIÓN · MES", suma(delMes, m => m.regimen === "ARBA_REC"), "para la DJ de IIBB", RG.ARBA_REC.color],
    ["LEY 25.413 · MES", suma(delMes, m => /^LEY25413/.test(m.regimen)), "pago a cuenta Ganancias", RG.LEY25413_DB.color],
    ["PAGOS ARBA + AFIP · MES", suma(delMes, m => RG[m.regimen].tipo === "pago"), "informativo", RG.PAGO_AFIP.color],
  ];
  const pos = [[1, 2], [3, 3], [4, 5], [6, 7], [8, 10]];
  [0, 1, 2].forEach(k => { ws.getRow(fila + k).height = k === 1 ? 30 : 18; });
  kpis.forEach(([tit, val, sub, color], i) => {
    const [a, b] = pos[i];
    for (let k = 0; k < 3; k++) { ws.mergeCells(fila + k, a, fila + k, b); const c = ws.getCell(fila + k, a); c.fill = fill(k === 0 ? color : C.alt); c.border = { left: { style: "thick", color: { argb: color } } }; c.alignment = { horizontal: "center", vertical: "middle", wrapText: true };
      if (k === 0) { c.value = tit; c.font = { bold: true, size: 9, color: { argb: C.white } }; }
      if (k === 1) { c.value = val; c.numFmt = MONEY; c.font = { bold: true, size: 16, color: { argb: color } }; }
      if (k === 2) { c.value = sub; c.font = { size: 8, italic: true, color: { argb: C.muted } }; } }
  });
  fila += 3;

  /* 1. Resumen por período */
  const COLS = ["ARBA_REC", "SIRCREB", "PERC_IVA", "LEY25413_DB", "LEY25413_CR"];
  seccion(1, "RESUMEN POR PERÍODO Y RÉGIMEN", `Lo informado desde ${fAR(DESDE)}, por mes del movimiento. «Total retenido» suma retenciones, percepciones e impuestos (deducibles); los pagos van aparte. Los totales son fórmulas.`);
  cabecera(["Período", "ARBA recaudación", "SIRCREB", "Percepción IVA", "Ley 25.413 déb.", "Ley 25.413 créd.", "Total retenido", "Pagos ARBA", "Pagos AFIP", "Movim."],
    [C.navy, RG.ARBA_REC.color, RG.SIRCREB.color, RG.PERC_IVA.color, RG.LEY25413_DB.color, RG.LEY25413_CR.color, C.navyDark, RG.PAGO_ARBA.color, RG.PAGO_AFIP.color, C.navy]);
  const periodos = [...new Set(historial.map(m => m.periodo))].sort();
  const ini = fila;
  periodos.forEach((p, i) => {
    const r = ws.getRow(fila), ms = historial.filter(m => m.periodo === p);
    r.getCell(1).value = nomPer(p);
    COLS.forEach((id, k) => { r.getCell(2 + k).value = suma(ms, m => m.regimen === id) || null; });
    r.getCell(7).value = { formula: `SUM(B${fila}:F${fila})` };
    r.getCell(8).value = suma(ms, m => m.regimen === "PAGO_ARBA") || null; r.getCell(9).value = suma(ms, m => m.regimen === "PAGO_AFIP") || null; r.getCell(10).value = ms.length;
    for (let c = 1; c <= N; c++) { const x = r.getCell(c); x.border = borde; x.fill = fill(p === periodoAct ? "FFEAF2FB" : i % 2 ? C.alt : C.white); x.font = { size: 10, bold: c === 7 || p === periodoAct, color: { argb: C.text } }; if (c >= 2 && c <= 9) x.numFmt = MONEY; if (c === 10) x.alignment = { horizontal: "center" }; }
    r.height = 18; fila++;
  });
  if (!periodos.length) { linea("Sin movimientos impositivos informados todavía.", { italic: true, size: 10, color: { argb: C.muted } }); }
  else {
    const rt = ws.getRow(fila); rt.getCell(1).value = "TOTAL";
    for (let c = 2; c <= 10; c++) { const L = String.fromCharCode(64 + c); rt.getCell(c).value = { formula: `SUM(${L}${ini}:${L}${fila - 1})` }; }
    for (let c = 1; c <= N; c++) { const x = rt.getCell(c); x.fill = fill(C.okBg); x.font = { bold: true, size: 11, color: { argb: C.okFg } }; x.border = { top: { style: "medium", color: { argb: C.okFg } }, bottom: { style: "medium", color: { argb: C.okFg } } }; if (c >= 2 && c <= 9) x.numFmt = MONEY; if (c === 10) x.alignment = { horizontal: "center" }; }
    rt.height = 22; fila++;
  }

  /* 2. Dónde se computa */
  seccion(2, "DÓNDE SE COMPUTA CADA CONCEPTO", "Guía rápida. La imputación final la define el contador.");
  cabecera(["Régimen", "", "", "Dónde se computa / para qué sirve", "", "", "", "Total " + nomPer(periodoAct), "Total informado", ""]);
  ws.mergeCells(fila - 1, 1, fila - 1, 3); ws.mergeCells(fila - 1, 4, fila - 1, 7); ws.mergeCells(fila - 1, 9, fila - 1, 10);
  for (const rg of REGIMENES) {
    const r = ws.getRow(fila); ws.mergeCells(fila, 1, fila, 3); ws.mergeCells(fila, 4, fila, 7); ws.mergeCells(fila, 9, fila, 10);
    r.getCell(1).value = rg.nombre; r.getCell(1).fill = fill(rg.fondo); r.getCell(1).font = { bold: true, size: 10, color: { argb: rg.color } };
    r.getCell(4).value = rg.destino; r.getCell(4).font = { size: 9, color: { argb: C.text } };
    r.getCell(8).value = suma(delMes, m => m.regimen === rg.id); r.getCell(9).value = suma(historial, m => m.regimen === rg.id);
    [8, 9].forEach(c => { r.getCell(c).numFmt = MONEY; r.getCell(c).font = { bold: true, size: 10, color: { argb: C.text } }; });
    for (let c = 1; c <= N; c++) { r.getCell(c).border = borde; r.getCell(c).alignment = Object.assign({ vertical: "middle", wrapText: true }, c === 1 || c === 4 ? { indent: 1 } : {}); }
    r.height = 30; fila++;
  }

  /* 3 y 4. Detalle */
  const detalle = (lista, gris) => {
    cabecera(["Fecha", "Período", "Régimen", "Descripción del banco", "Referencia", "Base imponible", "Alícuota", "Importe", "Control / CUIT", gris ? "Informado el" : "Estado"]);
    const desde = fila;
    const orden = lista.slice().sort((a, b) => (RG[a.regimen].tipo === RG[b.regimen].tipo ? 0 : RG[a.regimen].tipo === "retencion" ? -1 : 1) || a.fecha.localeCompare(b.fecha) || a.referencia.localeCompare(b.referencia));
    orden.forEach((m, i) => {
      const rg = RG[m.regimen], r = ws.getRow(fila);
      const ok = m.control == null ? null : Math.abs(m.control) <= 0.05;
      const ctrl = m.regimen === "PAGO_AFIP" ? `CUIT ${fmtCuit(m.cuitPagado)}${m.propio ? " (propio)" : " (otro CUIT)"}`
        : m.regimen === "PAGO_ARBA" ? (m.objeto ? "Obj. " + m.objeto : "") : m.control == null ? "sin base" : ok ? "✓ base × alíc." : "⚠ dif. $" + m.control.toFixed(2);
      r.values = [fAR(m.fecha), m.periodo, rg.corto, m.descripcion, m.referencia, m.base, m.alicuota != null ? m.alicuota / 100 : null, m.importe, ctrl, gris ? fAR(m.enviado_en) : "NUEVO"];
      for (let c = 1; c <= N; c++) { const x = r.getCell(c); x.border = borde; x.font = { size: 9, color: { argb: gris ? C.gray : C.text }, italic: !!gris }; x.fill = fill(i % 2 ? C.alt : C.white); x.alignment = { vertical: "middle", wrapText: c === 4 || c === 3 || c === 9 }; }
      r.getCell(3).fill = fill(gris ? C.grayBg : rg.fondo); r.getCell(3).font = { bold: true, size: 9, color: { argb: gris ? C.gray : rg.color } };
      [6, 8].forEach(c => { r.getCell(c).numFmt = MONEY; }); r.getCell(7).numFmt = "0.00%"; r.getCell(8).font = { bold: true, size: 10, color: { argb: gris ? C.gray : C.text } };
      r.getCell(9).font = { size: 9, color: { argb: ok === false ? C.warnFg : gris ? C.gray : m.regimen === "PAGO_AFIP" && !m.propio ? C.warnFg : C.okFg } }; if (ok === false) r.getCell(9).fill = fill(C.warnBg);
      if (!gris) { r.getCell(10).fill = fill(C.nuevoBg); r.getCell(10).font = { bold: true, size: 9, color: { argb: C.nuevoFg } }; }
      [2, 5, 7, 10].forEach(c => { r.getCell(c).alignment = { horizontal: "center", vertical: "middle" }; });
      r.height = 32; if (gris) r.outlineLevel = 1; fila++;
    });
    for (const id of REGIMENES.map(x => x.id).filter(id => orden.some(m => m.regimen === id))) {
      const rg = RG[id], r = ws.getRow(fila); ws.mergeCells(fila, 1, fila, 7);
      r.getCell(1).value = "Subtotal " + rg.corto; r.getCell(1).alignment = { horizontal: "right" };
      r.getCell(8).value = { formula: `SUMIF(C${desde}:C${fila - 1},"${rg.corto}",H${desde}:H${fila - 1})` }; r.getCell(8).numFmt = MONEY;
      [1, 8].forEach(c => { r.getCell(c).font = { bold: true, size: 9, color: { argb: gris ? C.gray : rg.color } }; r.getCell(c).fill = fill(gris ? C.grayBg : rg.fondo); });
      if (gris) r.outlineLevel = 1; fila++;
    }
  };
  seccion(3, "DETALLE · NUEVOS EN ESTA CORRIDA", "Movimientos que no se habían informado antes. Primero retenciones, percepciones e impuestos; después los pagos a ARBA y AFIP.");
  if (nuevos.length) {
    detalle(nuevos, false);
    for (const [txt, val, bg, fg] of [[`TOTAL RETENIDO NUEVO (${nuevos.filter(m => RG[m.regimen].tipo === "retencion").length} movimientos)`, suma(nuevos, m => RG[m.regimen].tipo === "retencion"), C.okBg, C.okFg],
      [`TOTAL PAGOS NUEVOS (${nuevos.filter(m => RG[m.regimen].tipo === "pago").length} movimientos, informativo)`, suma(nuevos, m => RG[m.regimen].tipo === "pago"), C.grayBg, C.text]]) {
      const r = ws.getRow(fila); ws.mergeCells(fila, 1, fila, 7); r.getCell(1).value = txt; r.getCell(1).alignment = { horizontal: "right" }; r.getCell(8).value = val; r.getCell(8).numFmt = MONEY;
      [1, 8].forEach(c => { r.getCell(c).fill = fill(bg); r.getCell(c).font = { bold: true, size: 11, color: { argb: fg } }; }); r.height = 22; fila++;
    }
  } else linea("No hay movimientos impositivos nuevos en esta corrida.", { italic: true, size: 10, color: { argb: C.muted } }, 22);

  seccion(4, "YA INFORMADOS EN CORRIDAS ANTERIORES", "El extracto de hoy los vuelve a traer. Van en gris como referencia: NO se informan de nuevo. Se pliegan con el «−» del margen.");
  if (yaEnviados.length) detalle(yaEnviados, true); else linea("Ninguno: todo lo que trae este extracto es nuevo.", { italic: true, size: 10, color: { argb: C.muted } }, 20);

  /* 5. Controles */
  seccion(5, "CONTROLES DE ESTA CORRIDA");
  const ctrl = [
    ["Movimientos impositivos leídos en el extracto (desde " + fAR(DESDE) + ")", String(leido.length)],
    ["Ya informados en corridas anteriores (no se repiten)", String(yaEnviados.length)],
    ["Nuevos informados en esta corrida", String(nuevos.length)],
    ["Anteriores al inicio del informe (" + fAR(DESDE) + "), no incluidos", String(anteriores)],
    ["Base × alícuota", leido.every(m => m.control == null || Math.abs(m.control) <= 0.05) ? "todas coinciden (diferencias ≤ $0,05 por redondeo del banco)" : "HAY DIFERENCIAS: revisar las marcadas con ⚠"],
    ["Pagos a AFIP de otros CUIT", leido.filter(m => m.regimen === "PAGO_AFIP" && !m.propio).length ? leido.filter(m => m.regimen === "PAGO_AFIP" && !m.propio).map(m => fmtCuit(m.cuitPagado)).filter((v, i, a) => a.indexOf(v) === i).join(", ") + ": revisar a quién corresponden" : "ninguno"],
    ["Conceptos con forma de impuesto que el sistema no reconoce", aRevisar.length ? aRevisar.length + ": revisar (ver abajo)" : "ninguno"],
  ];
  for (const [k, v] of ctrl) { const r = ws.getRow(fila); ws.mergeCells(fila, 1, fila, 5); ws.mergeCells(fila, 6, fila, N); r.getCell(1).value = k; r.getCell(6).value = v;
    r.getCell(1).font = { size: 10, color: { argb: C.text } }; r.getCell(6).font = { bold: true, size: 10, color: { argb: /HAY|revisar/.test(v) ? C.warnFg : C.okFg } };
    [1, 6].forEach(c => { r.getCell(c).border = borde; r.getCell(c).alignment = { indent: 1, vertical: "middle", wrapText: true }; }); r.height = 20; fila++; }
  for (const d of aRevisar) { const c = linea(`A revisar · ${fAR(d.fecha)} · ${d.descripcion} · ref. ${d.referencia} · $ ${d.importe.toFixed(2)}`, { size: 9, color: { argb: C.warnFg } }, 20); c.fill = fill(C.warnBg); }
  vacia(10);
  linea("Fuente: extracto bancario Santander («Últimos movimientos»). Las constancias oficiales están en ARBA (consulta de recaudaciones bancarias), SIFERE (SIRCREB) y ARCA (Mis Retenciones). Importes en pesos; base y alícuota tomadas de la descripción del banco.", { italic: true, size: 8, color: { argb: C.muted } }, 30);
  ws.headerFooter.oddFooter = "&L&8Mercado Limpio · Retenciones e impuestos bancarios&R&8Página &P de &N";
  return ws;
}

/* Números para el cuerpo del email y para la app. */
function resumen(nuevos, yaEnviados) {
  return { nuevos: nuevos.length, yaEnviados: (yaEnviados || []).length,
    retenidoNuevo: suma(nuevos, m => RG[m.regimen].tipo === "retencion"), pagosNuevos: suma(nuevos, m => RG[m.regimen].tipo === "pago") };
}

module.exports = { leerImpuestosExtracto, normalizarMovimientos, clasificar, registrarEnviados, agregarHojaImpuestos, resumen, REGIMENES, DESDE, CUIT_TITULAR };
