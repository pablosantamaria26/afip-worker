/* ═══════════════════════════════════════════════════════════════════
   INGRESOS NO FACTURADOS Y POR QUÉ (24/11/2026)

   Hoja del reporte al contador que explica CADA transferencia recibida del extracto:
     · facturada en esta corrida, antes (otra corrida o a mano) o desde la Caja → con su comprobante;
     · ingreso de Ornella (cuenta compartida, lo factura ella con factura C);
     · no es venta (lista de «nunca facturar»: empleados, familia, personal);
     · a revisar (posible Ornella o posible pago de una factura de la Caja), error al emitir, o sin facturar.
   La suma de todos los grupos es el total de transferencias recibidas: sirve para conciliar banco contra facturación.

   No depende de lo que mande la app: el servidor recalcula cada motivo con los mismos controles que usa al facturar
   (facturas guardadas, Ornella, Caja y la lista). Si alguno no se puede consultar, se avisa en la hoja y esas
   transferencias quedan como «sin facturar» con la nota (nunca se inventa un motivo).
   ═══════════════════════════════════════════════════════════════════ */
"use strict";

const soloDig = v => String(v || "").replace(/\D/g, "");
const r2 = n => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
const f10 = v => String(v || "").slice(0, 10);
const sumarDias = (iso, n) => { const d = new Date(iso + "T12:00:00Z"); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
const mesSig = iso => { const [a, m] = iso.split("-").map(Number); return m === 12 ? `${a + 1}-01` : `${a}-${String(m + 1).padStart(2, "0")}`; };
const anulada = f => /ANULADA POR/i.test(String(f.email_error || ""));

/* Grupos, en el orden en que se muestran. «facturado» = ya tiene comprobante de Mercado Limpio. */
const GRUPOS = {
  CORRIDA: { orden: 1, titulo: "Facturada en esta corrida", facturado: true, explica: "Detalle en la hoja «Extracto Bancario»." },
  ANTES:   { orden: 2, titulo: "Ya facturada antes", facturado: true, explica: "Corrida anterior del extracto o factura hecha a mano." },
  CAJA:    { orden: 3, titulo: "Facturada desde la Caja", facturado: true, explica: "La factura se emitió desde la Caja (por remito); la transferencia es su pago." },
  ORNELLA: { orden: 4, titulo: "Ingreso de Ornella", facturado: false, explica: "Cuenta compartida: alquiler de Tiempo Libre, lo factura Ornella con su factura C." },
  PERSONAL:{ orden: 5, titulo: "No es venta", facturado: false, explica: "Empleados, familia, amigos o gastos personales (lista de «nunca facturar»)." },
  REVISAR: { orden: 6, titulo: "A revisar", facturado: false, explica: "Posible ingreso de Ornella o posible pago de una factura de la Caja: no se facturó para no duplicar." },
  ERROR:   { orden: 7, titulo: "Error al emitir", facturado: false, explica: "Se intentó facturar y ARCA o el sistema rechazó: se puede reintentar." },
  SIN:     { orden: 8, titulo: "Sin facturar", facturado: false, explica: "No se eligió para facturar en esta corrida." },
};

/* Un resultado del trabajo de facturación → grupo y respaldo. */
function desdeResultado(r) {
  if (r.ok && !r.skipped) return { grupo: "CORRIDA", respaldo: r.comprobante || "" };
  if (r.nuncaFacturar) return { grupo: "PERSONAL", respaldo: r.motivo || r.comprobante || "" };
  if (r.ornella) return r.posible ? { grupo: "REVISAR", respaldo: r.motivo || "Posible ingreso de Ornella" } : { grupo: "ORNELLA", respaldo: r.comprobante || "Ornella" };
  if (r.otroCanal) return r.posible ? { grupo: "REVISAR", respaldo: r.motivo || "Posible pago de una factura ya emitida" } : { grupo: "CAJA", respaldo: [r.comprobante, r.motivo].filter(Boolean).join(" · ") };
  if (r.skipped) return { grupo: "ANTES", respaldo: r.comprobante || "" };
  return { grupo: "ERROR", respaldo: r.error || "Error al emitir" };
}

/* Clasificación pura (sin red). Devuelve una fila por transferencia, en el mismo orden que `todas`.
   facturas: [{comprobante,cuit_cliente,total,fecha,referencia_bancaria,email_error,origen}] (las vigentes del rango)
   ornella/caja: { marcas: [...] } paralelas a `todas`, o null si no se pudo consultar
   lista: lista de «nunca facturar» ({cuits}) o null; marcaNunca(lista,cuit) → {texto,nombre}|null */
function clasificar({ todas = [], resultados = [], facturas = [], ornella = null, caja = null, lista = null, marcaNunca = null, sinVerificar = [] }) {
  const usadosR = new Set(), usadasF = new Set();
  const vig = facturas.filter(f => Number(f.total) > 0 && !anulada(f));
  const datos = t => ({ cuit: soloDig(t.cuit), monto: r2(Math.abs(Number(t.monto) || 0)), ref: String(t.referencia || "").trim(), fecha: f10(t.fecha) });

  /* 1ª vuelta — lo que pasó en esta corrida (el trabajo guarda la referencia y el monto de cada transferencia).
     Va primero y para todas: la factura que se acaba de emitir queda reservada y no puede justificar OTRA transferencia
     del mismo cliente por el mismo monto. */
  const montoR = r => r2(Math.abs(Number(r.montoTransf != null ? r.montoTransf : r.monto != null ? r.monto : r.total) || 0));
  const porResultado = todas.map(t => {
    const { cuit, monto, ref } = datos(t);
    let k = ref ? resultados.findIndex((r, j) => !usadosR.has(j) && soloDig(r.cuit) === cuit && String(r.referencia || "") === ref) : -1;
    if (k < 0) k = resultados.findIndex((r, j) => !usadosR.has(j) && soloDig(r.cuit) === cuit && !r.referencia && Math.abs(montoR(r) - monto) < 0.01);
    if (k < 0) return null;
    usadosR.add(k);
    const d = desdeResultado(resultados[k]);
    if (d.grupo === "CORRIDA" || d.grupo === "ANTES") vig.forEach((f, j) => { if (f.comprobante && f.comprobante === resultados[k].comprobante) usadasF.add(j); });
    return d;
  });

  return todas.map((t, i) => {
    const { cuit, monto, ref, fecha } = datos(t);
    const fila = { fecha, nombre: String(t.nombre || ""), cuit, monto, referencia: ref };
    if (porResultado[i]) return Object.assign(fila, porResultado[i]);

    /* 2) Ya facturada: por referencia bancaria (exacto) o, en facturas sin referencia, CUIT + importe (±$2) en el mes de la
       transferencia o el siguiente (mismo criterio que el control de duplicados al facturar). Cada factura justifica una sola. */
    let fk = ref ? vig.findIndex((f, j) => !usadasF.has(j) && String(f.referencia_bancaria || "") === ref && (!cuit || soloDig(f.cuit_cliente) === cuit)) : -1;
    if (fk < 0 && cuit.length === 11 && fecha) {
      const meses = [fecha.slice(0, 7), mesSig(fecha)];
      fk = vig.findIndex((f, j) => !usadasF.has(j) && !f.referencia_bancaria && soloDig(f.cuit_cliente) === cuit && Math.abs(Number(f.total) - monto) <= 2
        && meses.includes(f10(f.fecha).slice(0, 7)));
    }
    if (fk >= 0) { usadasF.add(fk); const f = vig[fk];
      return Object.assign(fila, f.origen === "caja" ? { grupo: "CAJA", respaldo: f.comprobante } : { grupo: "ANTES", respaldo: f.comprobante }); }

    /* 3) Ornella, Caja, lista de «nunca facturar», posibles. */
    const mo = ornella && ornella.marcas[i], mc = caja && caja.marcas[i];
    if (mo && mo.tipo === "ornella") return Object.assign(fila, { grupo: "ORNELLA", respaldo: mo.comprobante ? "Factura " + mo.comprobante + " de Ornella" : "Pago registrado por Ornella (sin factura todavía)" });
    if (mc && mc.tipo !== "posible") return Object.assign(fila, { grupo: "CAJA", respaldo: [mc.comprobante, mc.motivo].filter(Boolean).join(" · ") });
    const mn = lista && marcaNunca ? marcaNunca(lista, cuit) : null;
    if (mn) return Object.assign(fila, { grupo: "PERSONAL", respaldo: mn.texto + (mn.nombre ? " · " + mn.nombre : "") });
    if (mo && mo.tipo === "posible") return Object.assign(fila, { grupo: "REVISAR", respaldo: "Posible ingreso de Ornella (pago del mismo importe y fecha, sin documento)" });
    if (mc) return Object.assign(fila, { grupo: "REVISAR", respaldo: "Posible pago de " + mc.comprobante + ": " + (mc.motivo || "") });
    return Object.assign(fila, { grupo: "SIN", respaldo: sinVerificar.length ? "No se pudo consultar: " + sinVerificar.join(", ") + ". Revisar antes de facturar." : "" });
  });
}

/* Resumen por grupo (cantidad e importe), en orden. */
function resumen(filas) {
  const out = Object.keys(GRUPOS).map(g => ({ grupo: g, ...GRUPOS[g], cantidad: 0, importe: 0 }));
  for (const f of filas) { const x = out.find(o => o.grupo === f.grupo); x.cantidad++; x.importe = r2(x.importe + f.monto); }
  const tot = (p) => out.filter(p).reduce((a, x) => ({ cantidad: a.cantidad + x.cantidad, importe: r2(a.importe + x.importe) }), { cantidad: 0, importe: 0 });
  return { grupos: out, total: tot(() => true), facturado: tot(x => x.facturado), noFacturado: tot(x => !x.facturado) };
}

/* Carga lo necesario (con tolerancia a fallas de cada fuente) y clasifica. deps = { supabase, Ornella, OtroCanal, Nunca } */
async function armar(deps, todas, resultados) {
  const lista = Array.isArray(todas) ? todas.filter(t => Number(t.monto) > 0) : [];
  const sinVerificar = [];
  const fechas = lista.map(t => f10(t.fecha)).filter(f => /^\d{4}-\d{2}-\d{2}$/.test(f)).sort();
  let facturas = [];
  if (fechas.length && deps.supabase) {
    try {
      const desde = fechas[0], hasta = sumarDias(fechas[fechas.length - 1], 65);
      for (let p = 0; p < 20; p++) {
        const { data, error } = await deps.supabase.from("facturas").select("comprobante, cuit_cliente, total, fecha, referencia_bancaria, email_error, origen")
          .gte("fecha", desde).lte("fecha", hasta).gt("total", 0).order("fecha", { ascending: true }).range(p * 1000, p * 1000 + 999);
        if (error) throw new Error(error.message);
        facturas = facturas.concat(data || []);
        if (!data || data.length < 1000) break;
      }
    } catch (e) { sinVerificar.push("facturas ya emitidas"); facturas = []; }
  }
  let ornella = null, caja = null, nunca = null;
  try { ornella = await deps.Ornella.verificar(lista); } catch (e) { sinVerificar.push("facturación de Ornella"); }
  try { caja = await deps.OtroCanal.verificar(deps.supabase, lista); } catch (e) { sinVerificar.push("facturas de la Caja"); }
  try { nunca = await deps.Nunca.leer(deps.supabase); } catch (e) { sinVerificar.push("lista de nunca facturar"); }
  const filas = clasificar({ todas: lista, resultados: resultados || [], facturas, ornella, caja, lista: nunca, marcaNunca: deps.Nunca && deps.Nunca.marca, sinVerificar });
  return { filas, resumen: resumen(filas), sinVerificar };
}

/* ── Hoja del Excel ── */
const C = { navy: "FF1E3A5F", navyDark: "FF142A47", amber: "FFC97A3A", white: "FFFFFFFF", text: "FF1A2530", muted: "FF6B7F94", alt: "FFF4F7FA", line: "FFD8E3EE",
  okBg: "FFE2F0D9", okFg: "FF1F4E2B", warnBg: "FFFFF2CC", warnFg: "FF7F6000", redBg: "FFFBE3E1", redFg: "FF9B2C22" };
const COLOR = { CORRIDA: "FF1F6B3A", ANTES: "FF2E7D4F", CAJA: "FF3A7D6B", ORNELLA: "FF7A4FA0", PERSONAL: "FF5B6B7C", REVISAR: "FFB7791F", ERROR: "FFB03A2E", SIN: "FF1F5FA8" };
const MONEY = '"$" #,##0.00;[Red]-"$" #,##0.00;"—"';
const fAR = iso => (iso ? String(iso).slice(0, 10).split("-").reverse().join("/") : "");
const fmtCuit = c => (c && c.length === 11 ? `${c.slice(0, 2)}-${c.slice(2, 10)}-${c.slice(10)}` : c || "");

/* datos = { filas, resumen, sinVerificar, periodo, fechaCorrida } */
function agregarHoja(wb, datos) {
  const { filas = [], resumen: rs = resumen(datos.filas || []), sinVerificar = [] } = datos;
  const fill = argb => ({ type: "pattern", pattern: "solid", fgColor: { argb } });
  const borde = { bottom: { style: "hair", color: { argb: C.line } } };
  const ws = wb.addWorksheet("Ingresos no facturados", { properties: { tabColor: { argb: "FF7A4FA0" } }, views: [{ state: "frozen", ySplit: 3, showGridLines: false }],
    pageSetup: { orientation: "landscape", fitToPage: true, fitToWidth: 1, fitToHeight: 0, paperSize: 9, margins: { left: 0.4, right: 0.4, top: 0.5, bottom: 0.5, header: 0.2, footer: 0.2 } } });
  ws.columns = [{ width: 12 }, { width: 32 }, { width: 16 }, { width: 17 }, { width: 13 }, { width: 26 }, { width: 62 }];
  const N = 7; let fila = 4;
  const vacia = h => { ws.getRow(fila).height = h || 8; fila++; };
  const linea = (texto, font, h, wrap) => { ws.mergeCells(fila, 1, fila, N); const c = ws.getCell(fila, 1); c.value = texto; c.font = font; c.alignment = { indent: 1, wrapText: wrap !== false, vertical: "middle" }; ws.getRow(fila).height = h || 18; fila++; return c; };
  const seccion = (n, texto, sub) => { vacia(14); const c = linea(`${n} · ${texto}`, { bold: true, size: 12, color: { argb: C.white } }, 24); c.fill = fill(C.navyDark); if (sub) linea(sub, { italic: true, size: 9, color: { argb: C.muted } }, 18); };
  const cabecera = textos => { const r = ws.getRow(fila); textos.forEach((t, i) => { const c = r.getCell(i + 1); c.value = t; c.font = { bold: true, size: 9, color: { argb: C.white } }; c.fill = fill(C.navy); c.alignment = { horizontal: i === 3 ? "right" : "left", vertical: "middle", wrapText: true }; }); r.height = 26; fila++; };

  /* Encabezado */
  ws.mergeCells(1, 1, 1, N); const t = ws.getCell(1, 1); t.value = "INGRESOS NO FACTURADOS Y POR QUÉ"; t.fill = fill(C.navy); t.font = { bold: true, size: 18, color: { argb: C.white } }; t.alignment = { vertical: "middle", indent: 1 }; ws.getRow(1).height = 40;
  ws.mergeCells(2, 1, 2, N); const s2 = ws.getCell(2, 1);
  s2.value = `Transferencias recibidas en el extracto${datos.periodo ? " · " + datos.periodo : ""} · Corrida del ${fAR(datos.fechaCorrida)} · Cada ingreso con su situación y respaldo`;
  s2.fill = fill(C.navy); s2.font = { size: 10, color: { argb: "FFC9D6E3" } }; s2.alignment = { indent: 1, vertical: "middle" }; ws.getRow(2).height = 20;
  ws.mergeCells(3, 1, 3, N); ws.getCell(3, 1).fill = fill(C.amber); ws.getRow(3).height = 4;
  if (sinVerificar.length) { vacia(6); const c = linea("⚠ No se pudo consultar: " + sinVerificar.join(", ") + ". Las transferencias afectadas figuran como «Sin facturar» con esa nota.", { bold: true, size: 10, color: { argb: C.warnFg } }, 26); c.fill = fill(C.warnBg); }

  /* Indicadores */
  vacia(10);
  const kpis = [["TRANSFERENCIAS RECIBIDAS", rs.total, C.navy, [1, 2]], ["CON FACTURA DE MERCADO LIMPIO", rs.facturado, COLOR.CORRIDA, [3, 4]], ["SIN FACTURA DE MERCADO LIMPIO", rs.noFacturado, COLOR.ORNELLA, [5, 7]]];
  [0, 1, 2].forEach(k => { ws.getRow(fila + k).height = k === 1 ? 30 : 18; });
  kpis.forEach(([tit, v, color, [a, b]]) => {
    for (let k = 0; k < 3; k++) { ws.mergeCells(fila + k, a, fila + k, b); const c = ws.getCell(fila + k, a); c.fill = fill(k === 0 ? color : C.alt); c.border = { left: { style: "thick", color: { argb: color } } }; c.alignment = { horizontal: "center", vertical: "middle" };
      if (k === 0) { c.value = tit; c.font = { bold: true, size: 9, color: { argb: C.white } }; }
      if (k === 1) { c.value = v.importe; c.numFmt = MONEY; c.font = { bold: true, size: 16, color: { argb: color } }; }
      if (k === 2) { c.value = v.cantidad + (v.cantidad === 1 ? " transferencia" : " transferencias"); c.font = { size: 8, italic: true, color: { argb: C.muted } }; } }
  });
  fila += 3;

  /* 1. Resumen: la suma de los grupos es el total recibido. */
  seccion(1, "RESUMEN", "La suma de todos los grupos es el total de transferencias recibidas del extracto.");
  { const r = ws.getRow(fila); ["Situación", "", "Cantidad", "Importe", "", "Qué significa", ""].forEach((x, i) => { const c = r.getCell(i + 1); c.value = x; c.font = { bold: true, size: 9, color: { argb: C.white } }; c.fill = fill(C.navy); c.alignment = { horizontal: i === 2 ? "center" : i === 3 ? "right" : "left", vertical: "middle" }; });
    ws.mergeCells(fila, 1, fila, 2); ws.mergeCells(fila, 6, fila, 7); r.height = 22; fila++; }
  const filaResumen = (titulo, cant, imp, explica, color, negrita, fondo) => {
    const r = ws.getRow(fila); ws.mergeCells(fila, 1, fila, 2); ws.mergeCells(fila, 6, fila, 7);
    r.getCell(1).value = titulo; r.getCell(3).value = cant; r.getCell(4).value = imp; r.getCell(6).value = explica;
    for (let c = 1; c <= N; c++) { const x = r.getCell(c); x.fill = fill(fondo || C.white); x.border = borde; x.alignment = { vertical: "middle", horizontal: c === 3 ? "center" : c === 4 ? "right" : "left", wrapText: c === 6, indent: c === 1 ? 1 : 0 }; x.font = { size: 10, bold: !!negrita && c <= 4, color: { argb: c === 1 ? color : c === 6 ? C.muted : C.text } }; }
    r.getCell(1).border = { left: { style: "thick", color: { argb: color } }, bottom: borde.bottom };
    r.getCell(4).numFmt = MONEY; r.height = 30; fila++;
  };
  rs.grupos.filter(g => g.cantidad).forEach(g => filaResumen(g.titulo, g.cantidad, g.importe, g.explica, COLOR[g.grupo], true));
  filaResumen("TOTAL RECIBIDO", rs.total.cantidad, rs.total.importe, `Con factura de Mercado Limpio: ${rs.facturado.cantidad} · sin factura: ${rs.noFacturado.cantidad}`, C.navy, true, C.okBg);

  /* Detalle */
  const detalle = (n, titulo, sub, grupos) => {
    const lista = filas.filter(f => grupos.includes(f.grupo)).sort((a, b) => GRUPOS[a.grupo].orden - GRUPOS[b.grupo].orden || a.fecha.localeCompare(b.fecha) || a.nombre.localeCompare(b.nombre));
    seccion(n, titulo, sub);
    if (!lista.length) { linea("Nada en este grupo.", { italic: true, size: 10, color: { argb: C.muted } }); return; }
    cabecera(["Fecha", "Quién transfiere", "CUIT", "Importe", "Referencia", "Situación", "Respaldo / motivo"]);
    lista.forEach((f, i) => {
      const r = ws.getRow(fila);
      r.values = [fAR(f.fecha), f.nombre, fmtCuit(f.cuit), f.monto, f.referencia, GRUPOS[f.grupo].titulo, f.respaldo];
      for (let c = 1; c <= N; c++) { const x = r.getCell(c); x.fill = fill(i % 2 ? C.alt : C.white); x.border = borde; x.font = { size: 10, color: { argb: c === 6 ? COLOR[f.grupo] : C.text }, bold: c === 6 }; x.alignment = { vertical: "middle", horizontal: c === 4 ? "right" : c === 1 || c === 3 || c === 5 ? "center" : "left", wrapText: c === 7 }; }
      /* alto según las líneas que ocupa el respaldo (columna de 62 de ancho ≈ 58 caracteres por línea) */
      r.getCell(4).numFmt = MONEY; r.height = Math.max(18, Math.ceil(String(f.respaldo || "").length / 58) * 14 + 4); fila++;
    });
    const rt = ws.getRow(fila); ws.mergeCells(fila, 1, fila, 3); rt.getCell(1).value = `Subtotal · ${lista.length} transferencia${lista.length === 1 ? "" : "s"}`; rt.getCell(4).value = r2(lista.reduce((s, f) => s + f.monto, 0));
    [1, 4].forEach(c => { rt.getCell(c).font = { bold: true, size: 10, color: { argb: C.okFg } }; }); rt.getCell(1).alignment = { horizontal: "right" }; rt.getCell(4).numFmt = MONEY;
    for (let c = 1; c <= N; c++) rt.getCell(c).fill = fill(C.okBg); rt.height = 20; fila++;
  };
  detalle(2, "SIN FACTURA DE MERCADO LIMPIO", "Ingresos de Ornella, lo que no es venta, lo que quedó a revisar, errores y lo que no se eligió facturar.", ["ORNELLA", "PERSONAL", "REVISAR", "ERROR", "SIN"]);
  detalle(3, "FACTURADAS POR OTRO CAMINO", "Facturas emitidas antes de esta corrida (otra corrida, a mano o desde la Caja). Lo facturado en esta corrida está en la hoja «Extracto Bancario».", ["ANTES", "CAJA"]);
  return ws;
}

/* Una línea para el cuerpo del email. */
function textoEmail(rs) {
  const partes = rs.grupos.filter(g => !g.facturado && g.cantidad).map(g => `${g.titulo.toLowerCase()} ${g.cantidad}`);
  return `${rs.noFacturado.cantidad} sin factura de Mercado Limpio${partes.length ? " (" + partes.join(" · ") + ")" : ""}`;
}

module.exports = { GRUPOS, clasificar, resumen, armar, agregarHoja, textoEmail, desdeResultado };
