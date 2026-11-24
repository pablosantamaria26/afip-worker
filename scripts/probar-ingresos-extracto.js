/* Prueba SIN red de lib/ingresos-extracto.js (24/11/2026): hoja «Ingresos no facturados y por qué».
   Datos inventados (CUIT y nombres ficticios).    node scripts/probar-ingresos-extracto.js */
const ExcelJS = require("exceljs"), XLSX = require("xlsx");
const I = require("../lib/ingresos-extracto");
const Nunca = require("../lib/nunca-facturar");
let ok = 0, mal = 0;
const t = (n, c, d) => { c ? ok++ : mal++; console.log((c ? "  ✓ " : "  ✗ ") + n + (!c && d != null ? " — " + JSON.stringify(d).slice(0, 400) : "")); };

/* Transferencias del extracto (ficticias) */
const T = [
  { fecha: "2026-11-03", nombre: "cliente uno", cuit: "20111111112", monto: 50000, referencia: "R01" },   // 0 emitida en esta corrida
  { fecha: "2026-11-10", nombre: "cliente uno", cuit: "20111111112", monto: 50000, referencia: "R02" },   // 1 mismo cliente y monto: NO elegida
  { fecha: "2026-11-04", nombre: "cliente dos", cuit: "20222222223", monto: 80000, referencia: "R03" },   // 2 facturada antes (por referencia)
  { fecha: "2026-11-05", nombre: "cliente tres", cuit: "20333333334", monto: 70000, referencia: "R04" },  // 3 facturada antes sin referencia (CUIT+monto)
  { fecha: "2026-11-06", nombre: "cliente cuatro", cuit: "20444444445", monto: 90000, referencia: "R05" },// 4 su factura está ANULADA → sin facturar
  { fecha: "2026-11-07", nombre: "comercio caja", cuit: "30555555556", monto: 120000, referencia: "R06" },// 5 factura de la Caja (origen caja, CUIT+monto)
  { fecha: "2026-11-08", nombre: "huesped uno", cuit: "27666666667", monto: 300000, referencia: "R07" },  // 6 Ornella con factura C
  { fecha: "2026-11-09", nombre: "huesped dos", cuit: "27777777778", monto: 200000, referencia: "R08" },  // 7 Ornella posible → a revisar
  { fecha: "2026-11-11", nombre: "pago parcial", cuit: "20888888889", monto: 40000, referencia: "R09" },  // 8 Caja por marca (pago en partes)
  { fecha: "2026-11-12", nombre: "pariente", cuit: "27999999990", monto: 1500, referencia: "R10" },       // 9 nunca facturar
  { fecha: "2026-11-13", nombre: "otro nombre", cuit: "20121212121", monto: 60000, referencia: "R11" },   // 10 Caja posible → a revisar
  { fecha: "2026-11-14", nombre: "falla arca", cuit: "20131313131", monto: 10000, referencia: "R12" },    // 11 error al emitir
  { fecha: "2026-11-15", nombre: "pariente", cuit: "27999999990", monto: 1500, referencia: "R13" },       // 12 nunca (omitida en la corrida)
  { fecha: "2026-11-16", nombre: "duplicada", cuit: "20141414141", monto: 30000, referencia: "R14" },     // 13 omitida por duplicado en la corrida
];
const resultados = [
  { ok: true, nombre: "CLIENTE UNO", cuit: "20111111112", comprobante: "A-00005-00000900", total: 50000, referencia: "R01", fechaTransf: "2026-11-03", montoTransf: 50000 },
  { ok: false, nombre: "falla arca", cuit: "20131313131", monto: 10000, error: "ARCA no responde", referencia: "R12", fechaTransf: "2026-11-14", montoTransf: 10000 },
  { ok: true, skipped: true, nuncaFacturar: true, nombre: "pariente", cuit: "27999999990", comprobante: "No facturar · Familia o amigos", motivo: "En la lista de nunca facturar (Familia o amigos)", referencia: "R13", montoTransf: 1500 },
  { ok: true, skipped: true, nombre: "duplicada", cuit: "20141414141", comprobante: "B-00005-00000800", total: 30000, referencia: "R14", montoTransf: 30000 },
];
const facturas = [
  { comprobante: "A-00005-00000900", cuit_cliente: "20111111112", total: 50000, fecha: "2026-11-24", referencia_bancaria: "R01" },
  { comprobante: "B-00005-00000700", cuit_cliente: "20222222223", total: 80000, fecha: "2026-11-05", referencia_bancaria: "R03" },
  { comprobante: "A-00005-00000600", cuit_cliente: "20333333334", total: 70001, fecha: "2026-11-06", referencia_bancaria: null },
  { comprobante: "A-00005-00000500", cuit_cliente: "20444444445", total: 90000, fecha: "2026-11-07", referencia_bancaria: "R05", email_error: "ANULADA POR NC-A-00005-00000009" },
  { comprobante: "FA-A-00005-00000400", cuit_cliente: "30555555556", total: 120000, fecha: "2026-11-02", referencia_bancaria: null, origen: "caja" },
];
const mOrn = T.map(() => null); mOrn[6] = { tipo: "ornella", comprobante: "C-00002-00000031" }; mOrn[7] = { tipo: "posible", comprobante: "" };
const mCaja = T.map(() => null); mCaja[8] = { tipo: "caja", comprobante: "FA-A-00005-00000401", motivo: "pago en partes que suman el total" }; mCaja[10] = { tipo: "posible", comprobante: "FA-A-00005-00000402", motivo: "quien transfiere coincide con el email de la factura" };
const lista = { cuits: { "27999999990": { nombre: "pariente (prueba)", motivo: "familia" } }, historial: [] };

console.log("\n1 · Clasificación");
const F = I.clasificar({ todas: T, resultados, facturas, ornella: { marcas: mOrn }, caja: { marcas: mCaja }, lista, marcaNunca: Nunca.marca });
const g = i => F[i].grupo;
t("emitida en esta corrida, ubicada por SU referencia", g(0) === "CORRIDA" && F[0].respaldo === "A-00005-00000900");
t("el mismo cliente con otra transferencia del mismo monto NO queda como facturada (sin facturar)", g(1) === "SIN", F[1]);
t("facturada antes por referencia bancaria, con su comprobante", g(2) === "ANTES" && F[2].respaldo === "B-00005-00000700");
t("facturada antes sin referencia: CUIT + importe (±$2) en el mes", g(3) === "ANTES" && F[3].respaldo === "A-00005-00000600");
t("una factura ANULADA no justifica la transferencia", g(4) === "SIN", F[4]);
t("factura emitida desde la Caja (origen caja) → «Facturada desde la Caja»", g(5) === "CAJA" && /FA-A-00005-00000400/.test(F[5].respaldo));
t("Ornella con factura C → «Ingreso de Ornella» con la factura", g(6) === "ORNELLA" && /C-00002-00000031/.test(F[6].respaldo));
t("Ornella posible → «A revisar»", g(7) === "REVISAR" && /Ornella/.test(F[7].respaldo));
t("Caja por pago en partes → «Facturada desde la Caja»", g(8) === "CAJA" && /FA-A-00005-00000401/.test(F[8].respaldo));
t("lista de nunca facturar → «No es venta» con el motivo", g(9) === "PERSONAL" && /Familia/.test(F[9].respaldo), F[9]);
t("Caja posible → «A revisar» con la factura candidata", g(10) === "REVISAR" && /FA-A-00005-00000402/.test(F[10].respaldo));
t("error al emitir → «Error al emitir» con el motivo", g(11) === "ERROR" && /ARCA/.test(F[11].respaldo));
t("omitida en la corrida por la lista → «No es venta»", g(12) === "PERSONAL");
t("omitida en la corrida por duplicada → «Ya facturada antes» con el comprobante", g(13) === "ANTES" && F[13].respaldo === "B-00005-00000800");

{ /* La factura recién emitida, guardada SIN referencia, no puede justificar la otra transferencia del mismo cliente y monto
     (aunque esa otra venga antes en la lista). */
  const T2 = [{ fecha: "2026-11-03", nombre: "c", cuit: "20151515151", monto: 1000, referencia: "X1" }, { fecha: "2026-11-12", nombre: "c", cuit: "20151515151", monto: 1000, referencia: "X2" }];
  const F2 = I.clasificar({ todas: T2, resultados: [{ ok: true, cuit: "20151515151", comprobante: "B-00005-00000999", referencia: "X2", montoTransf: 1000 }],
    facturas: [{ comprobante: "B-00005-00000999", cuit_cliente: "20151515151", total: 1000, fecha: "2026-11-24", referencia_bancaria: null }] });
  t("la factura de esta corrida queda reservada: la otra transferencia igual queda «Sin facturar»", F2[0].grupo === "SIN" && F2[1].grupo === "CORRIDA", F2.map(f => f.grupo));
}

console.log("\n2 · Resumen");
const R = I.resumen(F);
const totalT = T.reduce((s, x) => s + x.monto, 0);
t("la suma de los grupos es el total recibido (cantidad e importe)", R.total.cantidad === T.length && Math.abs(R.total.importe - totalT) < 0.01, R.total);
t("con factura + sin factura = total", R.facturado.cantidad + R.noFacturado.cantidad === T.length && Math.abs(R.facturado.importe + R.noFacturado.importe - totalT) < 0.01);
t("con factura = esta corrida + antes + Caja (6)", R.facturado.cantidad === 6, R.facturado);
t("línea del email", /^8 sin factura de Mercado Limpio \(ingreso de ornella 1 · no es venta 2 · a revisar 2 · error al emitir 1 · sin facturar 2\)$/.test(I.textoEmail(R)), I.textoEmail(R));

console.log("\n3 · Fallas de consulta");
(async () => {
  const sbFalla = { from: () => { const api = { select: () => api, gte: () => api, lte: () => api, gt: () => api, order: () => api, range: async () => ({ data: null, error: { message: "base caída" } }) }; return api; } };
  const A = await I.armar({ supabase: sbFalla, Ornella: { verificar: async () => { throw new Error("sin conexión"); } }, OtroCanal: { verificar: async () => { throw new Error("x"); } }, Nunca: { leer: async () => { throw new Error("y"); }, marca: Nunca.marca } }, T.slice(0, 3), []);
  t("si nada se puede consultar: todo «Sin facturar» con la nota, nunca un motivo inventado", A.filas.every(f => f.grupo === "SIN" && /No se pudo consultar/.test(f.respaldo)) && A.sinVerificar.length === 4, A.sinVerificar);
  const sbOk = { from: () => { const api = { select: () => api, gte: () => api, lte: () => api, gt: () => api, order: () => api, range: async () => ({ data: facturas, error: null }) }; return api; } };
  const B = await I.armar({ supabase: sbOk, Ornella: { verificar: async x => ({ marcas: x.map((_, i) => mOrn[i]) }) }, OtroCanal: { verificar: async (_, x) => ({ marcas: x.map((_, i) => mCaja[i]) }) }, Nunca: { leer: async () => lista, marca: Nunca.marca } }, T, resultados);
  t("armar() con todo disponible da lo mismo que la clasificación directa", JSON.stringify(B.filas.map(f => f.grupo)) === JSON.stringify(F.map(f => f.grupo)) && !B.sinVerificar.length);

  console.log("\n4 · Hoja del Excel");
  const wb = new ExcelJS.Workbook();
  I.agregarHoja(wb, { ...B, periodo: "Noviembre de 2026", fechaCorrida: "2026-11-24" });
  const buf = await wb.xlsx.writeBuffer();
  const ws = XLSX.read(buf, { cellFormula: true }).Sheets["Ingresos no facturados"];
  const celdas = Object.keys(ws).filter(a => a[0] !== "!");
  const textos = celdas.map(a => String(ws[a].v));
  t("existe la hoja «Ingresos no facturados» con título y fecha de corrida", ws && textos.includes("INGRESOS NO FACTURADOS Y POR QUÉ") && textos.some(x => /Corrida del 24\/11\/2026/.test(x)));
  t("sin fórmulas (los totales se ven en cualquier vista previa)", !celdas.some(a => ws[a].f));
  t("TOTAL RECIBIDO con la cantidad y el importe del extracto", (() => { const a = celdas.find(x => ws[x].v === "TOTAL RECIBIDO"); const fila = a && a.replace(/\D/g, ""); return a && ws["C" + fila].v === T.length && Math.abs(ws["D" + fila].v - totalT) < 0.01; })());
  t("el detalle trae cada transferencia no emitida en esta corrida (13) y no la emitida", textos.filter(x => /^R\d\d$/.test(x)).length === 13 && !textos.includes("R01"), textos.filter(x => /^R\d\d$/.test(x)));
  t("cada fila del detalle dice su situación", ["Ingreso de Ornella", "No es venta", "A revisar", "Error al emitir", "Sin facturar", "Ya facturada antes", "Facturada desde la Caja"].every(s => textos.includes(s)));
  const cols = (XLSX.read(buf, { cellStyles: true }).Sheets["Ingresos no facturados"]["!cols"] || []).map(c => c && (c.wch || c.width));
  t("columnas anchas para importes y respaldo (no aparece ####)", cols[3] >= 15 && cols[6] >= 50, cols);
  const wb2 = new ExcelJS.Workbook(); I.agregarHoja(wb2, { filas: [], sinVerificar: ["facturación de Ornella"], fechaCorrida: "2026-11-24" });
  const ws2 = XLSX.read(await wb2.xlsx.writeBuffer()).Sheets["Ingresos no facturados"];
  t("sin transferencias y con una consulta caída: la hoja sale con el aviso y «Nada en este grupo»", Object.values(ws2).some(c => /No se pudo consultar: facturación de Ornella/.test(String(c && c.v))) && Object.values(ws2).some(c => c && c.v === "Nada en este grupo."));

  console.log(`\n${ok} bien · ${mal} mal`); process.exit(mal ? 1 : 0);
})().catch(e => { console.error("ERROR", e); process.exit(1); });
