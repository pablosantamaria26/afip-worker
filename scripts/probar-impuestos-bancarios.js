/* Prueba SIN red, SIN Resend real y SIN Supabase real de las retenciones/impuestos bancarios (24/11/2026).
     node scripts/probar-impuestos-bancarios.js [carpeta-de-extractos] [salida.xlsx]
   1) Lee los extractos reales de Santander. 2) Simula corridas del 15/11 y del 19/11 contra un Supabase falso (registro privado).
   3) Corre la función REAL generarReporteExtracto de index.js (extraída con vm) con un Resend falso que solo anota destinatarios y adjunto. */
const fs = require("fs"), path = require("path"), vm = require("vm");
const XLSX = require("xlsx"), ExcelJS = require("exceljs");
const Imp = require("../lib/impuestos-bancarios");
let ok = 0, mal = 0;
const t = (n, c, d) => { c ? ok++ : mal++; console.log((c ? "  ✓ " : "  ✗ ") + n + (!c && d != null ? " — " + JSON.stringify(d).slice(0, 400) : "")); };
const DIR = process.argv[2] || path.join(__dirname, "..", "..");
const SALIDA = process.argv[3] || "";
const filas = f => XLSX.utils.sheet_to_json(XLSX.readFile(path.join(DIR, f)).Sheets[XLSX.readFile(path.join(DIR, f)).SheetNames[0]], { header: 1, defval: "" });

/* Supabase falso: tabla facturas (para el reporte) + Storage con buckets (el registro privado). */
function supabaseFalso() {
  const buckets = {}, ctl = { fallarRegistro: false };
  const facturas = [{ comprobante: "FA-A-00005-00000150", cuit_cliente: "27956214225", nombre_cliente: "YAN XIUYU", total: 100000, cae: "1", pdf_url: "", mes: 11, anio: 2026, fecha: "2026-11-19" }];
  const q = () => { const f = []; const api = { select() { return api; }, eq(c, v) { f.push(r => r[c] === v); return api; }, gt(c, v) { f.push(r => r[c] > v); return api; }, gte(c, v) { f.push(r => r[c] >= v); return api; }, lte(c, v) { f.push(r => r[c] <= v); return api; }, range() { return api; }, lt(c, v) { f.push(r => r[c] < v); return api; }, order() { return api; }, in() { return api; },
    then(res) { res({ data: facturas.filter(r => f.every(x => x(r))), error: null }); } }; return api; };
  const err = m => ({ data: null, error: { message: m } });
  return { ctl, buckets, from: q, storage: {
    getBucket: async n => ctl.fallarRegistro ? err("caído") : buckets[n] ? { data: { name: n, public: buckets[n].public }, error: null } : err("not found"),
    createBucket: async (n, o) => { buckets[n] = { public: !!(o && o.public), files: {} }; return { data: { name: n }, error: null }; },
    from: n => ({
      upload: async (p, buf) => { if (ctl.fallarRegistro) return err("caído"); (buckets[n] = buckets[n] || { files: {} }).files[p] = buf.toString(); return { data: {}, error: null }; },
      download: async p => { const b = buckets[n]; return b && b.files[p] ? { data: { text: async () => b.files[p] }, error: null } : err("no"); },
      list: async carpeta => { if (ctl.fallarRegistro) return err("caído"); const b = buckets[n] || { files: {} }; return { data: Object.keys(b.files).filter(k => k.startsWith(carpeta + "/")).map(k => ({ name: k.slice(carpeta.length + 1) })), error: null }; },
    }) } };
}

/* La función REAL del reporte, sacada de index.js y corrida con dobles. */
function reporteReal(sb, envios, fallarEnvio) {
  const src = fs.readFileSync(path.join(__dirname, "..", "index.js"), "utf8").replace(/\r\n/g, "\n");
  const a = src.indexOf("async function generarReporteExtracto("), b = src.indexOf("// ── Store de jobs de extracto", a);
  const ctx = { module: { exports: {} }, console: { log() {}, warn() {}, error: (...x) => envios.errores.push(x.join(" ")) }, Buffer, Date, Number, String, Math, JSON, Promise, Object, Array, Map, Set, RegExp, process,
    ExcelJS, ImpBanco: Imp, supabase: sb,
    Ingresos: require("../lib/ingresos-extracto"), Ornella: { verificar: async x => ({ marcas: x.map(() => null) }) }, OtroCanal: { verificar: async (_, x) => ({ marcas: x.map(() => null) }) },
    Nunca: { leer: async () => ({ cuits: { "27000000006": { nombre: "prueba", motivo: "personal" } }, historial: [] }), marca: require("../lib/nunca-facturar").marca }, EMISOR: { nombreVisible: "MERCADO LIMPIO DISTRIBUIDORA" }, GMAIL_USER: "ventas@mercadolimpio.ar",
    formatMoneyAR: n => Number(n || 0).toLocaleString("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }), onlyDigits: s => String(s || "").replace(/\D/g, ""), todayISO: () => "2026-11-19", round2: n => Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100,
    resendClient: { emails: { send: async m => { if (fallarEnvio) return { error: { message: "Resend: rechazo simulado" } }; envios.lista.push(m); return { error: null }; } } } };
  vm.createContext(ctx); vm.runInContext(src.slice(a, b) + "\nmodule.exports = generarReporteExtracto;", ctx);
  return ctx.module.exports;
}

(async () => {
  console.log("\n1 · Lectura de los extractos reales");
  const nov = Imp.leerImpuestosExtracto(filas("movimientos (1 AL 19-11).xlsx"));
  const cuenta = (id) => nov.movimientos.filter(m => m.regimen === id);
  t("cuenta y período del extracto", /533-352959\/9/.test(nov.cuenta) && nov.periodoExtracto === "01/11/2026 - 19/11/2026", [nov.cuenta, nov.periodoExtracto]);
  t("noviembre: 18 recaudaciones ARBA, 4 de Ley 25.413 y 1 pago a AFIP", cuenta("ARBA_REC").length === 18 && cuenta("LEY25413_DB").length === 4 && cuenta("PAGO_AFIP").length === 1, nov.movimientos.map(m => m.regimen));
  const afip = cuenta("PAGO_AFIP")[0] || {};
  t("el pago a AFIP del 04/11 trae el CUIT de la obligación (27-94996520-7, no es el titular) y $44.784,77", afip.cuitPagado === "27949965207" && afip.propio === false && afip.importe === 44784.77, afip);
  t("ARBA trae base y alícuota (1,80 %) y el control base × alícuota da ≤ $0,05", cuenta("ARBA_REC").every(m => m.alicuota === 1.8 && m.base > 0 && Math.abs(m.control) <= 0.05));
  t("importes en positivo y huella única por movimiento", nov.movimientos.every(m => m.importe > 0) && new Set(nov.movimientos.map(m => m.clave)).size === nov.movimientos.length);
  t("ningún concepto «a revisar» en noviembre", nov.aRevisar.length === 0, nov.aRevisar);
  const oct = Imp.leerImpuestosExtracto(filas("movimientos (39).xlsx"));
  t("octubre queda fuera (arranque 01/11/2026): 0 informados, y se cuentan como anteriores", oct.movimientos.length === 0 && oct.anteriores > 30, oct.anteriores);
  const ago = Imp.leerImpuestosExtracto(filas("movimientos (34).xlsx"));
  t("agosto: los pagos ARBA/AFIP se reconocen (y quedan fuera por la fecha de arranque)", ago.movimientos.length === 0 && ago.anteriores >= 4, ago.anteriores);
  const raro = Imp.leerImpuestosExtracto([[], [null, "Fecha", "Sucursal origen", "Descripción", "Referencia", "Caja de Ahorro"], [null, "20/11/2026", "x", "Impuesto de sellos provincia", "000016300", "-1500.00"], [null, "20/11/2026", "x", "Transferencia recibida De juan iva / var / 20123456789", "1", "5000"]]);
  t("un concepto con forma de impuesto que no está en el catálogo va «a revisar»; una transferencia de alguien llamado «iva» no", raro.aRevisar.length === 1 && /sellos/.test(raro.aRevisar[0].descripcion), raro.aRevisar);

  console.log("\n2 · Lo que la app devuelve se valida");
  const basura = Imp.normalizarMovimientos([{ regimen: "INVENTADO", fecha: "2026-11-02", importe: 5 }, { regimen: "ARBA_REC", fecha: "2026-10-30", importe: 5 }, { regimen: "ARBA_REC", fecha: "ayer", importe: 5 }, { ...nov.movimientos[0], clave: "trucha" }, nov.movimientos[0]]);
  t("descarta régimen desconocido, fechas anteriores al arranque o mal formadas, recalcula la huella y no duplica", basura.length === 1 && basura[0].clave === nov.movimientos[0].clave, basura.length);

  console.log("\n3 · Corridas del 15/11 y del 19/11 con el reporte REAL (Resend falso)");
  const sb = supabaseFalso(), envios = { lista: [], errores: [] };
  const hasta15 = { ...nov, movimientos: nov.movimientos.filter(m => m.fecha <= "2026-11-15") };
  const resultados = [{ ok: true, comprobante: "FA-A-00005-00000150", cuit: "27956214225", nombre: "YAN XIUYU", total: 100000, cae: "1", referencia: "R2", fechaTransf: "2026-11-12", montoTransf: 100000 }];
  /* Transferencias del extracto (ficticias): el mismo cliente transfirió dos veces el mismo monto; se facturó la segunda. Otra es personal. */
  const todas = [{ cuit: "27956214225", monto: 100000, nombre: "yan xiuyu", fecha: "2026-11-03", referencia: "R1" }, { cuit: "27956214225", monto: 100000, nombre: "yan xiuyu", fecha: "2026-11-12", referencia: "R2" }, { cuit: "27000000006", monto: 1500, nombre: "persona prueba", fecha: "2026-11-05", referencia: "R3" }];
  const base = { resultados, todasTransferencias: todas, emailReporte: "santamariapablodaniel@gmail.com", fecha: "2026-11-15", totalFacturado: 100000 };
  await reporteReal(sb, envios)({ ...base, impuestos: hasta15, fechaCorrida: "2026-11-15" });
  if (!envios.lista.length) console.log("ERRORES DEL REPORTE:", envios.errores);
  const m1 = envios.lista[0] || {};
  t("el email va AL CONTADOR con copia a Pablo", JSON.stringify(m1.to) === JSON.stringify(["Sticcoj@estudiosticco.com.ar"]) && JSON.stringify(m1.cc) === JSON.stringify(["santamariapablodaniel@gmail.com"]), { to: m1.to, cc: m1.cc });
  t("el cuerpo del email resume las retenciones nuevas", /Retenciones e Impuestos Banco/.test(m1.html || "") && new RegExp(hasta15.movimientos.length + " nuevos").test(m1.html || ""));
  const reg = sb.buckets["registros-internos"];
  t("lo enviado queda en un registro PRIVADO (bucket no público), por mes", reg && reg.public === false && Object.keys(reg.files).join() === "impuestos-bancarios/2026-11.json");
  const leerAdj = async (m) => { const wb = new ExcelJS.Workbook(); await wb.xlsx.load(Buffer.from(m.attachments[0].content, "base64")); return wb; };
  let wb1 = await leerAdj(m1); let hoja = wb1.getWorksheet("Retenciones e Impuestos Banco");
  t("el Excel trae las hojas de siempre y la nueva «Retenciones e Impuestos Banco»", ["Facturas Emitidas", "Notas de Crédito", "Extracto Bancario", "Retenciones e Impuestos Banco"].every(n => wb1.getWorksheet(n)), wb1.worksheets.map(w => w.name));
  const contar = (ws, txt) => { let n = 0; ws.eachRow(r => r.eachCell(c => { if (c.value === txt) n++; })); return n; };
  t("1ª corrida: todo lo del 1 al 15/11 figura como NUEVO", contar(hoja, "NUEVO") === hasta15.movimientos.length, { nuevos: contar(hoja, "NUEVO"), esperado: hasta15.movimientos.length });

  envios.lista = [];
  await reporteReal(sb, envios)({ ...base, impuestos: nov, fechaCorrida: "2026-11-19" });
  const m2 = envios.lista[0]; const wb2 = await leerAdj(m2); hoja = wb2.getWorksheet("Retenciones e Impuestos Banco");
  const nuevos19 = nov.movimientos.filter(m => m.fecha > "2026-11-15").length;
  t("2ª corrida con el extracto 1–19/11: solo lo del 16 al 19 es NUEVO; lo anterior va como «ya informado»", contar(hoja, "NUEVO") === nuevos19 && new RegExp(nuevos19 + " nuevos").test(m2.html), { nuevos: contar(hoja, "NUEVO"), esperado: nuevos19 });
  t("el pie del email dice el día de la corrida (19/11), no la fecha del período (15/11)", /Generado el 19\/11\/2026/.test(m2.html) && !/Generado el 2026-11-15/.test(m2.html), (m2.html.match(/Generado el [^<·]*/) || [])[0]);
  /* Se lee con SheetJS: ExcelJS, al releer, pierde los resultados que valen 0 (aunque el archivo trae <v>0</v>). */
  const sinResultado = []; let formulas = 0;
  const wbX = XLSX.read(Buffer.from(m2.attachments[0].content, "base64"), { cellFormula: true });
  wbX.SheetNames.forEach(n => { const ws = wbX.Sheets[n]; Object.keys(ws).filter(a => a[0] !== "!" && ws[a].f).forEach(a => { formulas++; if (typeof ws[a].v !== "number") sinResultado.push(n + "!" + a); }); });
  t("todas las fórmulas llevan el total ya calculado (se ven en la vista previa de Gmail/celular)", formulas > 0 && !sinResultado.length, { formulas, sinResultado: sinResultado.slice(0, 8) });
  t("el adjunto se llama Conciliacion_Noviembre_de_2026.xlsx (sin el año repetido)", m2.attachments[0].filename === "Conciliacion_Noviembre_de_2026.xlsx", m2.attachments[0].filename);
  const hIng = wb2.getWorksheet("Ingresos no facturados");
  t("el Excel trae la hoja «Ingresos no facturados» antes de la de retenciones", hIng && wb2.worksheets.map(w => w.name).join("|") === "Facturas Emitidas|Notas de Crédito|Extracto Bancario|Ingresos no facturados|Retenciones e Impuestos Banco", wb2.worksheets.map(w => w.name));
  const enSituacion = txt => { let n = 0; hIng.eachRow(r => { if (r.getCell(6).value === txt && r.getCell(5).value) n++; }); return n; }; /* columna «Situación» del detalle (filas con referencia) */
  t("… y explica las dos transferencias sin factura (una sin facturar, otra personal) y el email lo resume", enSituacion("Sin facturar") === 1 && enSituacion("No es venta") === 1 && m2.html.includes("2 sin factura de Mercado Limpio (no es venta 1 · sin facturar 1) · $101.500,00"), (m2.html.match(/Ingresos no facturados<\/strong>[\s\S]{0,200}/) || [])[0]);
  const hExt = wb2.getWorksheet("Extracto Bancario"); let fechaExt = null; hExt.eachRow(r => { if (r.getCell(5).value === "FA-A-00005-00000150") fechaExt = r.getCell(1).value; });
  t("«Extracto Bancario» muestra la fecha de LA transferencia facturada (12/11), no la primera del cliente (03/11)", fechaExt === "2026-11-12", fechaExt);
  const tot = []; hoja.eachRow(r => { if (r.getCell(1).value === "TOTAL") tot.push(r.getCell(7).value); });
  t("el TOTAL retenido calculado coincide con la suma de los movimientos", tot.length === 1 && Math.abs(tot[0].result - nov.movimientos.filter(m => !/^PAGO_/.test(m.regimen)).reduce((s, m) => s + m.importe, 0)) < 0.01, tot);
  envios.lista = [];
  await reporteReal(sb, envios)({ ...base, impuestos: nov, fechaCorrida: "2026-11-19" });
  hoja = (await leerAdj(envios.lista[0])).getWorksheet("Retenciones e Impuestos Banco");
  t("el mismo archivo subido otra vez: 0 nuevos (nada se repite)", contar(hoja, "NUEVO") === 0 && /0 nuevos/.test(envios.lista[0].html));

  console.log("\n4 · Fallas");
  const sb2 = supabaseFalso(), env2 = { lista: [], errores: [] };
  await reporteReal(sb2, env2, true)({ ...base, impuestos: hasta15, fechaCorrida: "2026-11-15" });
  t("si el email falla, NO se registra nada como enviado", !sb2.buckets["registros-internos"] || !Object.keys(sb2.buckets["registros-internos"].files).length);
  await reporteReal(sb2, env2)({ ...base, impuestos: hasta15, fechaCorrida: "2026-11-16" });
  hoja = (await leerAdj(env2.lista[0])).getWorksheet("Retenciones e Impuestos Banco");
  t("… y en la corrida siguiente esos movimientos van como NUEVOS", contar(hoja, "NUEVO") === hasta15.movimientos.length);
  const sb3 = supabaseFalso(); sb3.ctl.fallarRegistro = true; const env3 = { lista: [], errores: [] };
  await reporteReal(sb3, env3)({ ...base, impuestos: nov, fechaCorrida: "2026-11-19" });
  let avisa = false; (await leerAdj(env3.lista[0])).getWorksheet("Retenciones e Impuestos Banco").eachRow(r => r.eachCell(c => { if (/No se pudo verificar/.test(String(c.value))) avisa = true; }));
  t("registro caído: el reporte sale igual y la hoja avisa que no se pudo verificar lo ya enviado", env3.lista.length === 1 && avisa);
  const env4 = { lista: [], errores: [] };
  await reporteReal(supabaseFalso(), env4)({ ...base, impuestos: null, fechaCorrida: "2026-11-19" });
  let avisaPdf = false; (await leerAdj(env4.lista[0])).getWorksheet("Retenciones e Impuestos Banco").eachRow(r => r.eachCell(c => { if (/no era el Excel del banco/.test(String(c.value))) avisaPdf = true; }));
  t("extracto en PDF (sin retenciones leídas): la hoja lo explica y el reporte sale igual", env4.lista.length === 1 && avisaPdf);
  const env5 = { lista: [], errores: [] };
  await reporteReal(supabaseFalso(), env5)({ ...base, impuestos: { movimientos: [{ regimen: "ARBA_REC", fecha: "2026-11-20", importe: "no-es-numero" }] }, fechaCorrida: "2026-11-20" });
  t("datos raros de la app no rompen nada: el reporte sale", env5.lista.length === 1 && !env5.errores.length, env5.errores);

  if (SALIDA) { fs.writeFileSync(SALIDA, Buffer.from(m2.attachments[0].content, "base64")); console.log("\n  Excel de la 2ª corrida guardado en " + SALIDA); }
  console.log(`\n${ok} bien · ${mal} mal`); process.exit(mal ? 1 : 0);
})().catch(e => { console.error("ERROR", e); process.exit(1); });
