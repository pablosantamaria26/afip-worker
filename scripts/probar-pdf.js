// Genera PDFs de prueba con el generador REAL (funciones de index.js + lib/pdf-comprobante.js)
// sin levantar el servidor ni emitir nada en ARCA.
// Uso: node scripts/probar-pdf.js <carpeta-salida>
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const QRCode = require("qrcode");
const lib = require("../lib/pdf-comprobante");
process.on("unhandledRejection", (e) => { console.error("RECHAZO SIN CAPTURAR:", e?.message || e); process.exitCode = 3; });

const salida = process.argv[2] || path.join(__dirname, "..", "pdf-pruebas");
fs.mkdirSync(salida, { recursive: true });

// ── Extraer las funciones reales del comprobante desde index.js ──
const src = fs.readFileSync(path.join(__dirname, "..", "index.js"), "utf8").replace(/\r\n/g, "\n");
const tramo = (desde, hasta) => {
  const a = src.indexOf(desde); const b = src.indexOf(hasta, a + 1);
  if (a < 0 || b < 0) throw new Error(`No encontré el tramo: ${desde}`);
  return src.slice(a, b);
};
const codigo = [
  tramo("const EMISOR = {", "function log("),
  tramo("const CONDICION_IVA_TXT = {", "const CONDICIONES_VALIDAS_CLASE_A"),
  tramo("const NC_TIPO_MAP = {", "function inferCbteTipoFromComprobante"),
  tramo("// Datos del emisor que van en todos los comprobantes", "// ROUTES"),
  tramo("function buildNotaCreditoHtml({", "app.post(\"/anular-comprobante\""),
].join("\n") + "\nmodule.exports = { buildFacturaHtml, buildNotaCreditoHtml };";
const ctx = {
  module: { exports: {} }, console,
  CBTE_TIPO_REAL: 1, CUIT_DISTRIBUIDORA: 23332382314,
  pad: (n, l) => String(n).padStart(l, "0"),
  round2: (n) => Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100,
  renderComprobanteHtml: lib.renderComprobanteHtml, fechaAR: lib.fechaAR,
};
vm.createContext(ctx);
vm.runInContext(codigo, ctx);
const { buildFacturaHtml, buildNotaCreditoHtml } = ctx.module.exports;

// ── Datos de prueba ──
const r2 = ctx.round2;
const CATALOGO = ["Make escobilla c/base", "Make secador doble goma 41cm", "Make escobillon super", "Romyl rejilla microfibra", "Samantha balde 10 lts",
  "Make esponja acero 12 gr", "Make guante classic M", "Make trapo de piso tejido reforzado blanco 50 x 60", "Make bolsas para consorcios verdes 90 x 120 cm x 10 u", "Make balde plastico oval con escurridor"];
function items(n, { largo = false } = {}) {
  return Array.from({ length: n }, (_, i) => {
    const cantidad = [6, 12, 1, 24, 6][i % 5];
    const precioConIva = r2(1500 + ((i * 7919) % 18000) + 0.37 * (i % 3));
    const subtotalConIva = r2(cantidad * precioConIva);
    const subtotalNeto = r2(subtotalConIva / 1.21);
    let descripcion = `${CATALOGO[i % CATALOGO.length]}${n > 10 ? ` (${i + 1})` : ""}`;
    if (largo && i % 3 === 0) descripcion += " — presentación mayorista en bulto cerrado, color surtido, reforzado para uso profesional intensivo en comercios y depósitos";
    return { descripcion, cantidad, precioConIva, subtotalConIva, subtotalNeto, precioNeto: r2(subtotalNeto / cantidad) };
  });
}
const totales = (its) => {
  const total = r2(its.reduce((s, x) => s + x.subtotalConIva, 0));
  const neto = r2(its.reduce((s, x) => s + x.subtotalNeto, 0));
  return { total, neto, iva: r2(total - neto) };
};
async function qr(tipoCmp, nro, total, cuit) {
  const p = { ver: 1, fecha: "2026-10-16", cuit: 23332382314, ptoVta: 5, tipoCmp, nroCmp: nro, importe: total, moneda: "PES", ctz: 1, tipoDocRec: 80, nroDocRec: Number(cuit), tipoCodAut: "E", codAut: 86428519907647 };
  return QRCode.toDataURL(`https://www.arca.gob.ar/fe/qr/?p=${Buffer.from(JSON.stringify(p)).toString("base64")}`, { margin: 2, width: 360 });
}
const RI = { cuit: "27956214225", nombre: "YAN XIUYU", condicionIVA: "IVA Responsable Inscripto", domicilioAfip: "LISANDRO DE LA TORRE 2560 - INGENIERO BUDGE - BUENOS AIRES - CP: 1773", domicilioRemito: "" };
const CF = { cuit: "27364836002", nombre: "FERRUFINO VANESA SOLEDAD", condicionIVA: "Consumidor Final", domicilioAfip: "", domicilioRemito: "" };
const MONO = { cuit: "20183258738", nombre: "D ONOFRIO OSCAR ALEJANDRO", condicionIVA: "Responsable Monotributo", domicilioAfip: "BOUCHARD 1596 - ADROGUE - BUENOS AIRES - CP: 1846", domicilioRemito: "" };
const LARGO = { cuit: "30715234569", nombre: "DISTRIBUIDORA INTEGRAL DE ARTICULOS DE LIMPIEZA Y BAZAR SAN JOSE DE FLORES SOCIEDAD DE RESPONSABILIDAD LIMITADA", condicionIVA: "IVA Responsable Inscripto", domicilioAfip: "AVENIDA PRESIDENTE TENIENTE GENERAL JUAN DOMINGO PERON 12345 PISO 3 DEPARTAMENTO B - SAN JUSTO - LA MATANZA - BUENOS AIRES - CP: 1754", domicilioRemito: "Calle de entrega 456, depósito trasero, Villa Luzuriaga" };

const casos = [
  { id: "01-A-1-item", tipo: 1, rec: RI, its: items(1), ref: ["14468573", "16/10/2026"] },
  { id: "02-A-5-items-original", tipo: 1, rec: RI, its: [
      { descripcion: "Make escobilla c/base", cantidad: 6, precioNeto: 10222.11, subtotalNeto: 61332.64, precioConIva: 12368.75, subtotalConIva: 74212.50 },
      { descripcion: "Make secador doble goma 41cm", cantidad: 6, precioNeto: 6266.46, subtotalNeto: 37598.78, precioConIva: 7582.42, subtotalConIva: 45494.52 },
      { descripcion: "Make escobillon 6 hileras", cantidad: 1, precioNeto: 64137.22, subtotalNeto: 64137.22, precioConIva: 77606.04, subtotalConIva: 77606.04 },
      { descripcion: "Romyl rejilla microfibra", cantidad: 6, precioNeto: 11831.37, subtotalNeto: 70988.23, precioConIva: 14315.96, subtotalConIva: 85895.76 },
      { descripcion: "Samantha balde 10 lts", cantidad: 1, precioNeto: 28041.26, subtotalNeto: 28041.26, precioConIva: 33929.93, subtotalConIva: 33929.93 } ],
    fijos: { neto: 262098.14, iva: 55040.61, total: 317138.75 }, ref: ["14468573", "16/10/2026"], nro: 1 },
  { id: "03-A-15-items", tipo: 1, rec: RI, its: items(15) },
  { id: "04-A-20-items", tipo: 1, rec: RI, its: items(20), ref: ["99887766", "14/10/2026"] },
  { id: "05-A-25-items", tipo: 1, rec: RI, its: items(25) },
  { id: "06-A-40-items", tipo: 1, rec: RI, its: items(40) },
  { id: "07-A-60-items", tipo: 1, rec: RI, its: items(60) },
  { id: "08-A-100-items", tipo: 1, rec: RI, its: items(100) },
  { id: "09-A-razon-social-larga", tipo: 1, rec: LARGO, its: items(8), venta: "Cuenta Corriente" },
  { id: "10-A-descripciones-largas", tipo: 1, rec: RI, its: items(18, { largo: true }) },
  { id: "11-B-consumidor-final", tipo: 6, rec: CF, its: items(6), ref: ["13724964", "17/10/2026"], nro: 9 },
  { id: "12-B-30-items", tipo: 6, rec: CF, its: items(30) },
  { id: "13-M-historica", tipo: 51, rec: RI, its: items(4), nro: 632 },
  { id: "14-A-monotributo-con-descuento", tipo: 1, rec: MONO, its: items(5), descuento: true },
  { id: "15-A-manual-sin-referencia", tipo: 1, rec: RI, its: items(3), venta: "Efectivo" },
  { id: "16-A-vista-previa", tipo: 1, rec: RI, its: items(3), preview: true },
  { id: "17-A-parte-1-de-2", tipo: 1, rec: RI, its: items(25), nota: "FACTURA PARTE 1 DE 2\nTotal remito: $ 1.234.567,00\nParte 1: $ 600.000,00" },
];

(async () => {
  const resumen = [];
  for (const c of casos) {
    const its = c.its;
    const t = c.fijos || totales(its);
    const nro = c.nro || 1;
    const html = buildFacturaHtml({
      receptor: c.rec, fechaISO: "2026-10-16", pv: 5, nro, items: its, neto: t.neto, iva: t.iva, total: t.total,
      cae: c.preview ? null : "86428519907647", caeVtoISO: c.preview ? null : "2026-10-26",
      condicionVenta: c.venta || "Transferencia Bancaria", qrDataUrl: c.preview ? null : await qr(c.tipo, nro, t.total, c.rec.cuit), isPreview: !!c.preview,
      notaFactura: c.nota || "", refBanco: c.ref ? c.ref[0] : "", refFechaTransf: c.ref ? c.ref[1] : "",
      subtotalBruto: c.descuento ? r2(t.total / 0.95) : 0, descuentoPct: c.descuento ? 5 : 0, descuentoImporte: c.descuento ? r2(t.total / 0.95 - t.total) : 0, totalFinal: t.total,
      cbteTipo: c.tipo,
    });
    if (process.env.HTML) fs.writeFileSync(path.join(salida, `${c.id}.html`), html);
    const pdf = await lib.generarPdf(html);
    const comp = `${c.tipo === 6 ? "B" : c.tipo === 51 ? "M" : "A"}-00005-${String(nro).padStart(8, "0")}`;
    const nombre = lib.nombreArchivoComprobante(comp, c.rec.nombre);
    fs.writeFileSync(path.join(salida, `${c.id}__${nombre}`), pdf);
    resumen.push({ caso: c.id, items: its.length, total: t.total, neto: t.neto, iva: t.iva, descripciones: its.map((x) => x.descripcion), archivo: nombre, kb: Math.round(pdf.length / 1024) });
  }
  // Notas de crédito
  for (const [id, tipoNC, rec, orig] of [["18-NC-A", 3, RI, "A-00005-00000001"], ["19-NC-B", 8, CF, "B-00005-00000009"], ["20-NC-M", 53, RI, "M-00005-00000632"]]) {
    const total = 317138.75;
    const html = buildNotaCreditoHtml({ emisor: { nombreVisible: "MERCADO LIMPIO DISTRIBUIDORA" }, receptor: { cuit: rec.cuit, nombre: rec.nombre, domicilioAfip: rec.domicilioAfip, condicionIVA: rec.condicionIVA },
      fechaISO: "2026-10-18", pv: 5, nro: 2, cae: "86428519911111", caeVtoISO: "2026-10-28", qrDataUrl: await qr(tipoNC, 2, total, rec.cuit), total,
      originalComprobante: orig, originalCae: "86428519907647", motivo: "Factura emitida por error", cbteTipo: tipoNC });
    const pdf = await lib.generarPdf(html);
    const comp = `NC-${tipoNC === 8 ? "B" : tipoNC === 53 ? "M" : "A"}-00005-00000002`;
    const nombre = lib.nombreArchivoComprobante(comp, rec.nombre);
    fs.writeFileSync(path.join(salida, `${id}__${nombre}`), pdf);
    resumen.push({ caso: id, items: 1, total, descripciones: [`Anulación de ${orig}`], archivo: nombre, kb: Math.round(pdf.length / 1024) });
  }
  fs.writeFileSync(path.join(salida, "resumen.json"), JSON.stringify(resumen, null, 1));
  console.table(resumen.map(({ descripciones, ...r }) => r));
})().catch((e) => { console.error(e); process.exit(1); });
