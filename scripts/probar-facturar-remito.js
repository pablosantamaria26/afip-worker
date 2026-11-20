/* Prueba SIN red ni ARCA del módulo lib/facturar-remito.js y de la columna «Código» del PDF (19/11/2026).
     node scripts/probar-facturar-remito.js
   Servidor Express falso (junta las rutas), Supabase falso en memoria (tabla facturas + storage), facturarCore falso que simula los
   pasos y guarda la factura como lo haría el real. Nada sale a internet. */
const assert = (c, n, d) => { if (c) { ok++; console.log("  ✓ " + n); } else { mal++; console.log("  ✗ " + n + (d != null ? " — " + JSON.stringify(d).slice(0, 300) : "")); } };
let ok = 0, mal = 0;
const { montarFacturarRemito, PASOS } = require("../lib/facturar-remito");
const { renderComprobanteHtml } = require("../lib/pdf-comprobante");
const espera = ms => new Promise(r => setTimeout(r, ms));

/* ── Supabase falso ─────────────────────────────────────────────── */
function supabaseFalso() {
  const facturas = [], storage = {};
  const q = (tabla) => {
    const st = { tabla, filtros: [], orden: null, sel: null };
    const api = {
      select(s) { st.sel = s; return api; },
      in(col, vals) { st.filtros.push(r => vals.includes(r[col])); return api; },
      eq(col, v) { st.filtros.push(r => r[col] === v); return api; },
      order() { return api; },
      then(res, rej) { try { const rows = (tabla === "facturas" ? facturas : []).filter(r => st.filtros.every(f => f(r))); res({ data: rows.map(r => ({ ...r })), error: null }); } catch (e) { rej(e); } },
    };
    return api;
  };
  return {
    facturas, storage,
    from: q,
    storage: { from: () => ({ upload: async (p, buf) => { storage[p] = buf.toString(); return { data: {}, error: null }; }, download: async p => storage[p] ? { data: { text: async () => storage[p] }, error: null } : { data: null, error: { message: "no" } } }) },
    _storage: storage,
  };
}
/* ── Express falso ──────────────────────────────────────────────── */
function appFalsa() {
  const rutas = {};
  const app = { get: (p, h) => { rutas["GET " + p] = h; }, post: (p, h) => { rutas["POST " + p] = h; } };
  const llamar = async (metodo, ruta, { body, params, query } = {}) => {
    const h = rutas[metodo + " " + ruta]; if (!h) throw new Error("sin ruta " + metodo + " " + ruta);
    let status = 200, out = null, enviado = false;
    const res = { status(s) { status = s; return res; }, json(j) { out = j; enviado = true; return res; }, get headersSent() { return enviado; } };
    await h({ body: body || {}, params: params || {}, query: query || {} }, res);
    return { status, json: out };
  };
  return { app, llamar };
}

(async () => {
  console.log("\n1 · Columna «Código» del PDF");
  const base = { preview: true, tipo: { letra: "A", cod: 1, nombre: "FACTURA A", esB: false, esNC: false },
    emisor: { nombreVisible: "ML", razonSocial: "ML SRL", domicilio: "x", condicionIVA: "RI", cuit: "30711514179", iibb: "x", inicioActividades: "01/01/2020" },
    comprobante: { pv: "00005", nro: "00000001", fecha: "19/11/2026", cae: "VISTA PREVIA", caeVto: "", qr: "" },
    receptor: { cuit: "20321023561", nombre: "CLIENTE", condicionIVA: "Responsable Inscripto", condicionVenta: "Cuenta Corriente", domicilios: [{ etiqueta: "Domicilio", valor: "x" }] },
    referencia: null, notaParte: "", totales: { neto: 100, iva: 21, total: 121, descuento: null }, leyendas: [] };
  const conCod = renderComprobanteHtml({ ...base, items: [{ descripcion: "LAVANDINA 1L", codigo: "7791905023203", cantidad: 5, precioNeto: 20, subtotalNeto: 100, precioConIva: 24.2, subtotalConIva: 121 }] });
  const sinCod = renderComprobanteHtml({ ...base, items: [{ descripcion: "LAVANDINA 1L", cantidad: 5, precioNeto: 20, subtotalNeto: 100, precioConIva: 24.2, subtotalConIva: 121 }] });
  assert(/<th>Código<\/th>/.test(conCod) && /7791905023203/.test(conCod), "con código: aparece la columna «Código» y el EAN en la fila");
  assert(!/<th>Código<\/th>/.test(sinCod) && !/class="cod"/.test(sinCod), "sin código: el PDF queda exactamente como antes (sin columna)");
  const conCodB = renderComprobanteHtml({ ...base, tipo: { ...base.tipo, letra: "B", esB: true }, items: [{ descripcion: "X", codigo: "779", cantidad: 1, precioNeto: 1, subtotalNeto: 1, precioConIva: 1.21, subtotalConIva: 1.21 }] });
  assert(/<th>Código<\/th>/.test(conCodB), "también en Factura B");

  console.log("\n2 · Trabajo por remito: anti-duplicado, un solo trabajo a la vez, estados");
  const sb = supabaseFalso(); const { app, llamar } = appFalsa();
  let emisiones = 0, fallarProxima = false, demora = 120;
  const facturarCore = async (body, extra) => {
    emisiones++;
    const pasos = ["preparando", "padron", "cae", "pdf", "guardado", "email"];
    for (const p of pasos) { extra.onPaso(p); await espera(demora / pasos.length); if (p === "cae" && fallarProxima) { fallarProxima = false; return { status: 500, json: { message: "ARCA: error simulado" } }; } }
    const comp = "FA-00005-" + String(1000 + emisiones).padStart(8, "0");
    sb.facturas.push({ comprobante: comp, nro_factura: 1000 + emisiones, punto_venta: 5, cae: "7" + emisiones, cbte_tipo: 1, total: body.total, pdf_url: "https://x/" + comp + ".pdf", fecha: "2026-11-19", email_to: body.emailCliente || "", email_status: body.emailCliente ? "sent" : "skipped", email_error: "", remito_ref: extra.remitoRef, origen: extra.origen });
    return { status: 200, json: { ok: true, facturas: [{ comprobante: comp, nroFactura: 1000 + emisiones, cae: "7" + emisiones, total: body.total, pdfUrl: "https://x/" + comp + ".pdf" }], emailEnviado: !!body.emailCliente, emailError: body.emailCliente ? "" : "sin email", remito: extra.remitoRef } };
  };
  montarFacturarRemito(app, { facturarCore, supabase: () => sb, bucket: () => "b", condicionVentaDefault: "Cuenta Corriente" });
  const cuerpo = { remito: "QR-2026-000024", cuitCliente: "20-32102356-1", emailCliente: "c@x.com", items: [{ descripcion: "A", codigo: "779", cantidad: 5, precioConIva: 10, subtotalConIva: 50 }], total: 50 };

  let r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "" } });
  assert(r.status === 400 && /remito/i.test(r.json.message), "sin remito → 400");
  r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, cuitCliente: "123" } });
  assert(r.status === 400 && /CUIT/.test(r.json.message), "CUIT inválido → 400");
  r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, items: [] } });
  assert(r.status === 400, "sin renglones → 400");

  r = await llamar("POST", "/facturar-remito", { body: cuerpo });
  assert(r.status === 200 && r.json.ok && r.json.jobId && r.json.estado === "procesando", "inicia el trabajo y devuelve jobId al instante", r);
  const jobId = r.json.jobId;
  const r2 = await llamar("POST", "/facturar-remito", { body: cuerpo });
  assert(r2.json.jobId === jobId && r2.json.repetida === true, "un segundo toque mientras corre devuelve el MISMO trabajo (no emite otra)");
  let est = await llamar("GET", "/estado-facturacion/:jobId", { params: { jobId } });
  assert(est.json.estado === "procesando" && PASOS.includes(est.json.paso) && est.json.pasosTotal === 7 && est.json.resultado === null, "el estado informa el paso en curso y no adelanta resultado", est.json);
  let fr = await llamar("GET", "/factura-remito/:remito", { params: { remito: "qr-2026-000024" } });
  assert(fr.json.jobEnCurso === jobId && fr.json.facturas.length === 0, "la consulta por remito dice que hay un trabajo en curso");
  await espera(400);
  est = await llamar("GET", "/estado-facturacion/:jobId", { params: { jobId } });
  assert(est.json.estado === "terminado" && est.json.paso === "listo" && est.json.resultado.ok && est.json.resultado.facturas[0].comprobante === "FA-00005-00001001", "termina: resultado con la factura, CAE y PDF", est.json);
  assert(est.json.resultado.emailEnviado === true, "el estado del email viene aparte (enviado)");
  assert(emisiones === 1 && sb.facturas[0].remito_ref === "QR-2026-000024" && sb.facturas[0].origen === "caja", "se emitió UNA sola vez y quedó con remito_ref y origen=caja");
  assert(Object.keys(sb._storage).some(k => k.includes(jobId)), "el trabajo terminado se persistió en Storage");

  r = await llamar("POST", "/facturar-remito", { body: cuerpo });
  assert(r.json.yaFacturado === true && r.json.facturas[0].comprobante === "FA-00005-00001001" && emisiones === 1, "volver a pedir el mismo remito devuelve la factura existente SIN emitir");
  fr = await llamar("GET", "/facturas-remitos", { query: { remitos: "QR-2026-000024,QR-2026-000099" } });
  assert(fr.json.facturas["QR-2026-000024"].length === 1 && !fr.json.facturas["QR-2026-000099"], "consulta en lote: solo el facturado trae factura");

  fallarProxima = true;
  r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000025", emailCliente: "" } });
  await espera(400);
  est = await llamar("GET", "/estado-facturacion/:jobId", { params: { jobId: r.json.jobId } });
  assert(est.json.estado === "error" && /simulado/.test(est.json.resultado.message), "si ARCA falla, el trabajo queda en error con el motivo (y se puede reintentar)", est.json);
  r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000025", emailCliente: "" } });
  await espera(400);
  est = await llamar("GET", "/estado-facturacion/:jobId", { params: { jobId: r.json.jobId } });
  assert(est.json.estado === "terminado" && est.json.resultado.emailEnviado === false, "el reintento emite y, sin email, la factura sale igual con «email no enviado»");
  assert(sb.facturas.length === 2, "en total dos facturas (una por remito), ninguna duplicada");

  const mod = montarFacturarRemito(appFalsa().app, { facturarCore, supabase: () => sb, bucket: () => "b" });
  const { llamar: l2 } = (() => { const a = appFalsa(); montarFacturarRemito(a.app, { facturarCore, supabase: () => sb, bucket: () => "b" }); return a; })();
  est = await l2("GET", "/estado-facturacion/:jobId", { params: { jobId } });
  assert(est.json.estado === "terminado" && est.json.resultado.facturas[0].comprobante === "FA-00005-00001001", "tras un reinicio del servidor, el estado se recupera de Storage");
  est = await l2("GET", "/estado-facturacion/:jobId", { params: { jobId: "fac_inexistente" } });
  assert(est.status === 404 && /factura-remito/.test(est.json.message), "trabajo desconocido → 404 e indica consultar por remito");
  const cuerpoCond = { ...cuerpo, remito: "QR-2026-000026" }; let recibido = null;
  const { app: a3, llamar: l3 } = appFalsa(); montarFacturarRemito(a3, { facturarCore: async (b, x) => { recibido = b; return { status: 200, json: { ok: true, facturas: [] } }; }, supabase: () => sb, bucket: () => "b", condicionVentaDefault: "Cuenta Corriente" });
  await l3("POST", "/facturar-remito", { body: cuerpoCond }); await espera(50);
  assert(recibido && recibido.condicionVenta === "Cuenta Corriente" && recibido.cuitCliente === "20321023561" && recibido.items[0].codigo === "779", "al núcleo llegan: condición «Cuenta Corriente» por defecto, CUIT normalizado y el código de barras");

  console.log(`\n${ok} bien · ${mal} mal`); process.exit(mal ? 1 : 0);
})().catch(e => { console.error("ERROR", e); process.exit(1); });
