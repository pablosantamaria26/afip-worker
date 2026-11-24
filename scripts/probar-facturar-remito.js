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
  /* ctl.demora: latencia de red de cada consulta (como el Supabase real; sin ella la carrera de dos pedidos no se ve).
     ctl.fallarStorage: el almacenamiento responde con error. */
  const ctl = { demora: 0, fallarStorage: false };
  const tarde = v => ctl.demora ? new Promise(r => setTimeout(() => r(v), ctl.demora)) : Promise.resolve(v);
  const q = (tabla) => {
    const st = { tabla, filtros: [], orden: null, sel: null };
    const api = {
      select(s) { st.sel = s; return api; },
      in(col, vals) { st.filtros.push(r => vals.includes(r[col])); return api; },
      eq(col, v) { st.filtros.push(r => r[col] === v); return api; },
      order() { return api; },
      then(res, rej) { try { const rows = (tabla === "facturas" ? facturas : []).filter(r => st.filtros.every(f => f(r))); tarde({ data: rows.map(r => ({ ...r })), error: null }).then(res); } catch (e) { rej(e); } },
    };
    return api;
  };
  const err = m => ({ data: null, error: { message: m } });
  return {
    facturas, storage, ctl,
    from: q,
    storage: { from: () => ({
      /* upsert:false = crear sin pisar (como Supabase: si existe responde «The resource already exists», 409) */
      upload: async (p, buf, o) => { if (ctl.fallarStorage) return tarde(err("storage caído")); if (o && o.upsert === false && storage[p] != null) return tarde({ data: null, error: { message: "The resource already exists", statusCode: "409" } }); storage[p] = buf.toString(); return tarde({ data: {}, error: null }); },
      download: async p => ctl.fallarStorage ? tarde(err("storage caído")) : tarde(storage[p] ? { data: { text: async () => storage[p] }, error: null } : err("no")),
      remove: async ps => { ps.forEach(p => { delete storage[p]; }); return tarde({ data: {}, error: null }); },
      list: async carpeta => ctl.fallarStorage ? tarde(err("storage caído")) : tarde({ data: Object.keys(storage).filter(k => k.startsWith(carpeta + "/")).map(k => ({ name: k.slice(carpeta.length + 1) })), error: null }),
    }) },
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
  let emisiones = 0, fallarProxima = false, fallarTrasCae = false, demora = 120;
  const facturarCore = async (body, extra) => {
    emisiones++;
    const pasos = ["preparando", "padron", "cae", "pdf", "guardado", "email"];
    for (const p of pasos) {
      extra.onPaso(p); await espera(demora / pasos.length);
      if (p === "cae" && fallarProxima) { fallarProxima = false; return { status: 500, json: { message: "ARCA: error simulado" } }; }
      /* como el real: el comprobante que se va a pedir se anota ANTES de pedirlo; el CAE, apenas llega */
      if (p === "cae" && extra.onIntento) { await extra.onIntento({ nro: 1000 + emisiones, pv: 5, tipo: 1 }); await extra.onIntento({ autorizado: true, cae: "7" + emisiones, nro: 1000 + emisiones }); }
      if (p === "pdf" && fallarTrasCae) { fallarTrasCae = false; return { status: 500, json: { message: "falló el guardado después del CAE (simulado)" } }; }
    }
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

  console.log("\n3 · Bloqueo por remito (24/11/2026): simultáneos, reinicios y verificación en ARCA");
  const marcaDe = r => sb._storage["remitos/lock_" + r + ".json"];
  const terminar = async () => { for (let i = 0; i < 40 && Object.values(sb._storage).length && [...(function* () { for (const k in sb._storage) if (k.startsWith("remitos/")) yield k; })()].some(k => JSON.parse(sb._storage[k]).estado === "procesando"); i++) await espera(50); await espera(80); };
  sb.ctl.demora = 40;   // latencia como la de Supabase real
  const antes1 = emisiones;
  const [v1, v2] = await Promise.all([llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000030" } }), llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000030" } })]);
  assert(v1.json.jobId && v1.json.jobId === v2.json.jobId && v2.json.repetida === true, "dos ventanas a la vez: la segunda recibe el MISMO trabajo (antes: dos trabajos y dos CAE)", [v1.json, v2.json]);
  assert(!!marcaDe("QR-2026-000030") && JSON.parse(marcaDe("QR-2026-000030")).estado === "procesando", "mientras emite queda la marca del remito en Storage");
  await espera(500); await terminar();
  assert(emisiones - antes1 === 1 && sb.facturas.filter(f => f.remito_ref === "QR-2026-000030").length === 1, "se pidió CAE UNA sola vez y hay una sola factura del remito", { emisiones: emisiones - antes1 });
  assert(!marcaDe("QR-2026-000030"), "al quedar guardada la factura, la marca se borra (la verdad pasa a ser `facturas`)");

  /* Remito NUEVO del mismo cliente y mismo importe que uno ya facturado: se emite (antes un voucher pendiente por CUIT+importe lo podía frenar). */
  const antes2 = emisiones;
  r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000031" } }); await espera(500); await terminar();
  assert(r.json.jobId && emisiones - antes2 === 1 && sb.facturas.filter(f => f.remito_ref === "QR-2026-000031").length === 1, "remito nuevo, mismo CUIT y mismo importe que uno facturado: se factura normalmente");

  /* Reinicio a mitad de una emisión: marca huérfana de otra instancia, vieja. */
  const vieja = new Date(Date.now() - 10 * 60 * 1000).toISOString();
  const huerfana = (r, extra) => { sb._storage["remitos/lock_" + r + ".json"] = JSON.stringify(Object.assign({ remito: r, jobId: "fac_viejo", instancia: "i_otra", estado: "procesando", inicio: vieja, actualizado: vieja, cuit: "20321023561", total: 50, intento: null }, extra)); };
  let arca = { existe: false }, consultasArca = 0, arcaFalla = false;
  const { app: a4, llamar: l4 } = appFalsa();
  montarFacturarRemito(a4, { facturarCore, supabase: () => sb, bucket: () => "b", comprobanteEnArca: async (pv, tipo, nro) => { consultasArca++; if (arcaFalla) throw new Error("ARCA no responde"); return arca; } });

  huerfana("QR-2026-000040", { intento: null });
  let n0 = emisiones; r = await l4("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000040" } }); await espera(500); await terminar();
  assert(r.json.jobId && emisiones - n0 === 1 && consultasArca === 0, "se cortó ANTES de pedir el CAE: se suelta y se emite (sin consultar ARCA)", r.json);

  huerfana("QR-2026-000041", { intento: { nro: 1500, pv: 5, tipo: 1 } }); arca = { existe: true, docNro: "20321023561", total: 50, cae: "8888" };
  n0 = emisiones; r = await l4("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000041" } });
  assert(r.status === 409 && r.json.revisar && emisiones === n0 && consultasArca === 1, "se cortó después de pedir el comprobante 1500 y ARCA LO TIENE: 409 «revisar», no se emite otra", r.json);
  assert(JSON.parse(marcaDe("QR-2026-000041")).estado === "autorizado_sin_guardar" && /CAE 8888/.test(r.json.message), "la marca queda «autorizado sin guardar» con el CAE que informó ARCA");
  let fl = await l4("GET", "/facturas-remitos", { query: { remitos: "QR-2026-000041,QR-2026-000030" } });
  assert(fl.json.bloqueos["QR-2026-000041"].estado === "revisar" && fl.json.facturas["QR-2026-000030"].length === 1 && !fl.json.bloqueos["QR-2026-000030"], "la consulta en lote informa el bloqueo «revisar» (y no lo confunde con un remito pendiente)", fl.json);
  n0 = emisiones; r = await l4("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000041" } });
  assert(r.status === 409 && emisiones === n0, "un reintento posterior sigue frenado (no emite nunca por las dudas)");

  huerfana("QR-2026-000042", { intento: { nro: 1501, pv: 5, tipo: 1 } }); arca = { existe: false };
  n0 = emisiones; r = await l4("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000042" } }); await espera(500); await terminar();
  assert(r.json.jobId && emisiones - n0 === 1, "ARCA confirma que ese comprobante NO existe: se suelta y se emite una vez", r.json);

  huerfana("QR-2026-000043", { intento: { nro: 1502, pv: 5, tipo: 1 } }); arcaFalla = true;
  n0 = emisiones; r = await l4("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000043" } });
  assert(r.status === 503 && emisiones === n0 && !!marcaDe("QR-2026-000043"), "si ARCA no responde, no se emite y la marca se conserva", r.json);
  arcaFalla = false;

  const reciente = new Date().toISOString();
  huerfana("QR-2026-000044", { inicio: reciente, actualizado: reciente });
  n0 = emisiones; r = await l4("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000044" } });
  assert(r.status === 409 && r.json.enCurso && emisiones === n0, "marca reciente de OTRA instancia (deploy a mitad): «en curso», no se emite", r.json);
  fl = await l4("GET", "/factura-remito/:remito", { params: { remito: "QR-2026-000044" } });
  assert(fl.json.bloqueo && fl.json.bloqueo.estado === "en_curso" && !fl.json.facturas.length, "la consulta por remito lo informa como «en curso»", fl.json);

  /* Falla DESPUÉS del CAE: la marca queda para revisar; falla ANTES: se suelta y se puede reintentar. */
  fallarTrasCae = true; n0 = emisiones;
  r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000045" } }); await espera(500); await terminar();
  assert(JSON.parse(marcaDe("QR-2026-000045") || "{}").estado === "autorizado_sin_guardar", "falló el guardado después del CAE: la marca queda «autorizado sin guardar»");
  r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000045" } });
  assert(r.status === 409 && r.json.revisar && emisiones - n0 === 1, "reintentar ese remito: 409 «revisar», no se pide otro CAE", r.json);
  fallarProxima = true;
  r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000046" } }); await espera(500); await terminar();
  assert(!marcaDe("QR-2026-000046"), "ARCA rechazó (sin CAE): la marca se suelta");
  n0 = emisiones; r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000046" } }); await espera(500); await terminar();
  assert(r.json.jobId && emisiones - n0 === 1, "y el reintento emite normalmente");

  sb.ctl.fallarStorage = true; n0 = emisiones;
  r = await llamar("POST", "/facturar-remito", { body: { ...cuerpo, remito: "QR-2026-000047" } });
  assert(r.status === 503 && emisiones === n0, "si no se puede registrar el bloqueo, no se emite (503)", r.json);
  sb.ctl.fallarStorage = false; sb.ctl.demora = 0;

  console.log(`\n${ok} bien · ${mal} mal`); process.exit(mal ? 1 : 0);
})().catch(e => { console.error("ERROR", e); process.exit(1); });
