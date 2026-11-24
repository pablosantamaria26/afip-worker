/* ═══════════════════════════════════════════════════════════════════
   FACTURAR REMITOS DESDE LA CAJA (19/11/2026)

   La Caja de Queruclor no habla con ARCA: le pide la factura a este servidor, que reutiliza EXACTAMENTE la misma emisión que
   POST /facturar (facturarCore): padrón → tipo A/B → CAE con reintento 10016 → PDF → guardado en `facturas` → email.

   Lo que agrega este módulo:
     · Anti-duplicado por remito: `facturas.remito_ref` es único. Antes de crear el trabajo se consulta; y facturarCore vuelve a
       consultar ANTES de pedir el CAE (y registra la intención en afip_vouchers_pendientes). Un reintento devuelve la factura
       que ya existe: nunca una segunda.
     · Un solo trabajo por remito a la vez (bloqueo en memoria): dos toques seguidos comparten el mismo jobId.
     · Segundo plano: POST devuelve un jobId al instante; GET /estado-facturacion/:jobId informa el paso en curso
       (preparando → padron → cae → pdf → guardado → email → listo) y el resultado final. El estado se persiste en Storage
       al terminar, como el extracto; si el servidor se reinició a mitad, el estado dice «interrumpido» y la Caja consulta
       GET /factura-remito/:remito, que es la verdad (lo que quedó en `facturas`).
     · Factura emitida ≠ email enviado: el resultado trae los dos estados por separado (emailEnviado / emailError).

   24/11/2026 — bloqueo por remito que resiste pedidos simultáneos y reinicios:
     · El bloqueo en memoria se toma ANTES de cualquier espera (antes se tomaba después de consultar `facturas`: dos ventanas
       a la vez pasaban las dos y emitían dos facturas — reproducido).
     · Además queda una marca por remito en Storage (`remitos/lock_<REMITO>.json`), creada sin pisar (upsert:false): si ya
       existe, otro pedido la tiene. La marca guarda el número que se le va a pedir a ARCA ANTES de pedirlo; si el servidor se
       reinicia a mitad, el próximo pedido pregunta a ARCA por ESE comprobante (no por CUIT e importe) antes de emitir.
       Se borra cuando la factura quedó guardada en `facturas` (que pasa a ser la verdad) o cuando falló antes del CAE.

   Rutas:
     POST /facturar-remito            { remito, cuitCliente, emailCliente?, condicionVenta?, domicilioRemito?, items:[{descripcion, codigo?, cantidad, precioConIva, subtotalConIva}], total }
     GET  /estado-facturacion/:jobId
     GET  /factura-remito/:remito
     GET  /facturas-remitos?remitos=a,b,c
   ═══════════════════════════════════════════════════════════════════ */
"use strict";

const PASOS = ["preparando", "padron", "cae", "pdf", "guardado", "email", "listo"];

function filaFactura(f) {
  return {
    comprobante: f.comprobante, nroFactura: f.nro_factura, puntoVenta: f.punto_venta, cae: f.cae, cbteTipo: f.cbte_tipo,
    total: Number(f.total), pdfUrl: f.pdf_url || "", fecha: f.fecha, emailTo: f.email_to || "", emailStatus: f.email_status || "",
    emailError: f.email_error || "", remito: f.remito_ref || "", origen: f.origen || "",
  };
}

function montarFacturarRemito(app, deps) {
  const jobs = new Map();          // jobId → job
  const porRemito = new Map();     // remito → jobId (trabajo en curso)
  const sb = () => (typeof deps.supabase === "function" ? deps.supabase() : deps.supabase);
  const ahora = () => new Date().toISOString();
  const condDefault = deps.condicionVentaDefault || "Cuenta Corriente";
  const SELECT = "comprobante, nro_factura, punto_venta, cae, cbte_tipo, total, pdf_url, fecha, email_to, email_status, email_error, remito_ref, origen";

  async function facturasDe(remitos) {
    const s = sb(); if (!s) return {};
    const { data, error } = await s.from("facturas").select(SELECT).in("remito_ref", remitos).order("nro_factura", { ascending: true });
    if (error) throw new Error("No se pudo consultar facturas por remito: " + (error.message || error));
    const out = {};
    for (const f of data || []) { (out[f.remito_ref] = out[f.remito_ref] || []).push(filaFactura(f)); }
    return out;
  }

  async function persistir(job) {
    const s = sb(); if (!s || !deps.bucket) return;
    try {
      await s.storage.from(deps.bucket()).upload(`jobs/facturar_${job.jobId}.json`, Buffer.from(JSON.stringify(job)), { contentType: "application/json", upsert: true });
    } catch (e) { console.warn("⚠️ [Remito] No se pudo persistir el trabajo:", e?.message); }
  }
  async function cargar(jobId) {
    const s = sb(); if (!s || !deps.bucket) return null;
    try {
      const { data, error } = await s.storage.from(deps.bucket()).download(`jobs/facturar_${jobId}.json`);
      if (error || !data) return null;
      return JSON.parse(await data.text());
    } catch { return null; }
  }

  const limpiarRemito = v => String(v || "").trim().toUpperCase();
  const remitoValido = r => /^[A-Z0-9][A-Z0-9._|-]{2,59}$/.test(r);
  const soloDigitos = v => String(v || "").replace(/\D/g, "");

  /* ── Marca por remito en Storage (ver encabezado, 24/11/2026) ── */
  const INSTANCIA = `i_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
  const VIGENCIA_MS = deps.vigenciaBloqueoMs || 5 * 60 * 1000;   // una emisión dura < 1 min; más que esto sin dueño vivo = interrumpida
  const CARPETA = "remitos", rutaMarca = r => `${CARPETA}/lock_${r}.json`;
  const almacen = () => { const s = sb(); return s && deps.bucket ? s.storage.from(deps.bucket()) : null; };
  const yaExiste = e => /exist|duplicate|409/i.test(String((e && (e.message || e.error || e.statusCode)) || e || ""));
  async function tomarMarca(remito, datos) {
    const st = almacen(); if (!st) return { ok: true, sinAlmacen: true };
    const { error } = await st.upload(rutaMarca(remito), Buffer.from(JSON.stringify(datos)), { contentType: "application/json", upsert: false });
    if (!error) return { ok: true };
    if (yaExiste(error)) return { ok: false, existente: await leerMarca(remito) };
    throw new Error("No se pudo registrar el bloqueo del remito: " + (error.message || error));
  }
  async function leerMarca(remito) {
    const st = almacen(); if (!st) return null;
    const { data, error } = await st.download(rutaMarca(remito));
    if (error || !data) return null;
    try { return JSON.parse(await data.text()); } catch { return null; }
  }
  async function escribirMarca(remito, datos) {
    const st = almacen(); if (!st) return;
    const { error } = await st.upload(rutaMarca(remito), Buffer.from(JSON.stringify(datos)), { contentType: "application/json", upsert: true });
    if (error) throw new Error("No se pudo actualizar el bloqueo del remito: " + (error.message || error));
  }
  async function soltarMarca(remito) {
    const st = almacen(); if (!st) return;
    try { await st.remove([rutaMarca(remito)]); } catch (e) { console.warn("⚠️ [Remito] No se pudo soltar el bloqueo:", e?.message); }
  }
  /* Marcas de una lista de remitos con UNA sola consulta (las marcas se borran al terminar: la carpeta queda casi vacía). */
  async function marcasDe(remitos) {
    const st = almacen(); if (!st) return {};
    const { data, error } = await st.list(CARPETA, { limit: 1000 });
    if (error) throw new Error("No se pudo consultar las emisiones en curso: " + (error.message || error));
    const pedidos = new Set(remitos), out = {};
    for (const o of data || []) {
      const m = /^lock_(.+)\.json$/.exec(o.name || ""); if (!m || !pedidos.has(m[1])) continue;
      const marca = await leerMarca(m[1]); if (marca) out[m[1]] = marca;
    }
    return out;
  }
  /* Qué significa una marca para quien consulta (la Caja): en curso, sin confirmar (se verifica en ARCA al reintentar) o revisar. */
  function estadoMarca(m) {
    if (!m) return null;
    const vivo = m.instancia === INSTANCIA ? !!(porRemito.get(m.remito) && (jobs.get(porRemito.get(m.remito)) || {}).estado === "procesando")
      : Date.now() - Date.parse(m.actualizado || m.inicio || 0) < VIGENCIA_MS;
    if (m.estado === "autorizado_sin_guardar" || (m.intento && m.intento.autorizado)) {
      return { estado: "revisar", cae: (m.intento && m.intento.cae) || "", nro: (m.intento && m.intento.nro) || null,
        mensaje: "ARCA autorizó la factura" + (m.intento && m.intento.cae ? " (CAE " + m.intento.cae + ")" : "") + " pero no terminó de guardarse. Revisala en el historial de Facturación antes de hacer nada más." };
    }
    if (vivo) return { estado: "en_curso", inicio: m.inicio, mensaje: "Hay una emisión de este remito en curso." };
    return { estado: "sin_confirmar", inicio: m.inicio, mensaje: "La emisión anterior se interrumpió. Al reintentar se consulta a ARCA antes de emitir: nunca sale una segunda factura." };
  }
  /* Marca de una emisión que ya no está viva: ¿se puede soltar? Solo si ARCA confirma que ESE comprobante no existe. */
  async function resolverMarca(remito, m) {
    const e = estadoMarca(m);
    if (!e) return { liberar: true };
    if (e.estado === "revisar") return { status: 409, json: { ok: false, revisar: true, bloqueo: e, message: e.mensaje } };
    if (e.estado === "en_curso") {
      if (m.instancia === INSTANCIA && porRemito.get(remito)) return { status: 200, json: { ok: true, jobId: porRemito.get(remito), estado: "procesando", repetida: true } };
      return { status: 409, json: { ok: false, enCurso: true, bloqueo: e, message: e.mensaje + " Esperá un momento y actualizá: no se emitió otra." } };
    }
    const it = m.intento || {};
    if (!it.nro) return { liberar: true };                     // se cortó antes de pedir el CAE: no hay nada en ARCA
    if (typeof deps.comprobanteEnArca !== "function") return { status: 409, json: { ok: false, revisar: true, bloqueo: e, message: "No se puede verificar en ARCA si la emisión interrumpida llegó a autorizarse. Revisá el historial de Facturación." } };
    let info;
    try { info = await deps.comprobanteEnArca(it.pv, it.tipo, it.nro); }
    catch (err) { return { status: 503, json: { ok: false, bloqueo: e, message: "No se pudo consultar a ARCA si la emisión interrumpida se autorizó (" + (err?.message || err) + "). No se emitió nada: probá de nuevo en unos minutos." } }; }
    if (info && info.existe && soloDigitos(info.docNro) === soloDigitos(m.cuit)) {
      m.estado = "autorizado_sin_guardar"; m.intento = Object.assign({}, it, { autorizado: true, cae: info.cae || it.cae || "" });
      try { await escribirMarca(remito, m); } catch (er) { console.warn("⚠️ [Remito]", er.message); }
      const r = estadoMarca(m); return { status: 409, json: { ok: false, revisar: true, bloqueo: r, message: r.mensaje } };
    }
    if (info && info.existe) return { status: 409, json: { ok: false, revisar: true, bloqueo: e, message: "ARCA tiene el comprobante " + it.nro + " con otro receptor: hay que revisarlo en Facturación antes de seguir." } };
    return { liberar: true };                                   // ARCA confirma que ese comprobante no existe: la emisión no llegó
  }

  app.get("/facturas-remitos", async (req, res) => {
    try {
      const lista = String(req.query.remitos || "").split(",").map(limpiarRemito).filter(remitoValido).slice(0, 200);
      if (!lista.length) return res.json({ ok: true, facturas: {}, bloqueos: {} });
      const [facturas, marcas] = await Promise.all([facturasDe(lista), marcasDe(lista)]);
      const bloqueos = {};
      for (const r of Object.keys(marcas)) { if (!(facturas[r] || []).length) bloqueos[r] = estadoMarca(marcas[r]); }
      for (const r of lista) { const j = porRemito.get(r); if (j && !(facturas[r] || []).length && !bloqueos[r]) bloqueos[r] = { estado: "en_curso", jobId: j, mensaje: "Hay una emisión de este remito en curso." }; }
      res.json({ ok: true, facturas, bloqueos });
    } catch (e) { res.status(502).json({ ok: false, message: e.message }); }
  });

  app.get("/factura-remito/:remito", async (req, res) => {
    try {
      const r = limpiarRemito(req.params.remito);
      if (!remitoValido(r)) return res.status(400).json({ ok: false, message: "Remito inválido" });
      const [m, marca] = await Promise.all([facturasDe([r]), leerMarca(r)]);
      const enCurso = porRemito.get(r) || null;
      const facturas = m[r] || [];
      res.json({ ok: true, remito: r, facturas, jobEnCurso: enCurso, job: enCurso ? resumen(jobs.get(enCurso)) : null,
        bloqueo: facturas.length ? null : estadoMarca(marca) });
    } catch (e) { res.status(502).json({ ok: false, message: e.message }); }
  });

  function resumen(job) {
    if (!job) return null;
    return {
      ok: true, jobId: job.jobId, remito: job.remito, estado: job.estado, paso: job.paso, pasos: job.pasos,
      pasoNumero: Math.max(0, PASOS.indexOf(job.paso)) + 1, pasosTotal: PASOS.length,
      inicio: job.inicio, fin: job.fin || null, status: job.status || null,
      resultado: job.estado === "procesando" ? null : job.resultado || null,
      interrumpido: job.estado === "interrumpido",
    };
  }

  app.post("/facturar-remito", async (req, res) => {
    try {
      const b = req.body || {};
      const remito = limpiarRemito(b.remito);
      if (!remitoValido(remito)) return res.status(400).json({ ok: false, message: "Falta el número de remito (o no es válido)." });
      const cuit = soloDigitos(b.cuitCliente);
      if (cuit.length !== 11) return res.status(400).json({ ok: false, message: "CUIT inválido" });
      const items = Array.isArray(b.items) ? b.items : [];
      if (!items.length) return res.status(400).json({ ok: false, message: "El remito no tiene renglones para facturar." });
      const total = Number(b.total || 0);
      if (!(total > 0)) return res.status(400).json({ ok: false, message: "El total del remito tiene que ser mayor que cero." });

      /* 1. Bloqueo en memoria, tomado ANTES de cualquier espera: un segundo pedido (doble toque, otra ventana) que llega mientras
            este consulta la base encuentra el remito reservado y recibe el MISMO trabajo. */
      const enCurso = porRemito.get(remito);
      if (enCurso && jobs.get(enCurso) && jobs.get(enCurso).estado === "procesando") {
        return res.json({ ok: true, jobId: enCurso, estado: "procesando", repetida: true });
      }
      const jobId = `fac_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      const job = { jobId, remito, estado: "procesando", paso: "preparando", pasos: [{ paso: "preparando", en: ahora() }], inicio: ahora(), cuit, total };
      jobs.set(jobId, job); porRemito.set(remito, jobId);
      const desistir = () => { if (porRemito.get(remito) === jobId) porRemito.delete(remito); jobs.delete(jobId); };

      const marca = { remito, jobId, instancia: INSTANCIA, estado: "procesando", inicio: job.inicio, actualizado: job.inicio, cuit, total, intento: null };
      try {
        /* 2. Ya facturado: se devuelve lo que hay, sin tocar ARCA. */
        const ya = await facturasDe([remito]);
        if ((ya[remito] || []).length) { desistir(); return res.json({ ok: true, yaFacturado: true, remito, facturas: ya[remito] }); }
        /* 3. Marca persistente (otra instancia del servidor, o una emisión anterior que se cortó). */
        let t = await tomarMarca(remito, marca);
        if (!t.ok) {
          const r = await resolverMarca(remito, t.existente || {});
          if (!r.liberar) { desistir(); return res.status(r.status).json(r.json); }
          await soltarMarca(remito);
          t = await tomarMarca(remito, marca);
          if (!t.ok) { desistir(); return res.status(409).json({ ok: false, enCurso: true, message: "Otra emisión de este remito empezó recién. Actualizá: no se emitió otra." }); }
        }
      } catch (e) {
        desistir();
        return res.status(503).json({ ok: false, message: "No se pudo verificar si el remito ya tiene factura (" + (e?.message || e) + "). No se emitió nada: probá de nuevo." });
      }
      res.json({ ok: true, jobId, estado: "procesando", remito });

      /* La marca anota cada intento ANTES de pedirlo a ARCA (comprobante que se va a pedir) y el CAE apenas llega. */
      const onIntento = async d => {
        marca.intento = Object.assign({}, marca.intento, d); marca.actualizado = ahora();
        if (d.autorizado) marca.estado = "autorizado";
        await escribirMarca(remito, marca);
      };
      const cuerpo = Object.assign({}, b, { cuitCliente: cuit, condicionVenta: String(b.condicionVenta || condDefault) });
      Promise.resolve()
        .then(() => deps.facturarCore(cuerpo, { remitoRef: remito, origen: "caja", onIntento, onPaso: p => { job.paso = p; job.pasos.push({ paso: p, en: ahora() }); } }))
        .then(r => {
          job.status = r.status; job.resultado = r.json;
          job.estado = (r.status === 200 && r.json && r.json.ok) ? "terminado" : "error";
          if (job.estado === "terminado") { job.paso = "listo"; job.pasos.push({ paso: "listo", en: ahora() }); }
          else job.error = (r.json && (r.json.message || r.json.mensaje)) || ("HTTP " + r.status);
        }, e => { job.estado = "error"; job.status = 500; job.error = e?.message || String(e); job.resultado = { message: job.error }; })
        .then(async () => {
          job.fin = ahora();
          /* Terminó bien (la factura ya está en `facturas`) o falló antes del CAE: se suelta. CAE sin guardar: queda marcada para revisar. */
          if (job.estado === "error" && marca.intento && marca.intento.autorizado) {
            marca.estado = "autorizado_sin_guardar"; marca.actualizado = ahora();
            try { await escribirMarca(remito, marca); } catch (e) { console.warn("⚠️ [Remito]", e.message); }
          } else await soltarMarca(remito);
          if (porRemito.get(remito) === jobId) porRemito.delete(remito);
          await persistir(job);
        });
    } catch (err) {
      console.error("❌ [/facturar-remito]", err?.message || err);
      if (!res.headersSent) res.status(500).json({ ok: false, message: err?.message || "Error al iniciar la facturación" });
    }
  });

  app.get("/estado-facturacion/:jobId", async (req, res) => {
    const { jobId } = req.params;
    let job = jobs.get(jobId), recuperado = false;
    if (!job) { job = await cargar(jobId); recuperado = !!job; }
    if (!job) return res.status(404).json({ ok: false, message: "Trabajo no encontrado (el servidor pudo reiniciarse). Consultá /factura-remito/:remito: ahí está lo que quedó emitido." });
    if (recuperado && job.estado === "procesando") { job.estado = "interrumpido"; job.fin = job.fin || ahora(); }
    jobs.set(jobId, job);
    res.json(resumen(job));
  });

  return { jobs, porRemito, PASOS, facturasDe, leerMarca, INSTANCIA };
}

module.exports = { montarFacturarRemito, PASOS, filaFactura };
