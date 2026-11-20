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

  app.get("/facturas-remitos", async (req, res) => {
    try {
      const lista = String(req.query.remitos || "").split(",").map(limpiarRemito).filter(remitoValido).slice(0, 200);
      if (!lista.length) return res.json({ ok: true, facturas: {} });
      res.json({ ok: true, facturas: await facturasDe(lista) });
    } catch (e) { res.status(502).json({ ok: false, message: e.message }); }
  });

  app.get("/factura-remito/:remito", async (req, res) => {
    try {
      const r = limpiarRemito(req.params.remito);
      if (!remitoValido(r)) return res.status(400).json({ ok: false, message: "Remito inválido" });
      const m = await facturasDe([r]);
      const enCurso = porRemito.get(r) || null;
      res.json({ ok: true, remito: r, facturas: m[r] || [], jobEnCurso: enCurso, job: enCurso ? resumen(jobs.get(enCurso)) : null });
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

      /* Ya en curso: mismo trabajo (dos toques seguidos no emiten dos veces). */
      const enCurso = porRemito.get(remito);
      if (enCurso && jobs.get(enCurso) && jobs.get(enCurso).estado === "procesando") {
        return res.json({ ok: true, jobId: enCurso, estado: "procesando", repetida: true });
      }
      /* Ya facturado: se devuelve lo que hay, sin tocar ARCA. */
      const ya = await facturasDe([remito]);
      if ((ya[remito] || []).length) return res.json({ ok: true, yaFacturado: true, remito, facturas: ya[remito] });

      const jobId = `fac_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      const job = { jobId, remito, estado: "procesando", paso: "preparando", pasos: [{ paso: "preparando", en: ahora() }], inicio: ahora(), cuit, total };
      jobs.set(jobId, job); porRemito.set(remito, jobId);
      res.json({ ok: true, jobId, estado: "procesando", remito });

      const cuerpo = Object.assign({}, b, { cuitCliente: cuit, condicionVenta: String(b.condicionVenta || condDefault) });
      Promise.resolve()
        .then(() => deps.facturarCore(cuerpo, { remitoRef: remito, origen: "caja", onPaso: p => { job.paso = p; job.pasos.push({ paso: p, en: ahora() }); } }))
        .then(r => {
          job.status = r.status; job.resultado = r.json;
          job.estado = (r.status === 200 && r.json && r.json.ok) ? "terminado" : "error";
          if (job.estado === "terminado") { job.paso = "listo"; job.pasos.push({ paso: "listo", en: ahora() }); }
          else job.error = (r.json && (r.json.message || r.json.mensaje)) || ("HTTP " + r.status);
        }, e => { job.estado = "error"; job.status = 500; job.error = e?.message || String(e); job.resultado = { message: job.error }; })
        .then(async () => { job.fin = ahora(); porRemito.delete(remito); await persistir(job); });
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

  return { jobs, porRemito, PASOS, facturasDe };
}

module.exports = { montarFacturarRemito, PASOS, filaFactura };
