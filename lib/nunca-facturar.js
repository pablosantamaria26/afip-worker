/* ═══════════════════════════════════════════════════════════════════
   LISTA DE «NUNCA FACTURAR» (24/11/2026)

   Transferencias que entran a la cuenta pero NO son ventas de Mercado Limpio: empleados que pasan plata, familia, amigos,
   gastos personales. Pablo las marca una vez desde la app (por CUIT) y desde ahí:
     · el análisis del extracto las muestra destildadas («No facturar · Empleado»), fuera de «Facturar todo»;
     · al facturar, el servidor las saltea aunque alguna app vieja las mande.
   Se guarda en el almacenamiento PRIVADO de Supabase (bucket «registros-internos», archivo nunca-facturar.json): la misma
   lista para el celular y la PC. Cada alta o baja queda con fecha en el historial del propio archivo.
   ═══════════════════════════════════════════════════════════════════ */
"use strict";

const BUCKET = "registros-internos";
const ARCHIVO = "nunca-facturar.json";
const MOTIVOS = { empleado: "Empleado", familia: "Familia o amigos", personal: "Personal (no es venta)", otro: "Otro" };
const soloDig = v => String(v || "").replace(/\D/g, "");

async function almacen(sb) {
  const st = sb.storage;
  const { data } = await st.getBucket(BUCKET);
  if (!data) { const { error } = await st.createBucket(BUCKET, { public: false }); if (error && !/exist/i.test(error.message || "")) throw new Error("No se pudo crear el almacenamiento privado: " + error.message); }
  return st.from(BUCKET);
}
async function leer(sb) {
  if (!sb) throw new Error("sin base de datos");
  const b = await almacen(sb);
  const { data, error } = await b.download(ARCHIVO);
  if (error || !data) {
    /* Archivo inexistente = lista vacía. Cualquier otro error se informa (no se adivina). */
    if (error && !/not found|no existe|404|object not found/i.test(String(error.message || error.statusCode || ""))) throw new Error("No se pudo leer la lista de nunca facturar: " + (error.message || error));
    return { cuits: {}, historial: [] };
  }
  const j = JSON.parse(await data.text());
  return { cuits: j.cuits || {}, historial: Array.isArray(j.historial) ? j.historial : [] };
}
async function guardar(sb, lista) {
  const b = await almacen(sb);
  const { error } = await b.upload(ARCHIVO, Buffer.from(JSON.stringify(lista)), { contentType: "application/json", upsert: true });
  if (error) throw new Error("No se pudo guardar la lista de nunca facturar: " + error.message);
}

async function agregar(sb, { cuit, nombre, motivo, quien }) {
  const c = soloDig(cuit);
  if (c.length !== 11) throw Object.assign(new Error("El CUIT tiene que tener 11 dígitos."), { status: 400 });
  const m = MOTIVOS[motivo] ? motivo : "otro";
  const lista = await leer(sb);
  const ahora = new Date().toISOString();
  lista.cuits[c] = { nombre: String(nombre || "").slice(0, 80), motivo: m, quien: String(quien || "Pablo").slice(0, 40), fecha: ahora };
  lista.historial.push({ accion: "agregar", cuit: c, nombre: lista.cuits[c].nombre, motivo: m, fecha: ahora });
  await guardar(sb, lista);
  return lista;
}
async function quitar(sb, { cuit, quien }) {
  const c = soloDig(cuit);
  const lista = await leer(sb);
  if (!lista.cuits[c]) return lista;
  lista.historial.push({ accion: "quitar", cuit: c, nombre: lista.cuits[c].nombre, quien: String(quien || "Pablo").slice(0, 40), fecha: new Date().toISOString() });
  delete lista.cuits[c];
  await guardar(sb, lista);
  return lista;
}
const marca = (lista, cuit) => { const e = lista && lista.cuits[soloDig(cuit)]; return e ? { motivo: e.motivo, texto: MOTIVOS[e.motivo] || "Otro", nombre: e.nombre } : null; };
const resumen = lista => Object.entries(lista.cuits).map(([cuit, e]) => ({ cuit, nombre: e.nombre, motivo: e.motivo, texto: MOTIVOS[e.motivo] || "Otro", fecha: e.fecha }))
  .sort((a, b) => a.nombre.localeCompare(b.nombre));

/* Al FACTURAR: si está en la lista, no se emite. */
function decidir(t, lista) {
  const m = marca(lista, t.cuit);
  if (!m) return { emitir: true };
  return { emitir: false, resultado: { ok: true, skipped: true, nuncaFacturar: true, comprobante: "No facturar · " + m.texto, motivo: "En la lista de nunca facturar (" + m.texto + ")" } };
}

function montarNuncaFacturar(app, deps) {
  const sb = () => (typeof deps.supabase === "function" ? deps.supabase() : deps.supabase);
  const responder = (res, p) => p.then(l => res.json({ ok: true, lista: resumen(l), motivos: MOTIVOS }))
    .catch(e => res.status(e.status || 502).json({ ok: false, message: e.message || String(e) }));
  app.get("/nunca-facturar", (req, res) => responder(res, leer(sb())));
  app.post("/nunca-facturar", (req, res) => responder(res, agregar(sb(), req.body || {})));
  app.post("/nunca-facturar/quitar", (req, res) => responder(res, quitar(sb(), req.body || {})));
}

module.exports = { leer, agregar, quitar, marca, resumen, decidir, montarNuncaFacturar, MOTIVOS };
