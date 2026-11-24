/* Prueba SIN red de lib/nunca-facturar.js (24/11/2026). Supabase falso con buckets.
     node scripts/probar-nunca-facturar.js */
const N = require("../lib/nunca-facturar");
let ok = 0, mal = 0;
const t = (n, c, d) => { c ? ok++ : mal++; console.log((c ? "  ✓ " : "  ✗ ") + n + (!c && d != null ? " — " + JSON.stringify(d).slice(0, 300) : "")); };
function sbFalso() {
  const buckets = {}, ctl = { caido: false };
  const err = m => ({ data: null, error: { message: m } });
  return { buckets, ctl, storage: {
    getBucket: async n => ctl.caido ? err("caído") : buckets[n] ? { data: { name: n }, error: null } : err("not found"),
    createBucket: async (n, o) => { buckets[n] = { public: !!(o && o.public), files: {} }; return { data: {}, error: null }; },
    from: n => ({
      upload: async (p, buf) => { if (ctl.caido) return err("caído"); buckets[n].files[p] = buf.toString(); return { data: {}, error: null }; },
      download: async p => ctl.caido ? err("storage caído") : buckets[n] && buckets[n].files[p] ? { data: { text: async () => buckets[n].files[p] }, error: null } : err("Object not found"),
    }) } };
}
function appFalsa() {
  const rutas = {}; const app = { get: (p, h) => { rutas["GET " + p] = h; }, post: (p, h) => { rutas["POST " + p] = h; } };
  const llamar = (m, r, body) => new Promise(res => { let status = 200; const resp = { status(s) { status = s; return resp; }, json(j) { res({ status, json: j }); } }; rutas[m + " " + r]({ body: body || {} }, resp); });
  return { app, llamar };
}
(async () => {
  const sb = sbFalso(); const { app, llamar } = appFalsa(); N.montarNuncaFacturar(app, { supabase: () => sb });
  let r = await llamar("GET", "/nunca-facturar");
  t("lista vacía al principio (sin archivo todavía)", r.status === 200 && r.json.ok && r.json.lista.length === 0, r);
  t("el almacenamiento se crea PRIVADO", sb.buckets["registros-internos"] && sb.buckets["registros-internos"].public === false);
  r = await llamar("POST", "/nunca-facturar", { cuit: "20-41895386-2", nombre: "angel nicolas calderon", motivo: "empleado" });
  t("agregar a Nico como empleado (CUIT con guiones se normaliza)", r.json.ok && r.json.lista.length === 1 && r.json.lista[0].cuit === "20418953862" && r.json.lista[0].texto === "Empleado", r.json);
  r = await llamar("POST", "/nunca-facturar", { cuit: "27352293321", nombre: "lucia casal", motivo: "personal" });
  t("agregar a Lucía como personal", r.json.lista.length === 2 && r.json.lista.some(x => x.cuit === "27352293321" && x.texto === "Personal (no es venta)"));
  r = await llamar("POST", "/nunca-facturar", { cuit: "123", nombre: "x", motivo: "empleado" });
  t("CUIT inválido → 400", r.status === 400 && /11 dígitos/.test(r.json.message), r);
  r = await llamar("POST", "/nunca-facturar", { cuit: "27352293321", nombre: "lucia casal", motivo: "inventado" });
  t("motivo desconocido → «Otro» (y no se duplica el CUIT)", r.json.lista.length === 2 && r.json.lista.find(x => x.cuit === "27352293321").motivo === "otro");
  const lista = await N.leer(sb);
  t("marca por CUIT", N.marca(lista, "20418953862").texto === "Empleado" && N.marca(lista, "20452262046") === null);
  let d = N.decidir({ cuit: "20418953862" }, lista);
  t("al facturar: lo de la lista NO se emite y queda informado", !d.emitir && d.resultado.skipped && d.resultado.nuncaFacturar && /Empleado/.test(d.resultado.motivo));
  t("al facturar: un cliente cualquiera se emite", N.decidir({ cuit: "20452262046" }, lista).emitir === true);
  t("sin lista disponible (null): no frena (se respeta lo elegido en la app)", N.decidir({ cuit: "20418953862" }, null).emitir === true);
  r = await llamar("POST", "/nunca-facturar/quitar", { cuit: "27352293321" });
  t("quitar a Lucía", r.json.lista.length === 1 && r.json.lista[0].cuit === "20418953862");
  const l2 = await N.leer(sb);
  t("el historial guarda altas y bajas con fecha", l2.historial.length === 4 && l2.historial.map(h => h.accion).join(",") === "agregar,agregar,agregar,quitar" && l2.historial.every(h => h.fecha));
  sb.ctl.caido = true;
  r = await llamar("GET", "/nunca-facturar");
  t("almacenamiento caído → error claro (no se inventa una lista vacía)", r.status === 502 && r.json.ok === false, r);
  console.log(`\n${ok} bien · ${mal} mal`); process.exit(mal ? 1 : 0);
})().catch(e => { console.error("ERROR", e); process.exit(1); });
