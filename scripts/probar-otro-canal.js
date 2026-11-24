/* Prueba SIN red de lib/facturas-otro-canal.js (24/11/2026), con datos INVENTADOS.
     node scripts/probar-otro-canal.js */
const C = require("../lib/facturas-otro-canal");
let ok = 0, mal = 0;
const t = (n, c, d) => { c ? ok++ : mal++; console.log((c ? "  ✓ " : "  ✗ ") + n + (!c && d != null ? " — " + JSON.stringify(d).slice(0, 320) : "")); };
const tr = (fecha, monto, cuit, nombre = "cliente") => ({ fecha, monto, cuit, nombre });
const fa = (comprobante, cuit, total, fecha, extra = {}) => Object.assign({ comprobante, cuit, nombre: "EMPRESA SA", total, fecha, origen: "caja", remito: "QR-2026-000099", email: "" }, extra);

(async () => {
  console.log("\n1 · Emparejar transferencias con facturas de la Caja / manuales");
  let r = C.emparejar([tr("2026-11-22", 1011930, "27228752563")], [fa("FA-A-00005-00000143", "27228752563", 1011930, "2026-11-24")]);
  t("mismo CUIT e importe exacto (aunque la factura sea 2 días posterior): «caja», no se factura", r.marcas[0] && r.marcas[0].tipo === "caja" && r.marcas[0].comprobante === "FA-A-00005-00000143", r.marcas);
  r = C.emparejar([tr("2026-11-21", 600000, "20352345165"), tr("2026-11-25", 556935, "20352345165"), tr("2026-11-26", 90000, "20352345165")], [fa("FA-A-00005-00000142", "20352345165", 1156935, "2026-11-20")]);
  t("pago en partes: dos transferencias del mismo CUIT que suman el total → las dos «caja»; la tercera queda libre (la factura ya está cubierta: es un pago nuevo)", r.marcas[0] && r.marcas[0].tipo === "caja" && r.marcas[1] && r.marcas[1].tipo === "caja" && r.marcas[2] === null, r.marcas);
  r = C.emparejar([tr("2026-11-09", 737390.15, "20300210040", "carlos daniel gimenez")], [fa("FA-A-00005-00000141", "30719055806", 1546213.32, "2026-11-20", { nombre: "FRAGANCIAS PERFUMERIAS S.A.", email: "carlosgimenez358@gmail.com" })]);
  t("el caso real: otro CUIT, pero el nombre coincide con el email de la factura → «posible» (revisar), con el comprobante", r.marcas[0] && r.marcas[0].tipo === "posible" && /FA-A-00005-00000141/.test(r.marcas[0].motivo) && /FRAGANCIAS/.test(r.marcas[0].motivo), r.marcas);
  r = C.emparejar([tr("2026-11-09", 737390.15, "20300210040", "carlos daniel gimenez")], [fa("FA-A-00005-00000141", "30719055806", 1546213.32, "2026-11-20", { email: "ventas@fragancias.com.ar" })]);
  t("otro CUIT y el email no tiene nada que ver → libre (se factura)", r.marcas[0] === null);
  r = C.emparejar([tr("2026-11-19", 88130, "20452262046", "valentin zheng")], [fa("FA-A-00005-00000141", "30719055806", 1546213.32, "2026-11-20", { email: "carlosgimenez358@gmail.com" })]);
  t("cliente sin relación con ninguna factura de la Caja → libre", r.marcas[0] === null);
  r = C.emparejar([tr("2026-08-01", 1011930, "27228752563")], [fa("FA-A-00005-00000143", "27228752563", 1011930, "2026-11-24")]);
  t("fuera de la ventana (transferencia 3 meses antes de la factura) → no se relaciona", r.marcas[0] === null);
  r = C.emparejar([tr("2026-11-10", 300000, "27228752563"), tr("2026-11-12", 300000, "27228752563")], [fa("FA-A-00005-00000150", "27228752563", 300000, "2026-11-10"), fa("FA-A-00005-00000151", "27228752563", 300000, "2026-11-11")]);
  t("dos facturas iguales al mismo CUIT y dos transferencias: cada una con la suya (nada queda libre, nada se duplica)", r.marcas.every(m => m && m.tipo === "caja") && new Set(r.marcas.map(m => m.comprobante)).size === 2, r.marcas);
  r = C.emparejar([tr("2026-11-10", 100000, "27228752563")], [fa("FA-M-00005-00000635", "27228752563", 173624.01, "2026-11-01", { origen: "manual", remito: "" })]);
  t("factura manual (no de la Caja) del mismo CUIT con otro importe → «posible» y lo dice", r.marcas[0] && r.marcas[0].tipo === "posible" && /otro canal/.test(r.marcas[0].motivo), r.marcas);

  console.log("\n2 · Nombre en el email y subconjuntos");
  t("«carlos daniel gimenez» ↔ carlosgimenez358@…", C.nombreEnEmail("carlos daniel gimenez", "carlosgimenez358@gmail.com"));
  t("«rossi paula gabriela» ↔ paularossi@…", C.nombreEnEmail("rossi paula gabriela", "paularossi@hotmail.com"));
  t("«jonathan ariel mollo» ↔ distribuidoramercadolimpio@… → no", !C.nombreEnEmail("jonathan ariel mollo", "distribuidoramercadolimpio@gmail.com"));
  t("una sola palabra corta («ana») no alcanza", !C.nombreEnEmail("ana", "ana123@gmail.com"));
  t("subconjunto: 600.000 + 556.935 = 1.156.935", JSON.stringify(C.subconjuntoQueSuma([600000, 90000, 556935], 1156935)) === "[0,2]");
  t("subconjunto: ninguno suma → null", C.subconjuntoQueSuma([100, 200, 300], 1000) === null);

  console.log("\n3 · Qué se hace al facturar");
  t("sin poder consultar las facturas: NO se emite", C.decidir({}, null, "timeout").emitir === false);
  let d = C.decidir({}, { tipo: "caja", comprobante: "FA-A-00005-00000142", total: 1156935, motivo: "pago en partes" }, "");
  t("ya facturada desde la Caja: se omite con su comprobante y total", !d.emitir && d.resultado.skipped && d.resultado.otroCanal && d.resultado.comprobante === "FA-A-00005-00000142" && d.resultado.total === 1156935);
  t("posible: se omite salvo que Pablo la tilde (forzarCaja)", C.decidir({}, { tipo: "posible", comprobante: "x", motivo: "m" }, "").emitir === false && C.decidir({ forzarCaja: true }, { tipo: "posible", comprobante: "x", motivo: "m" }, "").emitir === true);
  t("segura: ni tildándola se emite", C.decidir({ forzarCaja: true }, { tipo: "caja", comprobante: "x", motivo: "m" }, "").emitir === false);
  t("sin marca: se emite", C.decidir({}, null, "").emitir === true);

  console.log("\n4 · Lectura de la tabla (Supabase falso)");
  const filas = [
    { comprobante: "FA-A-00005-00000141", cuit_cliente: "30719055806", nombre_cliente: "FRAGANCIAS", total: 1546213.32, fecha: "2026-11-20", origen: "caja", remito_ref: "QR-2026-000024", condicion_venta: "Cuenta Corriente", email_to: "x@y.com", email_error: "", referencia_bancaria: null },
    { comprobante: "A-00005-00000100", cuit_cliente: "20111111112", nombre_cliente: "EXTRACTO SA", total: 100, fecha: "2026-11-20", origen: "", condicion_venta: "Transferencia Bancaria · EXTRACTO", email_to: "", email_error: "", referencia_bancaria: "123" },
    { comprobante: "A-00005-00000030", cuit_cliente: "27261524207", nombre_cliente: "ANULADA", total: 308000, fecha: "2026-10-18", origen: "", condicion_venta: "Transferencia Bancaria", email_to: "", email_error: "ANULADA POR NC-A-00005-00000001", referencia_bancaria: null },
    { comprobante: "FA-M-00005-00000635", cuit_cliente: "20940213380", nombre_cliente: "WU JINLAN", total: 173624.01, fecha: "2026-10-14", origen: "", condicion_venta: "Transferencia Bancaria", email_to: "", email_error: "", referencia_bancaria: null },
  ];
  const sb = { from: () => { const api = { select() { return api; }, gte() { return api; }, lte() { return api; }, gt() { return api; }, order() { return api; }, then(res) { res({ data: filas, error: null }); } }; return api; } };
  const fs = await C.cargarFacturasOtroCanal(sb, "2026-10-01", "2026-11-30");
  t("quedan solo las de la Caja y manuales vigentes (se descartan las del extracto y las anuladas)", fs.length === 2 && fs[0].origen === "caja" && fs[1].origen === "manual", fs.map(f => f.comprobante));
  const v = await C.verificar(sb, [tr("2026-11-21", 1546213.32, "30719055806")]);
  t("verificar: rango de 60 días antes y 45 después, y empareja", v.consultado.desde === "2026-09-22" && v.consultado.hasta === "2027-01-05" && v.marcas[0] && v.marcas[0].tipo === "caja", v);
  let err = null; try { await C.verificar({ from: () => ({ select() { return this; }, gte() { return this; }, lte() { return this; }, gt() { return this; }, order() { return this; }, then(res) { res({ data: null, error: { message: "caído" } }); } }) }, [tr("2026-11-21", 1, "30719055806")]); } catch (e) { err = e.message; }
  t("si la tabla no responde, falla (y quien llama no emite)", /No se pudo consultar/.test(err || ""), err);

  console.log(`\n${ok} bien · ${mal} mal`); process.exit(mal ? 1 : 0);
})().catch(e => { console.error("ERROR", e); process.exit(1); });
