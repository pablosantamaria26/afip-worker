/* Prueba SIN red de lib/conciliar-ornella.js (24/11/2026) con datos INVENTADOS (no hay DNI reales en el repo).
     node scripts/probar-conciliar-ornella.js */
const O = require("../lib/conciliar-ornella");
let ok = 0, mal = 0;
const t = (n, c, d) => { c ? ok++ : mal++; console.log((c ? "  ✓ " : "  ✗ ") + n + (!c && d != null ? " — " + JSON.stringify(d).slice(0, 300) : "")); };
const tr = (fecha, monto, cuit) => ({ fecha, monto, cuit });
const pg = (id, fecha, importe, doc, comprobantes = "C 00003-00000001", estados = "emitida") => ({ id, fecha, importe, doc, comprobantes, estados });

(async () => {
  console.log("\n1 · Documento dentro del CUIT");
  t("DNI de 8 dígitos dentro del CUIT", O.docCoincide("42879880", "27428798800"));
  t("DNI de 7 dígitos (CUIT con 0 adelante)", O.docCoincide("9876543", "20098765437"));
  t("CUIT contra CUIT", O.docCoincide("27253322239", "27253322239"));
  t("DNI distinto: no", !O.docCoincide("31243900", "27227248144"));
  t("sin documento o CUIT inválido: no", !O.docCoincide("", "27227248144") && !O.docCoincide("31243900", "123"));

  console.log("\n2 · Emparejar");
  /* mismo día, mismo importe, dos huéspedes distintos: el documento decide */
  let r = O.emparejar([tr("2026-11-18", 308000, "27227248144"), tr("2026-11-18", 308000, "27312439005"), tr("2026-11-18", 308000, "20111111112")],
    [pg("a", "2026-11-18", 308000, "31243900", "C 00003-00000027"), pg("b", "2026-11-18", 308000, "22724814", "C 00003-00000026")]);
  t("mismo día y mismo importe: cada pago se empareja con SU transferencia por documento; la tercera (otra persona) queda libre",
    r.marcas[0] && r.marcas[0].comprobante === "C 00003-00000026" && r.marcas[1] && r.marcas[1].comprobante === "C 00003-00000027" && r.marcas[2] === null, r.marcas);
  r = O.emparejar([tr("2026-11-01", 336000, "27249842015"), tr("2026-11-09", 336000, "27249842015")], [pg("a", "2026-11-05", 336000, "24984201")]);
  t("ventana de fecha: transferencia 4 días ANTES del pago → sí; 4 días DESPUÉS → no", r.marcas[0] && r.marcas[0].tipo === "ornella" && r.marcas[1] === null, r.marcas);
  r = O.emparejar([tr("2026-11-06", 330000, "20178916999"), tr("2026-11-07", 330000, "20178916999")], [pg("a", "2026-11-07", 330000, "17891699")]);
  t("un pago se empareja con UNA sola transferencia (la de fecha más cercana)", r.marcas[1] && r.marcas[1].tipo === "ornella" && r.marcas[0] === null, r.marcas);
  r = O.emparejar([tr("2026-11-10", 150000, "20333333334")], [pg("a", "2026-11-10", 150000, "")]);
  t("pago SIN documento y un único candidato → «posible» (no se factura solo)", r.marcas[0] && r.marcas[0].tipo === "posible", r.marcas);
  r = O.emparejar([tr("2026-11-10", 150000, "20333333334"), tr("2026-11-11", 150000, "20444444445")], [pg("a", "2026-11-10", 150000, "")]);
  t("pago sin documento con DOS candidatos → no se marca ninguno (no se adivina)", r.marcas.every(m => m === null) && r.sinTransferencia.length === 1);
  r = O.emparejar([tr("2026-11-13", 554400, "27253322239")], [pg("a", "2026-11-13", 554400, "27253322239", "", "")]);
  t("pago de Ornella SIN factura emitida: igual es suyo (se marca, con estado «sin factura»)", r.marcas[0] && r.marcas[0].tipo === "ornella" && r.marcas[0].estado === "sin factura", r.marcas);
  r = O.emparejar([tr("2026-11-13", 554400, "27253322239")], [pg("a", "2026-11-13", 554401, "27253322239")]);
  t("importe distinto (aunque sea $1) → no", r.marcas[0] === null);
  r = O.emparejar([tr("2026-11-14", 88130, "20452262046")], []);
  t("cliente de Mercado Limpio (sin pago de Ornella) → libre", r.marcas[0] === null);

  console.log("\n3 · Qué se hace al facturar");
  let d = O.decidir({}, null, "timeout");
  t("si no se pudo consultar a Ornella: NO se emite y se informa el error", !d.emitir && d.resultado.ok === false && /Ornella/.test(d.resultado.error));
  d = O.decidir({}, { tipo: "ornella", comprobante: "C 00003-00000020" }, "");
  t("de Ornella: se omite con su comprobante", !d.emitir && d.resultado.skipped && d.resultado.ornella && /C 00003-00000020/.test(d.resultado.comprobante));
  d = O.decidir({}, { tipo: "posible" }, "");
  t("posible de Ornella: se omite salvo que Pablo la tilde a mano", !d.emitir && d.resultado.posible && O.decidir({ forzarOrnella: true }, { tipo: "posible" }, "").emitir === true);
  t("de Ornella confirmada: ni tildándola a mano se emite", O.decidir({ forzarOrnella: true }, { tipo: "ornella" }, "").emitir === false);
  t("sin marca: se emite normalmente", O.decidir({}, null, "").emitir === true);

  console.log("\n4 · Consulta a la base de Ornella (fetch falso)");
  let pedido = null;
  const fetchOk = async (url, o) => { pedido = { url, body: JSON.parse(o.body), apikey: o.headers.apikey }; return { ok: true, status: 200, json: async () => [{ pago_id: "x", fecha: "2026-11-13", importe: "440000.00", doc_nro: "23062566", comprobantes: "C 00003-00000020", estados: "emitida" }] }; };
  const v = await O.verificar([tr("2026-11-13", 440000, "20230625663"), tr("2026-11-02", 1000, "20999999990")], { clave: "secreta", fetch: fetchOk });
  t("consulta el rango que cubre el extracto (3 días antes del primero, 7 después del último) con la clave", pedido.body.p_desde === "2026-10-30" && pedido.body.p_hasta === "2026-11-20" && pedido.body.p_clave === "secreta" && /ml_pagos_para_conciliar/.test(pedido.url), pedido);
  t("y empareja con lo que devuelve", v.marcas[0] && v.marcas[0].tipo === "ornella" && v.marcas[1] === null);
  let err = null; try { await O.verificar([tr("2026-11-13", 1, "20999999990")], { clave: "", fetch: fetchOk }); } catch (e) { err = e.message; }
  t("sin clave configurada: falla con un mensaje claro (y quien llama no emite)", /ORNELLA_CLAVE/.test(err || ""), err);
  err = null; try { await O.verificar([tr("2026-11-13", 1, "20999999990")], { clave: "mala", fetch: async () => ({ ok: false, status: 400, json: async () => ({ message: "no autorizado" }) }) }); } catch (e) { err = e.message; }
  t("clave rechazada por la base: falla", /no autorizado/.test(err || ""), err);
  const vacio = await O.verificar([{ fecha: "", monto: 1, cuit: "" }], { clave: "x", fetch: async () => { throw new Error("no debería consultar"); } });
  t("sin fechas no consulta nada", vacio.marcas[0] === null && vacio.consultado === null);

  console.log(`\n${ok} bien · ${mal} mal`); process.exit(mal ? 1 : 0);
})().catch(e => { console.error("ERROR", e); process.exit(1); });
