// Comprobantes PDF de Mercado Limpio (facturas A/B/M y notas de crédito).
// Presentación únicamente: recibe importes ya calculados/autorizados y no recalcula nada fiscal.
//
// Paginación: el HTML trae los renglones en una plantilla y un script que, dentro del
// mismo Chrome que imprime, arma hojas A4 midiendo la altura real de cada renglón.
// No hay límite fijo de renglones por hoja; un renglón nunca se parte entre hojas y el
// bloque de totales + CAE + QR siempre queda junto en la última.
const fs = require("fs");
const path = require("path");

const ASSETS = path.join(__dirname, "..", "assets");
const b64 = (f) => { try { return fs.readFileSync(path.join(ASSETS, f)).toString("base64"); } catch { return ""; } };
const LOGO = b64("logo-mercado-limpio.png");
const FUENTES = {
  texto: b64("fonts/SourceSans3-var.woff2"),
  titulo600: b64("fonts/BarlowSC-600.woff2"),
  titulo700: b64("fonts/BarlowSC-700.woff2"),
};

const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const money = (n) => new Intl.NumberFormat("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(n || 0));
const fechaAR = (iso) => (iso && /^\d{4}-\d{2}-\d{2}/.test(String(iso)) ? String(iso).slice(0, 10).split("-").reverse().join("/") : String(iso || ""));

const IC = {
  user: '<svg viewBox="0 0 24 24"><circle cx="12" cy="8" r="4"/><path d="M4 21c0-4.4 3.6-7 8-7s8 2.6 8 7"/></svg>',
  bank: '<svg viewBox="0 0 24 24"><path d="M3 21h18"/><path d="M5 18v-7"/><path d="M9.5 18v-7"/><path d="M14.5 18v-7"/><path d="M19 18v-7"/><path d="M12 3 21 8H3z"/></svg>',
  info: '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><path d="M12 16v-5"/><path d="M12 8h.01"/></svg>',
  link: '<svg viewBox="0 0 24 24"><path d="M10 13a5 5 0 0 0 7.5.5l3-3a5 5 0 0 0-7-7l-1.7 1.7"/><path d="M14 11a5 5 0 0 0-7.5-.5l-3 3a5 5 0 0 0 7 7l1.7-1.7"/></svg>',
};

const CSS = `
@font-face { font-family: "MLTexto"; src: url(data:font/woff2;base64,${FUENTES.texto}) format("woff2"); font-weight: 200 900; }
@font-face { font-family: "MLTitulo"; src: url(data:font/woff2;base64,${FUENTES.titulo600}) format("woff2"); font-weight: 600; }
@font-face { font-family: "MLTitulo"; src: url(data:font/woff2;base64,${FUENTES.titulo700}) format("woff2"); font-weight: 700; }
@page { size: A4; margin: 0; }
:root { --navy: #1B2A41; --navy2: #24375A; --orange: #E8792B; --panel: #F2F5F9; --line: #DCE3EC; --gris: #5A6577; --txt: #1B2A41; }
* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { background: #fff; }
body { font-family: "MLTexto", "Source Sans 3", "Helvetica Neue", Arial, sans-serif; color: var(--txt); font-size: 10.5pt; line-height: 1.32; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
svg { width: 100%; height: 100%; fill: none; stroke: currentColor; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
.fuente { display: none; }
.hoja { width: 210mm; height: 296.6mm; position: relative; overflow: hidden; background: #fff; padding: 11mm 11mm 0; page-break-after: always; break-after: page; }
.hoja:last-child { page-break-after: auto; break-after: auto; }
.cuerpo { overflow: hidden; display: flex; flex-direction: column; gap: 4mm; }
.tit { font-family: "MLTitulo", "Arial Narrow", Arial, sans-serif; font-weight: 700; letter-spacing: .2px; }

/* Encabezado completo */
.cab { display: grid; grid-template-columns: 50mm 1px minmax(0, 1fr) 73mm; column-gap: 4.5mm; align-items: center; }
.cab .logo { display: flex; align-items: center; }
.cab .logo img { width: 50mm; height: auto; display: block; }
.sep-v { align-self: stretch; background: var(--line); margin: 3mm 0; }
.emisor .nombre { font-size: 15pt; line-height: 1.1; color: var(--navy); margin-bottom: 2.6mm; }
.emisor .dato { font-size: 8.8pt; color: var(--gris); margin-bottom: 1.8mm; line-height: 1.3; }
.emisor .dato b { color: var(--navy); font-weight: 700; }
.emisor .dato .v { display: block; color: var(--navy); font-weight: 700; }
.cab-der { background: var(--panel); border-radius: 3mm; padding: 3.5mm 4.5mm 4mm; }
.cab-tipo { display: flex; align-items: center; gap: 4mm; margin-bottom: 2.5mm; }
.letra { width: 19mm; height: 17.5mm; border: 1.3px solid var(--navy); border-radius: 2mm; background: #fff; display: flex; flex-direction: column; align-items: center; justify-content: center; flex: 0 0 auto; }
.letra b { font-family: "MLTexto", Arial, sans-serif; font-weight: 800; font-size: 25pt; line-height: 1; color: var(--navy); }
.letra span { font-size: 7.5pt; font-weight: 700; margin-top: 1mm; color: var(--navy); }
.titulo-cbte { font-size: 24pt; line-height: 1.05; color: var(--navy); padding-bottom: 1.8mm; border-bottom: 2.4px solid var(--orange); display: inline-block; }
.titulo-cbte.nc { font-size: 15.5pt; line-height: 1.12; }
.kv { display: grid; grid-template-columns: auto minmax(0, 1fr); column-gap: 5mm; row-gap: 1.05mm; font-size: 9pt; }
.kv dd { overflow-wrap: anywhere; }
.kv dt { color: var(--gris); }
.kv dd { font-weight: 700; color: var(--navy); }
.kv .hr { grid-column: 1 / -1; height: 1px; background: var(--line); margin: 1mm 0; }

/* Encabezado de continuación */
.cab2 { display: grid; grid-template-columns: 47mm 1px minmax(0, 1fr); column-gap: 5mm; align-items: center; }
.cab2 .logo img { width: 45mm; height: auto; display: block; }
.cab2-panel { background: var(--panel); border-radius: 3mm; padding: 3.5mm 5mm; display: grid; grid-template-columns: auto minmax(42mm, 1fr) 1px minmax(0, 66mm); column-gap: 5mm; align-items: center; }
.cab2-panel .letra { width: 17mm; height: 16mm; }
.cab2-panel .letra b { font-size: 21pt; }
.cab2-panel .titulo-cbte { font-size: 20pt; }
.cab2-panel .titulo-cbte.nc { font-size: 14pt; }
.cab2-panel .sub { font-size: 10pt; color: var(--gris); margin-top: 2.5mm; }
.cab2-panel .sep-v { margin: 0; }
.cab2-panel .kv { font-size: 9pt; row-gap: 1.2mm; }

/* Cliente */
.cliente { background: var(--panel); border-radius: 3mm; padding: 3mm 4mm 3.5mm; }
.cliente-tit { display: flex; align-items: center; gap: 3mm; font-size: 13pt; color: var(--navy); margin: 0 0 2.5mm 1mm; }
.ico-circ { width: 8mm; height: 8mm; border-radius: 50%; background: var(--navy); color: #fff; padding: 1.7mm; flex: 0 0 auto; }
.cliente-card { background: #fff; border-radius: 2.2mm; padding: 3.2mm 5mm; display: grid; grid-template-columns: minmax(0, 1fr) 1px minmax(0, 1fr); column-gap: 6mm; }
.cliente-card .col { display: flex; flex-direction: column; gap: 1.4mm; font-size: 10pt; color: var(--gris); min-width: 0; }
.cliente-card b { color: var(--navy); font-weight: 700; overflow-wrap: anywhere; }
.franja { display: flex; align-items: center; flex-wrap: wrap; gap: 2mm 5mm; background: var(--panel); border-radius: 2.2mm; padding: 2.2mm 5mm; margin-top: 2.5mm; font-size: 9.8pt; color: var(--gris); }
.cliente .franja { background: #fff; }
.franja .ico { width: 6mm; height: 6mm; color: var(--navy); flex: 0 0 auto; }
.franja .tag { font-weight: 700; color: var(--navy); letter-spacing: .3px; font-size: 9pt; text-transform: uppercase; }
.franja .div { width: 1px; align-self: stretch; background: var(--line); }
.franja b { color: var(--navy); }
.parte { display: inline-block; margin-left: auto; background: #FFF3E6; color: #9A4A0E; border-radius: 1.5mm; padding: 1mm 2.5mm; font-size: 8.5pt; font-weight: 700; white-space: pre-line; }

/* Tabla */
table.items { width: 100%; border-collapse: separate; border-spacing: 0; border: 1px solid var(--line); border-radius: 2.2mm; overflow: hidden; table-layout: fixed; }
.items thead th { background: var(--navy); color: #fff; font-family: "MLTitulo", Arial, sans-serif; font-weight: 600; font-size: 9.6pt; text-transform: uppercase; letter-spacing: .3px; padding: 3mm 3mm; line-height: 1.15; text-align: right; vertical-align: middle; }
.items thead th:first-child { text-align: left; padding-left: 4mm; }
.items thead th.c { text-align: center; }
.items td { padding: 2.3mm 3mm; border-top: 1px solid #E7ECF2; border-left: 1px solid #E7ECF2; font-size: 10pt; text-align: right; vertical-align: top; font-variant-numeric: tabular-nums; }
.items td:first-child { border-left: 0; text-align: left; padding-left: 4mm; overflow-wrap: anywhere; }
.items td.c { text-align: center; }
.items td.fuerte { font-weight: 700; }
.items tbody tr:nth-child(even) td { background: #F8FAFC; }
.items tbody tr:first-child td { border-top: 0; }

/* Cierre */
.cierre { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); column-gap: 6mm; align-items: stretch; }
.arca { border: 1px solid var(--line); border-radius: 2.6mm; background: #F7F9FC; padding: 4mm 4.5mm; display: grid; grid-template-columns: 33mm 1px minmax(0, 1fr); column-gap: 4.5mm; align-items: center; }
.arca img { width: 33mm; height: 33mm; display: block; image-rendering: pixelated; }
.arca .qr-vacio { width: 33mm; height: 33mm; border: 1px dashed #B8C2CF; display: flex; align-items: center; justify-content: center; color: #8A96A8; font-size: 8pt; }
.arca .txt { font-size: 9.4pt; color: var(--gris); }
.arca .txt .aut { padding-bottom: 3.5mm; margin-bottom: 3.5mm; border-bottom: 1px solid var(--line); color: var(--navy); }
.arca .txt div + div { margin-top: 1.5mm; }
.arca .txt b { color: var(--navy); }
.totales { border: 1px solid var(--line); border-radius: 2.6mm; padding: 3.5mm 5mm; background: #fff; }
.totales-tit { font-size: 13.5pt; letter-spacing: 1.6px; color: var(--navy); padding-bottom: 1.4mm; border-bottom: 2.2px solid var(--orange); margin-bottom: 2.6mm; }
.trow { display: flex; justify-content: space-between; gap: 4mm; font-size: 10pt; color: var(--gris); margin-bottom: 1.5mm; }
.trow b { color: var(--navy); font-variant-numeric: tabular-nums; white-space: nowrap; }
.trow.linea { border-bottom: 1px dashed var(--line); padding-bottom: 1.8mm; margin-bottom: 2.2mm; }
.total-final { display: flex; align-items: center; justify-content: space-between; gap: 3mm; background: var(--panel); border-radius: 2mm; padding: 1.6mm 1.6mm 1.6mm 3mm; margin-top: 2.4mm; }
.total-final .l { font-family: "MLTitulo", Arial, sans-serif; font-weight: 700; font-size: 13pt; color: var(--navy); }
.total-final .v { background: var(--navy); color: #fff; border-radius: 1.6mm; padding: 2mm 4mm; font-weight: 700; font-size: 13.5pt; white-space: nowrap; font-variant-numeric: tabular-nums; }
.transp { margin-top: 2.4mm; border: 1px dashed #B8C2CF; border-radius: 2mm; padding: 2mm 3mm; }
.transp .t { font-size: 7.8pt; font-weight: 700; text-transform: uppercase; letter-spacing: .3px; color: var(--gris); margin-bottom: 1.4mm; }
.transp .trow { margin-bottom: 1mm; font-size: 9.2pt; }
.leyendas { border-top: 1px dashed var(--line); padding-top: 2.5mm; text-align: center; font-size: 8.6pt; font-weight: 700; color: var(--gris); }
.leyendas div + div { margin-top: 1.2mm; }

/* Hoja intermedia */
.continua { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); column-gap: 6mm; }
.continua .info { background: var(--panel); border-radius: 2.6mm; padding: 3.5mm 4.5mm; color: var(--gris); font-size: 9.2pt; }
.continua .info .t { display: flex; align-items: center; gap: 3mm; font-size: 11.5pt; color: var(--navy); margin-bottom: 2mm; }
.continua .info .t .ico-circ { width: 7mm; height: 7mm; padding: 1.3mm; }
.continua .info p + p { border-top: 1px solid var(--line); margin-top: 2mm; padding-top: 2mm; }
.continua .acum { padding: 3.5mm 4.5mm; }
.continua .acum .totales-tit { font-size: 11.5pt; margin-bottom: 2.2mm; }
.continua .acum .trow { font-size: 9.3pt; margin-bottom: 1.2mm; }
.continua .acum .total-final { margin-top: 2mm; }
.continua .acum .total-final .l { font-size: 10.5pt; }
.continua .acum .total-final .v { font-size: 12pt; padding: 1.5mm 3mm; }
.continua .acum .nota { text-align: center; font-size: 8.2pt; color: var(--gris); margin-top: 1.5mm; }

/* Pie */
.pie { position: absolute; left: 11mm; right: 11mm; bottom: 8mm; height: 12mm; display: flex; justify-content: space-between; align-items: flex-end; font-size: 8.6pt; color: var(--gris); }
.pie::before { content: ""; position: absolute; left: 0; right: 0; top: -2.5mm; height: 2px; background: linear-gradient(90deg, var(--orange) 0%, var(--navy) 55%, var(--navy) 100%); }
.pie b { display: block; color: var(--navy); font-size: 9pt; }
.pie .pag { font-size: 9.5pt; }

/* Vista previa */
.marca { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; pointer-events: none; z-index: 5; }
.marca span { transform: rotate(-30deg); font-family: "MLTitulo", Arial, sans-serif; font-weight: 700; font-size: 64pt; color: rgba(232, 121, 43, .13); white-space: nowrap; }
.aviso-prev { background: #FFF4D6; color: #8A5A00; border-radius: 2mm; text-align: center; font-weight: 700; font-size: 9.5pt; padding: 2mm; }

@media screen {
  body { background: #E5E9EF; }
  #hojas { zoom: var(--z, 1); padding: 4mm 0; }
  .hoja { margin: 0 auto 6mm; box-shadow: 0 2px 12px rgba(15,27,45,.18); }
}
`;

// Paginador (corre en el navegador). Sin llaves dobles: el texto pasa por plantillas.
const SCRIPT = `
(function () {
  function q(id) { return document.getElementById(id); }
  function clon(id) { var t = q(id); return t ? t.content.cloneNode(true) : document.createDocumentFragment(); }
  function money(n) { return new Intl.NumberFormat("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n); }
  function paginar() {
    var cont = q("hojas");
    var filas = Array.prototype.slice.call(q("tpl-filas").content.querySelectorAll("tr"));
    var hojas = [];
    function nuevaHoja(primera) {
      var h = document.createElement("section"); h.className = "hoja";
      if (q("tpl-marca")) h.appendChild(clon("tpl-marca"));
      var c = document.createElement("div"); c.className = "cuerpo"; h.appendChild(c);
      h.appendChild(clon("tpl-pie"));
      cont.appendChild(h);
      var pie = h.querySelector(".pie");
      c.style.height = (pie.offsetTop - c.offsetTop - 12) + "px";
      c.appendChild(clon(primera ? "tpl-cab1" : "tpl-cab2"));
      var reg = { hoja: h, cuerpo: c, tbody: null, n: 0 };
      hojas.push(reg);
      return reg;
    }
    function tabla(reg) {
      var t = document.createElement("table"); t.className = "items";
      t.appendChild(clon("tpl-thead"));
      var tb = document.createElement("tbody"); t.appendChild(tb);
      reg.cuerpo.insertBefore(t, reg.cuerpo.querySelector(".continua"));
      reg.tbody = tb;
    }
    function desborda(reg) { return reg.cuerpo.scrollHeight > reg.cuerpo.clientHeight + 1; }
    function contin(reg) { var f = clon("tpl-continua"); reg.cuerpo.appendChild(f); return reg.cuerpo.querySelector(".continua"); }

    function quitarCierre(reg) {
      var els = reg.cuerpo.querySelectorAll(".bloque-cierre");
      for (var k = 0; k < els.length; k++) els[k].remove();
    }
    // ¿Entran todos los renglones que faltan + el cierre en esta hoja? Evita abrir
    // una hoja nueva solo para los últimos renglones.
    function cerrarAca(reg, caja, resto) {
      caja.remove();
      resto.forEach(function (f) { reg.tbody.appendChild(f); });
      reg.cuerpo.appendChild(clon("tpl-cierre"));
      if (!desborda(reg)) return true;
      quitarCierre(reg);
      resto.forEach(function (f) { f.remove(); });
      reg.cuerpo.appendChild(caja);
      return false;
    }

    var reg = nuevaHoja(true);
    var caja = contin(reg);
    if (filas.length) tabla(reg);
    var cerrado = false;
    for (var i = 0; i < filas.length; i++) {
      var fila = filas[i];
      reg.tbody.appendChild(fila);
      if (desborda(reg) && reg.n > 0) {
        reg.tbody.removeChild(fila);
        if (cerrarAca(reg, caja, filas.slice(i))) { cerrado = true; break; }
        reg = nuevaHoja(false); caja = contin(reg); tabla(reg);
        reg.tbody.appendChild(fila);
      }
      reg.n++;
    }
    // Cierre: totales + CAE + QR + leyendas juntos en la última hoja
    if (!cerrado) {
    caja.remove();
    reg.cuerpo.appendChild(clon("tpl-cierre"));
    }
    if (!cerrado && desborda(reg)) {
      var cierreEls = reg.cuerpo.querySelectorAll(".bloque-cierre");
      for (var k = 0; k < cierreEls.length; k++) cierreEls[k].remove();
      contin(reg);
      reg = nuevaHoja(false);
      reg.cuerpo.appendChild(clon("tpl-cierre"));
      var s = reg.hoja.querySelector(".cab2-panel .sub"); if (s) s.textContent = "Totales y autorización";
    }
    // Subtotales acumulados (informativos) en las hojas que continúan
    var total = hojas.length, cuenta = 0, idx = 0;
    var sumN = 0, sumI = 0, sumT = 0;
    hojas.forEach(function (h, p) {
      var filasHoja = h.tbody ? h.tbody.querySelectorAll("tr") : [];
      for (var j = 0; j < filasHoja.length; j++) {
        sumN += parseFloat(filasHoja[j].getAttribute("data-n") || 0);
        sumI += parseFloat(filasHoja[j].getAttribute("data-i") || 0);
        sumT += parseFloat(filasHoja[j].getAttribute("data-t") || 0);
      }
      var a = h.hoja.querySelector(".continua");
      if (a) {
        var setv = function (sel, v) { var e = a.querySelector(sel); if (e) e.textContent = "$ " + money(v); };
        setv(".acum-n", sumN); setv(".acum-i", sumI); setv(".acum-t", sumT);
      }
      var pg = h.hoja.querySelector(".pag"); if (pg) pg.textContent = "Página " + (p + 1) + " de " + total;
    });
    // Vista en pantalla: ajustar al ancho disponible
    var w = document.documentElement.clientWidth;
    var hojaW = hojas[0].hoja.offsetWidth;
    if (w && hojaW && w < hojaW + 16) document.documentElement.style.setProperty("--z", String(Math.max(0.3, (w - 12) / hojaW)));
    window.__paginado = true;
    document.documentElement.setAttribute("data-paginas", String(total));
  }
  function iniciar() { try { paginar(); } catch (e) { window.__paginado = true; window.__errorPaginado = String(e); } }
  if (document.fonts && document.fonts.ready) document.fonts.ready.then(iniciar); else iniciar();
})();
`;

// ── Plantillas ──────────────────────────────────────────────────
function cabeceraCompleta(d) {
  const t = d.tipo, e = d.emisor, c = d.comprobante;
  return `
  ${d.preview ? `<div class="aviso-prev">VISTA PREVIA · NO VÁLIDA COMO COMPROBANTE (todavía no autorizada por ARCA)</div>` : ""}
  <header class="cab">
    <div class="logo">${LOGO ? `<img src="data:image/png;base64,${LOGO}" alt="Mercado Limpio Distribuidora">` : `<div class="tit" style="font-size:20pt">Mercado Limpio</div>`}</div>
    <div class="sep-v"></div>
    <div class="emisor">
      <div class="nombre tit">${esc(e.nombreVisible)}</div>
      <div class="dato">Razón Social: <b>${esc(e.razonSocial)}</b></div>
      <div class="dato">Domicilio Comercial:<span class="v">${esc(e.domicilio)}</span></div>
      <div class="dato">Condición frente al IVA:<span class="v">${esc(e.condicionIVA)}</span></div>
    </div>
    <div class="cab-der">
      <div class="cab-tipo">
        <div class="letra"><b>${esc(t.letra)}</b><span>COD. ${esc(t.cod)}</span></div>
        <div class="titulo-cbte tit${t.esNC ? " nc" : ""}">${esc(t.nombre)}</div>
      </div>
      <dl class="kv">
        <dt>Punto de Venta:</dt><dd>${esc(c.pv)}</dd>
        <dt>Comp. Nro:</dt><dd>${esc(c.nro)}</dd>
        <dt>Fecha de Emisión:</dt><dd>${esc(c.fecha)}</dd>
        <div class="hr"></div>
        <dt>CUIT:</dt><dd>${esc(e.cuit)}</dd>
        <dt>Ingresos Brutos:</dt><dd>${esc(e.iibb)}</dd>
        <dt>Inicio de Actividades:</dt><dd>${esc(e.inicioActividades)}</dd>
      </dl>
    </div>
  </header>
  ${bloqueCliente(d)}`;
}

function bloqueCliente(d) {
  const r = d.receptor;
  const dom = r.domicilios.map((x) => `<div>${esc(x.etiqueta)}: <b>${esc(x.valor)}</b></div>`).join("");
  let franja = "";
  if (d.asociado) {
    franja = `<div class="franja"><span class="ico">${IC.link}</span><span class="tag">Comprobante asociado</span><span class="div"></span><span><b>${esc(d.asociado.comprobante)}</b></span>${d.asociado.cae ? `<span class="div"></span><span>CAE: <b>${esc(d.asociado.cae)}</b></span>` : ""}${d.asociado.motivo ? `<span class="div"></span><span>Motivo: <b>${esc(d.asociado.motivo)}</b></span>` : ""}</div>`;
  } else if (d.referencia && (d.referencia.ref || d.referencia.fecha)) {
    franja = `<div class="franja"><span class="ico">${IC.bank}</span><span class="tag">Transf. bancaria</span>${d.referencia.ref ? `<span class="div"></span><span>Ref: <b>${esc(d.referencia.ref)}</b></span>` : ""}${d.referencia.fecha ? `<span class="div"></span><span>Fecha: <b>${esc(d.referencia.fecha)}</b></span>` : ""}</div>`;
  }
  return `
  <section class="cliente">
    <div class="cliente-tit tit"><span class="ico-circ">${IC.user}</span>DATOS DEL CLIENTE${d.notaParte ? `<span class="parte">${esc(d.notaParte)}</span>` : ""}</div>
    <div class="cliente-card">
      <div class="col">
        <div>CUIT: <b>${esc(r.cuit)}</b></div>
        <div>Condición frente al IVA: <b>${esc(r.condicionIVA)}</b></div>
        <div>Condición de Venta: <b>${esc(r.condicionVenta)}</b></div>
      </div>
      <div class="sep-v" style="margin:0"></div>
      <div class="col">
        <div>Apellido y Nombre / Razón Social: <b>${esc(r.nombre)}</b></div>
        ${dom}
      </div>
    </div>
    ${franja}
  </section>`;
}

function cabeceraContinuacion(d) {
  const t = d.tipo, c = d.comprobante;
  return `
  <header class="cab2">
    <div class="logo">${LOGO ? `<img src="data:image/png;base64,${LOGO}" alt="Mercado Limpio Distribuidora">` : ""}</div>
    <div class="sep-v"></div>
    <div class="cab2-panel">
      <div class="letra"><b>${esc(t.letra)}</b><span>COD. ${esc(t.cod)}</span></div>
      <div><div class="titulo-cbte tit${t.esNC ? " nc" : ""}">${esc(t.nombre)}</div><div class="sub">Continuación del detalle</div></div>
      <div class="sep-v"></div>
      <dl class="kv">
        <dt>Punto de Venta:</dt><dd>${esc(c.pv)}</dd>
        <dt>Comp. Nro:</dt><dd>${esc(c.nro)}</dd>
        <dt>Cliente:</dt><dd>${esc(d.receptor.nombre)}</dd>
        <dt>CUIT:</dt><dd>${esc(d.receptor.cuit)}</dd>
      </dl>
    </div>
  </header>`;
}

function encabezadoTabla(d) {
  return d.tipo.esB
    ? `<colgroup><col><col style="width:16mm"><col style="width:34mm"><col style="width:38mm"></colgroup>
       <thead><tr><th>Producto / Servicio</th><th class="c">Cant</th><th>Precio Unit.</th><th>Subtotal</th></tr></thead>`
    : `<colgroup><col><col style="width:15mm"><col style="width:27mm"><col style="width:28mm"><col style="width:18mm"><col style="width:30mm"></colgroup>
       <thead><tr><th>Producto / Servicio</th><th class="c">Cant</th><th>Precio<br>Unit.</th><th>Subtotal<br>Neto</th><th class="c">Alic.<br>IVA</th><th>Subtotal<br>C/IVA</th></tr></thead>`;
}

function filas(d) {
  return d.items.map((it) => {
    const attrs = `data-n="${Number(it.subtotalNeto || 0)}" data-i="${Number(it.subtotalConIva || 0) - Number(it.subtotalNeto || 0)}" data-t="${Number(it.subtotalConIva || 0)}"`;
    return d.tipo.esB
      ? `<tr ${attrs}><td>${esc(it.descripcion)}</td><td class="c">${esc(it.cantidad)}</td><td>$ ${money(it.precioConIva)}</td><td class="fuerte">$ ${money(it.subtotalConIva)}</td></tr>`
      : `<tr ${attrs}><td>${esc(it.descripcion)}</td><td class="c">${esc(it.cantidad)}</td><td>$ ${money(it.precioNeto)}</td><td>$ ${money(it.subtotalNeto)}</td><td class="c">21%</td><td class="fuerte">$ ${money(it.subtotalConIva)}</td></tr>`;
  }).join("");
}

function cierre(d) {
  const t = d.totales, c = d.comprobante;
  const desc = t.descuento ? `
      <div class="trow"><span>Subtotal (remito) c/IVA:</span><b>$ ${money(t.descuento.bruto)}</b></div>
      <div class="trow linea"><span>Descuento (${money(t.descuento.pct)}%):</span><b>−$ ${money(t.descuento.importe)}</b></div>` : "";
  const filasTot = d.tipo.esB
    ? `<div class="trow"><span>Subtotal:</span><b>$ ${money(t.total)}</b></div>
       <div class="trow"><span>Importe Otros Tributos:</span><b>$ 0,00</b></div>`
    : `<div class="trow"><span>Importe Neto Gravado:</span><b>$ ${money(t.neto)}</b></div>
       <div class="trow"><span>IVA 21%:</span><b>$ ${money(t.iva)}</b></div>
       <div class="trow"><span>Importe Otros Tributos:</span><b>$ 0,00</b></div>`;
  const transp = d.tipo.esB ? `
      <div class="transp"><div class="t">Régimen de Transparencia Fiscal al Consumidor (Ley 27.743)</div>
        <div class="trow"><span>IVA Contenido:</span><b>$ ${money(t.iva)}</b></div>
        <div class="trow"><span>Otros Impuestos Nacionales Indirectos:</span><b>$ 0,00</b></div></div>` : "";
  return `
  <section class="cierre bloque-cierre">
    <div class="arca">
      ${c.qr ? `<img src="${c.qr}" alt="QR ARCA">` : `<div class="qr-vacio">QR ARCA</div>`}
      <div class="sep-v" style="margin:0"></div>
      <div class="txt">
        <div class="aut">${d.preview ? "Vista previa: no autorizado" : "Comprobante Autorizado por ARCA"}</div>
        <div><b>CAE Nro:</b> ${esc(c.cae || "—")}</div>
        <div><b>Fecha Vto. CAE:</b> ${esc(c.caeVto || "—")}</div>
      </div>
    </div>
    <div class="totales">
      <div class="totales-tit tit">TOTALES</div>
      ${desc}${filasTot}
      <div class="total-final"><span class="l">IMPORTE TOTAL:</span><span class="v">$ ${money(t.total)}</span></div>
      ${transp}
    </div>
  </section>
  ${d.leyendas.length ? `<section class="leyendas bloque-cierre">${d.leyendas.map((l) => `<div>${esc(l)}</div>`).join("")}</section>` : ""}`;
}

function continuacion(d) {
  return `
  <section class="continua">
    <div class="info">
      <div class="t tit"><span class="ico-circ">${IC.info}</span>CONTINUACIÓN DE ${esc(d.tipo.nombre)}</div>
      <p>El detalle de productos continúa en la página siguiente.</p>
      <p>Los totales, el CAE y el código QR se encuentran en la última página de este comprobante.</p>
    </div>
    <div class="totales acum">
      <div class="totales-tit tit">SUBTOTAL ACUMULADO</div>
      ${d.tipo.esB ? "" : `<div class="trow"><span>Subtotal Neto Acumulado:</span><b class="acum-n">—</b></div>
      <div class="trow"><span>IVA 21% Acumulado:</span><b class="acum-i">—</b></div>`}
      <div class="total-final"><span class="l">${d.tipo.esB ? "Subtotal Acumulado:" : "Subtotal c/IVA Acumulado:"}</span><span class="v acum-t">—</span></div>
      <div class="nota">Informativo · continúa en la página siguiente</div>
    </div>
  </section>`;
}

function pie(d) {
  return `<footer class="pie"><div><b>${esc(d.emisor.nombreVisible)}</b>${esc(d.emisor.domicilio)}</div><div class="pag">Página 1 de 1</div></footer>`;
}

function renderComprobanteHtml(d) {
  const titulo = `${d.tipo.nombre} ${d.tipo.letra} ${d.comprobante.pv}-${d.comprobante.nro}`;
  return `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>${esc(titulo)}</title><style>${CSS}</style></head>
<body>
<main id="hojas"></main>
<template id="tpl-cab1">${cabeceraCompleta(d)}</template>
<template id="tpl-cab2">${cabeceraContinuacion(d)}</template>
<template id="tpl-thead">${encabezadoTabla(d)}</template>
<template id="tpl-filas"><table><tbody>${filas(d)}</tbody></table></template>
<template id="tpl-cierre">${cierre(d)}</template>
<template id="tpl-continua">${continuacion(d)}</template>
<template id="tpl-pie">${pie(d)}</template>
${d.preview ? `<template id="tpl-marca"><div class="marca"><span>VISTA PREVIA</span></div></template>` : ""}
<script>${SCRIPT}</script>
</body></html>`;
}

// ── Motor PDF (mismo Chromium que usaba html-pdf-node) ──────────
// Reintenta una vez: Chromium a veces corta la conexión al exportar ("Target closed")
async function generarPdf(html) {
  try {
    return await generarPdfUnaVez(html);
  } catch (e) {
    console.warn("⚠️ [PDF] Reintentando generación:", e?.message);
    return generarPdfUnaVez(html);
  }
}

async function generarPdfUnaVez(html) {
  const puppeteer = require("puppeteer");
  const browser = await puppeteer.launch({ args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--font-render-hinting=none"] });
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 900, height: 1200 });
    await page.setContent(html, { waitUntil: "load" });
    await page.waitForFunction("window.__paginado === true", { timeout: 30000 });
    const err = await page.evaluate("window.__errorPaginado || ''");
    if (err) throw new Error("Paginación del PDF: " + err);
    const buf = await page.pdf({ format: "A4", printBackground: true, preferCSSPageSize: true, margin: { top: 0, right: 0, bottom: 0, left: 0 } });
    // puppeteer 10 cierra el stream del PDF (IO.close) después de devolverlo: si el
    // navegador se cierra antes, esa promesa queda rechazada sin capturar
    await new Promise((r) => setTimeout(r, 150));
    return Buffer.from(buf);
  } finally {
    await browser.close();
  }
}

// ── Nombre de descarga legible ──────────────────────────────────
// FA-00005-00000001_YAN-XIUYU.pdf · FB-… · FM-… · NC-A-…
function nombreArchivoComprobante(comprobante, nombreCliente) {
  const s = String(comprobante || "").toUpperCase();
  const m = s.match(/^(?:(FA|NC)-)?([ABCM])-(\d{5})-(\d{8})$/) || s.match(/^(NC)-?([ABCM])?-?(\d{5})-(\d{8})$/);
  let base;
  if (m) {
    const esNC = m[1] === "NC";
    // una NC vieja sin letra en el rótulo queda "NC-pv-nro" (no se inventa la letra)
    base = esNC ? `NC-${m[2] ? m[2] + "-" : ""}${m[3]}-${m[4]}` : `F${m[2]}-${m[3]}-${m[4]}`;
  } else {
    base = s.replace(/[^A-Z0-9-]/g, "") || "COMPROBANTE";
  }
  const cli = String(nombreCliente || "")
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .toUpperCase().replace(/[^A-Z0-9]+/g, "-").replace(/^-+|-+$/g, "")
    .slice(0, 40).replace(/-+$/, "");
  return `${base}${cli ? "_" + cli : ""}.pdf`;
}

module.exports = { renderComprobanteHtml, generarPdf, nombreArchivoComprobante, fechaAR };
