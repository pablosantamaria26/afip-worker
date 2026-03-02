<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Mercado Limpio - Motor de Facturación</title>
  <style>
    :root {
      --bg-dark: #0f172a;
      --card-bg: #ffffff;
      --primary: #1e293b;
      --accent: #3b82f6;
      --text: #334155;
      --border: #e2e8f0;
      --success: #10b981;
      --warn: #f59e0b;
    }

    body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg-dark); color: var(--text); display: flex; justify-content: center; padding: 40px 20px; margin: 0; min-height: 100vh; }

    .container { width: 100%; max-width: 1400px; display: grid; grid-template-columns: 1fr 1fr; gap: 30px; }
    @media (max-width: 900px) { .container { grid-template-columns: 1fr; } body { padding: 20px 10px; } }

    .card { background: var(--card-bg); padding: 35px; border-radius: 16px; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.3); display: flex; flex-direction: column; }

    .header h2 { margin: 0 0 5px 0; color: var(--primary); font-size: 26px; font-weight: 900; }
    .header p { margin: 0 0 20px 0; font-size: 14px; color: #64748b; }

    .reader-box { background: #f8fafc; border: 2px dashed #94a3b8; padding: 40px 20px; border-radius: 12px; text-align: center; cursor: pointer; transition: all 0.2s ease; margin-bottom: 25px; }
    .reader-box:hover, .reader-box.drag { background: #eff6ff; border-color: var(--accent); }
    .reader-box label { display: block; font-size: 18px; font-weight: 800; color: var(--primary); pointer-events: none; }
    .reader-box .hint { font-size: 14px; color: #64748b; margin-top: 8px; pointer-events: none; }
    .file-list { margin-top: 15px; font-size: 13px; color: var(--accent); font-weight: bold; }

    .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 15px; }
    input, select, textarea {
      width: 100%;
      padding: 16px;
      border: 1px solid var(--border);
      border-radius: 8px;
      font-size: 15px;
      background: #f8fafc;
      color: var(--primary);
      font-weight: 600;
      transition: 0.2s;
      box-sizing: border-box;
    }
    input:focus, select:focus, textarea:focus { outline: none; border-color: var(--accent); background: #fff; }
    textarea { height: 80px; resize: none; margin-bottom: 15px; }

    .btn-group { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 15px; }
    .btn-tag { padding: 12px; font-size: 13px; background: #f1f5f9; border: 1px solid var(--border); border-radius: 8px; cursor: pointer; font-weight: bold; transition: 0.2s; }
    .btn-tag:hover { background: #e2e8f0; }

    button.main-btn { width: 100%; padding: 20px; border: 0; border-radius: 12px; cursor: pointer; font-size: 18px; font-weight: 900; background: var(--accent); color: #fff; transition: 0.2s; margin-top: 15px; }
    button.main-btn:disabled { opacity: 0.5; cursor: not-allowed; }
    button.main-btn:not(:disabled):hover { background: #2563eb; transform: translateY(-2px); }

    .spinner-container { display: none; text-align: center; padding: 20px; margin-bottom: 20px; }
    .spinner { width: 50px; height: 50px; border: 5px solid #e2e8f0; border-top: 5px solid var(--accent); border-radius: 50%; animation: spin 1s linear infinite; margin: 0 auto 15px auto; }

    .preview-spinner-box { flex: 1; display: none; flex-direction: column; align-items: center; justify-content: center; background: #f8fafc; border-radius: 12px; border: 1px solid var(--border); height: 100%; min-height: 700px; }
    .preview-spinner-box .spinner { width: 60px; height: 60px; border: 6px solid #e2e8f0; border-top: 6px solid var(--accent); }
    .preview-spinner-text { font-size: 16px; font-weight: 800; color: var(--accent); margin-top: 20px; animation: pulse 1.5s infinite; }

    @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }

    #mensaje {
      margin-top: 20px;
      font-size: 15px;
      font-weight: bold;
      padding: 18px;
      border-radius: 8px;
      display: none;
      text-align: center;
      white-space: pre-line;
    }

    #wa-container { margin-top: 20px; display: none; }
    .btn-wa { display: block; text-align: center; background: #25D366; color: white; padding: 18px; border-radius: 12px; text-decoration: none; font-weight: 900; font-size: 16px; }

    /* ✅ PREVIEW CONTROL BAR */
    .preview-topbar {
      display: none;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      background: #ffffff;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 12px;
      margin-bottom: 12px;
      box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08);
    }
    .preview-topbar .left, .preview-topbar .right { display: flex; align-items: center; gap: 10px; }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: #f8fafc;
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 13px;
      font-weight: 900;
      color: var(--primary);
      white-space: nowrap;
    }
    .pill small { font-weight: 800; color: #64748b; }
    .btn-mini {
      border: 1px solid var(--border);
      background: #ffffff;
      padding: 10px 12px;
      border-radius: 10px;
      font-weight: 900;
      cursor: pointer;
      transition: 0.15s;
      color: var(--primary);
      font-size: 13px;
      user-select: none;
    }
    .btn-mini:hover { transform: translateY(-1px); border-color: #cbd5e1; }
    .btn-mini:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
    .btn-mini.primary { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
    .btn-mini.warn { background: #fffbeb; border-color: #fde68a; color: #a16207; }
    .segmented {
      display: inline-flex;
      border: 1px solid var(--border);
      border-radius: 12px;
      overflow: hidden;
      background: #fff;
    }
    .segmented button {
      padding: 10px 12px;
      border: 0;
      background: transparent;
      font-weight: 900;
      cursor: pointer;
      font-size: 13px;
      color: #475569;
    }
    .segmented button.active {
      background: #1e293b;
      color: #ffffff;
    }
    .preview-container { flex: 1; display: flex; flex-direction: column; height: 100%; border: 1px solid var(--border); border-radius: 12px; overflow: hidden; background: #f8fafc; }
    .preview-frame { width: 100%; height: 100%; min-height: 700px; border: none; background: transparent; }
    .preview-placeholder { flex: 1; display: flex; align-items: center; justify-content: center; color: #94a3b8; font-weight: bold; font-size: 15px; text-align: center; padding: 30px; }

    /* ALL MODE container (render multiple previews stacked) */
    .allWrap {
      display: none;
      padding: 10px;
      overflow: auto;
      height: 100%;
      min-height: 700px;
      background: #f8fafc;
    }
    .allWrap .sheet {
      background: #ffffff;
      border: 1px solid var(--border);
      border-radius: 12px;
      margin: 10px 0;
      overflow: hidden;
      box-shadow: 0 10px 18px rgba(15, 23, 42, 0.08);
    }
    .allWrap .sheet .sheetHdr {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 12px 14px;
      background: #0f172a;
      color: #fff;
      font-weight: 900;
      font-size: 13px;
    }
    .allWrap .sheet .sheetHdr span { opacity: 0.9; font-weight: 800; }
    .allWrap .sheet .sheetBody { padding: 0; }
    .allWrap .sheet iframe { width: 100%; height: 850px; border: 0; }
  </style>
</head>

<body>
  <div class="container">
    <!-- LEFT -->
    <div class="card">
      <div class="header">
        <h2>MERCADO LIMPIO</h2>
        <p>Facturación Electrónica AFIP/ARCA</p>
      </div>

      <div class="reader-box" id="drop-zone" onclick="document.getElementById('fileRemito').click()">
        <label>📄 CARGAR PDF</label>
        <div class="hint">Arrastrá uno o múltiples remitos aquí</div>
        <input type="file" id="fileRemito" accept="application/pdf" multiple style="display:none" />
        <div id="file-list" class="file-list"></div>
      </div>

      <div id="spinner-box" class="spinner-container">
        <div class="spinner"></div>
        <div id="spinner-text" class="spinner-text">Procesando...</div>
      </div>

      <div class="grid-2">
        <input type="text" id="cuit" placeholder="CUIT Cliente (11 números)" onblur="triggerPreviewNow()" oninput="triggerPreview()" />
        <input type="text" id="monto" placeholder="Total a facturar ($)" onblur="triggerPreviewNow()" oninput="triggerPreview()" />
      </div>

      <select id="condicionVenta" onchange="triggerPreviewNow()">
        <option value="Transferencia Bancaria">Transferencia Bancaria</option>
        <option value="Efectivo">Efectivo</option>
        <option value="Cheque">Cheque</option>
      </select>

      <input type="email" id="email" placeholder="Email (Por defecto: distribuidora...)" style="margin-top:15px; margin-bottom:15px;" />

      <div class="btn-group">
        <button class="btn-tag" type="button" onclick="presetDetalle('Artículos Make')">🏷️ Make</button>
        <button class="btn-tag" type="button" onclick="presetDetalle('Artículos Romil')">🏷️ Romil</button>
        <button class="btn-tag" type="button" onclick="presetDetalle('Artículos de limpieza varios')">🏷️ Varios</button>
      </div>

      <textarea id="detalle" placeholder="Detalle manual (Solo si no subís PDF)" onblur="triggerPreviewNow()" oninput="triggerPreview()"></textarea>

      <div id="mensaje"></div>
      <button id="btn" class="main-btn" onclick="emitir()">EMITIR FACTURA ELECTRÓNICA</button>
      <div id="wa-container"><a id="wa-link" href="#" target="_blank" class="btn-wa">📱 ENVIAR POR WHATSAPP</a></div>
    </div>

    <!-- RIGHT -->
    <div class="card" style="padding: 0; background: transparent; box-shadow: none;">

      <!-- ✅ topbar controls -->
      <div id="previewTopbar" class="preview-topbar">
        <div class="left">
          <div class="pill" id="pillInfo">Vista previa</div>

          <button id="btnPrev" class="btn-mini" onclick="prevParte()" disabled>⬅️</button>
          <button id="btnNext" class="btn-mini" onclick="nextParte()" disabled>➡️</button>

          <div class="segmented">
            <button id="modeOne" class="active" onclick="setPreviewMode('ONE')">Parte</button>
            <button id="modeAll" onclick="setPreviewMode('ALL')">Todas</button>
          </div>

          <button id="btnRefresh" class="btn-mini primary" onclick="triggerPreviewNow()">↻ Refrescar</button>
        </div>

        <div class="right">
          <div class="pill"><small>Parte</small> <span id="pillParte">1</span>/<span id="pillTotalPartes">1</span></div>
          <button id="btnScrollTop" class="btn-mini warn" onclick="scrollToTopAll()" style="display:none;">⬆️ Arriba</button>
        </div>
      </div>

      <div id="previewSpinner" class="preview-spinner-box">
        <div class="spinner"></div>
        <div class="preview-spinner-text">Consultando padrón AFIP y generando factura...</div>
      </div>

      <div class="preview-container" id="previewContainer">
        <div class="preview-placeholder" id="previewPlaceholder">
          Cargá un PDF o llená los datos manualmente.<br><br>
          Al escribir los 11 números del CUIT, se buscará el nombre del cliente directamente en AFIP.
        </div>

        <!-- ✅ ONE mode -->
        <iframe id="previewFrame" class="preview-frame" style="display:none;"></iframe>

        <!-- ✅ ALL mode -->
        <div id="allWrap" class="allWrap"></div>
      </div>

    </div>
  </div>

  <script>
    const BASE = "http://localhost:3000";

    let itemsGlobal = [];
    let previewTimer = null;

    // datos del remito
    let domicilioRemitoGlobal = "";
    let subtotalBrutoGlobal = 0;
    let descuentoPctGlobal = 0;
    let descuentoImporteGlobal = 0;
    let totalFinalGlobal = 0;

    // ✅ control de partes preview
    let previewMode = "ONE";      // ONE | ALL
    let parteActual = 1;
    let totalPartes = 1;

    const fileInput = document.getElementById("fileRemito");
    const dropZone = document.getElementById("drop-zone");
    const spinnerBox = document.getElementById("spinner-box");
    const spinnerText = document.getElementById("spinner-text");
    const btn = document.getElementById("btn");

    const previewContainer = document.getElementById("previewContainer");
    const previewFrame = document.getElementById("previewFrame");
    const previewPlaceholder = document.getElementById("previewPlaceholder");
    const previewSpinner = document.getElementById("previewSpinner");

    const previewTopbar = document.getElementById("previewTopbar");
    const pillInfo = document.getElementById("pillInfo");
    const pillParte = document.getElementById("pillParte");
    const pillTotalPartes = document.getElementById("pillTotalPartes");
    const btnPrevEl = document.getElementById("btnPrev");
    const btnNextEl = document.getElementById("btnNext");
    const btnScrollTop = document.getElementById("btnScrollTop");

    const modeOneBtn = document.getElementById("modeOne");
    const modeAllBtn = document.getElementById("modeAll");

    const allWrap = document.getElementById("allWrap");

    function setMsg(t, c, bg) {
      const m = document.getElementById("mensaje");
      if (!t) { m.style.display = "none"; return; }
      m.textContent = t;
      m.style.color = c;
      m.style.background = bg;
      m.style.display = "block";
    }

    function presetDetalle(t) {
      document.getElementById("detalle").value = t;
      triggerPreviewNow();
    }

    function round2(n) { return Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100; }

    function formatMoneyAR(n) {
      try {
        return new Intl.NumberFormat("es-AR", { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Number(n || 0));
      } catch {
        return String(n);
      }
    }

    function parseMontoInput() {
      const monto = document.getElementById("monto").value.trim();
      const m = Number(String(monto).replace(/\./g, "").replace(",", "."));
      return (Number.isFinite(m) && m > 0) ? round2(m) : 0;
    }

    function spinnerState(active, text) {
      spinnerBox.style.display = active ? "block" : "none";
      dropZone.style.display = active ? "none" : "block";
      if (text) spinnerText.textContent = text;
    }

    function startSpinnerTexts() {
      const texts = ["Subiendo archivos...", "Analizando la tabla...", "Acomodando decimales...", "Calculando totales..."];
      let i = 0; spinnerText.textContent = texts[0];
      return setInterval(() => { i++; if (i < texts.length) spinnerText.textContent = texts[i]; }, 1500);
    }

    function triggerPreview() {
      clearTimeout(previewTimer);
      previewTimer = setTimeout(generarVistaPrevia, 1200);
    }

    function triggerPreviewNow() {
      clearTimeout(previewTimer);
      generarVistaPrevia();
    }

    function computeTotalPartesFromItemsLen() {
      const n = itemsGlobal.length || 0;
      totalPartes = Math.max(1, Math.ceil(n / 25)); // coincide con backend (25)
      if (parteActual > totalPartes) parteActual = totalPartes;
      if (parteActual < 1) parteActual = 1;

      pillParte.textContent = String(parteActual);
      pillTotalPartes.textContent = String(totalPartes);

      btnPrevEl.disabled = parteActual <= 1 || previewMode !== "ONE";
      btnNextEl.disabled = parteActual >= totalPartes || previewMode !== "ONE";
    }

    function setPreviewMode(mode) {
      previewMode = mode;

      modeOneBtn.classList.toggle("active", mode === "ONE");
      modeAllBtn.classList.toggle("active", mode === "ALL");

      btnPrevEl.disabled = (mode !== "ONE") || parteActual <= 1;
      btnNextEl.disabled = (mode !== "ONE") || parteActual >= totalPartes;

      // show/hide frames
      if (mode === "ONE") {
        allWrap.style.display = "none";
        btnScrollTop.style.display = "none";
        previewFrame.style.display = "block";
      } else {
        previewFrame.style.display = "none";
        allWrap.style.display = "block";
        btnScrollTop.style.display = "inline-flex";
      }

      triggerPreviewNow();
    }

    function prevParte() {
      if (parteActual > 1) { parteActual--; computeTotalPartesFromItemsLen(); triggerPreviewNow(); }
    }
    function nextParte() {
      if (parteActual < totalPartes) { parteActual++; computeTotalPartesFromItemsLen(); triggerPreviewNow(); }
    }

    function scrollToTopAll() {
      try { allWrap.scrollTo({ top: 0, behavior: "smooth" }); } catch { allWrap.scrollTop = 0; }
    }

    function buildPayloadForPreview(itemsToSend, totalToSend) {
      return {
        cuitCliente: document.getElementById("cuit").value.trim(),
        domicilioRemito: domicilioRemitoGlobal,
        condicionVenta: document.getElementById("condicionVenta").value,
        items: itemsToSend,
        subtotalBruto: subtotalBrutoGlobal || 0,
        descuentoPct: descuentoPctGlobal || 0,
        descuentoImporte: descuentoImporteGlobal || 0,
        total: totalToSend,

        // ✅ nuevo: modo y parte (backend lo toma si lo implementás)
        previewParte: (previewMode === "ALL") ? "ALL" : parteActual
      };
    }

    async function generarVistaPrevia() {
      const cuit = document.getElementById("cuit").value.trim();
      const detalleManual = document.getElementById("detalle").value.trim();

      let itemsToPreview = itemsGlobal.map(it => {
        const descripcion = String(it.descripcion || "").trim();
        const cantidad = Number(it.cantidad || 0);
        const precioConIva = round2(Number(it.precioConIva || 0));
        const subtotalConIva = round2(Number(it.subtotalConIva || (cantidad * precioConIva) || 0));
        return { descripcion, cantidad, precioConIva, subtotalConIva };
      }).filter(it => it.cantidad > 0 && it.precioConIva > 0 && it.subtotalConIva > 0);

      if (itemsToPreview.length === 0) {
        const m = parseMontoInput();
        if (m > 0) {
          itemsToPreview = [{
            descripcion: detalleManual || "Artículos varios",
            cantidad: 1,
            precioConIva: round2(m),
            subtotalConIva: round2(m)
          }];
        }
      }

      if (itemsToPreview.length === 0 && (!cuit || cuit.length < 11)) return;

      // topbar visible cuando hay algo que previsualizar
      previewTopbar.style.display = "flex";

      computeTotalPartesFromItemsLen();

      // spinner
      previewContainer.style.display = "none";
      previewSpinner.style.display = "flex";

      try {
        const totalComputed = round2(itemsToPreview.reduce((a, x) => a + Number(x.subtotalConIva || 0), 0));
        const totalToSend = totalFinalGlobal > 0 ? totalFinalGlobal : totalComputed;

        const payload = buildPayloadForPreview(itemsToPreview, totalToSend);

        const r = await fetch(`${BASE}/debug/preview`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });

        if (r.ok) {
          const htmlStr = await r.text();

          previewPlaceholder.style.display = "none";

          if (previewMode === "ONE") {
            allWrap.style.display = "none";
            previewFrame.style.display = "block";
            const doc = previewFrame.contentWindow.document;
            doc.open(); doc.write(htmlStr); doc.close();
          } else {
            previewFrame.style.display = "none";
            allWrap.style.display = "block";

            // Si backend soporta ALL: devuelve un HTML “contenedor”
            // Si no, devolvemos solo 1 y lo repetimos por partes desde el frontend.
            if (payload.previewParte === "ALL" && htmlStr.includes("<!--ALL_PREVIEW_CONTAINER-->")) {
              allWrap.innerHTML = htmlStr;
            } else {
              // fallback: generamos todas las partes desde frontend (llamadas en serie)
              await renderAllPartsFromClient(itemsToPreview, totalToSend);
            }
          }
        }
      } catch(e) {
        console.log("Error vista previa:", e);
      }

      // hide spinner
      previewSpinner.style.display = "none";
      previewContainer.style.display = "flex";

      // pill info
      const hasDesc = (descuentoImporteGlobal > 0 && subtotalBrutoGlobal > 0);
      pillInfo.textContent = hasDesc
        ? `Vista previa • Dto ${formatMoneyAR(descuentoPctGlobal)}% • Total $ ${formatMoneyAR(totalFinalGlobal)}`
        : `Vista previa • Total $ ${formatMoneyAR(totalFinalGlobal || 0)}`;
      pillParte.textContent = String(parteActual);
      pillTotalPartes.textContent = String(totalPartes);
    }

    async function renderAllPartsFromClient(itemsToPreview, totalToSend) {
      allWrap.innerHTML = "";
      const n = Math.max(1, totalPartes);

      for (let p = 1; p <= n; p++) {
        const payload = {
          ...buildPayloadForPreview(itemsToPreview, totalToSend),
          previewParte: p
        };

        const r = await fetch(`${BASE}/debug/preview`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });

        if (!r.ok) continue;
        const htmlStr = await r.text();

        const sheet = document.createElement("div");
        sheet.className = "sheet";
        sheet.innerHTML = `
          <div class="sheetHdr">Parte ${p} de ${n} <span>Chequeo completo</span></div>
          <div class="sheetBody"><iframe></iframe></div>
        `;
        allWrap.appendChild(sheet);

        const ifr = sheet.querySelector("iframe");
        const doc = ifr.contentWindow.document;
        doc.open(); doc.write(htmlStr); doc.close();
      }
    }

    async function leerPDF() {
      if (!fileInput.files.length) return;
      document.getElementById("file-list").innerHTML = `<b>${fileInput.files.length} archivo(s) listo(s)</b>`;
      document.getElementById("file-list").style.display = "block";

      setMsg("");
      document.getElementById("wa-container").style.display = "none";

      const animId = startSpinnerTexts();
      spinnerState(true);

      const formData = new FormData();
      for (let i = 0; i < fileInput.files.length; i++) formData.append("remito", fileInput.files[i]);

      try {
        const r = await fetch(`${BASE}/leer-remito`, { method: "POST", body: formData });
        const res = await r.json();

        clearInterval(animId);
        spinnerState(false);

        if (!r.ok) {
          const detail = res?.detail ? String(res.detail) : "";
          return setMsg("Error al leer el PDF.\n" + detail, "red", "#fee2e2");
        }

        domicilioRemitoGlobal = res.domicilioRemito || "";
        subtotalBrutoGlobal = Number(res.subtotalBruto || 0);
        descuentoPctGlobal = Number(res.descuentoPct || 0);
        descuentoImporteGlobal = Number(res.descuentoImporte || 0);
        totalFinalGlobal = Number(res.total || 0);

        document.getElementById("cuit").value = res.cuit || "";
        document.getElementById("monto").value = (res.total != null ? String(res.total) : "").replace(".", ",");
        itemsGlobal = Array.isArray(res.items) ? res.items : [];

        // reset preview pagination state
        parteActual = 1;
        computeTotalPartesFromItemsLen();
        previewTopbar.style.display = "flex";

        let msg = `✅ Extracción perfecta: ${itemsGlobal.length} ítems.`;
        if (domicilioRemitoGlobal) msg += `\n📍 ${domicilioRemitoGlobal}`;
        if (subtotalBrutoGlobal > 0 && descuentoImporteGlobal > 0 && totalFinalGlobal > 0) {
          msg += `\nSubtotal: $ ${formatMoneyAR(subtotalBrutoGlobal)} | Dto: ${formatMoneyAR(descuentoPctGlobal)}% (-$ ${formatMoneyAR(descuentoImporteGlobal)})`;
          msg += `\nTotal (post-desc): $ ${formatMoneyAR(totalFinalGlobal)}`;
        } else if (totalFinalGlobal > 0) {
          msg += `\nTotal: $ ${formatMoneyAR(totalFinalGlobal)}`;
        }

        setMsg(msg, "#10b981", "#dcfce7");
        btn.disabled = false;

        triggerPreviewNow();

      } catch (e) {
        clearInterval(animId);
        spinnerState(false);
        setMsg("Fallo de conexión.", "red", "#fee2e2");
      }
    }

    async function emitir() {
      const cuit = document.getElementById("cuit").value.trim();
      if (!cuit || cuit.length !== 11) return setMsg("CUIT inválido (deben ser 11 números).", "red", "#fee2e2");

      let items = itemsGlobal.map(it => {
        const descripcion = String(it.descripcion || "").trim();
        const cantidad = Number(it.cantidad || 0);
        const precioConIva = round2(Number(it.precioConIva || 0));
        const subtotalConIva = round2(Number(it.subtotalConIva || (cantidad * precioConIva) || 0));
        return { descripcion, cantidad, precioConIva, subtotalConIva };
      }).filter(it => it.cantidad > 0 && it.precioConIva > 0 && it.subtotalConIva > 0);

      if (items.length === 0) {
        const m = parseMontoInput();
        if (!m || m <= 0) return setMsg("Falta monto total o ítems.", "red", "#fee2e2");
        items = [{
          descripcion: document.getElementById("detalle").value || "Artículos Varios",
          cantidad: 1,
          precioConIva: round2(m),
          subtotalConIva: round2(m)
        }];

        domicilioRemitoGlobal = "";
        subtotalBrutoGlobal = 0;
        descuentoPctGlobal = 0;
        descuentoImporteGlobal = 0;
        totalFinalGlobal = round2(m);
        itemsGlobal = items;
      }

      btn.disabled = true;
      setMsg("🚀 Emitiendo y Autorizando en ARCA...", "white", "#3b82f6");

      try {
        const totalComputed = round2(items.reduce((a, x) => a + Number(x.subtotalConIva || 0), 0));
        const totalToSend = totalFinalGlobal > 0 ? totalFinalGlobal : totalComputed;

        const payload = {
          cuitCliente: cuit,
          domicilioRemito: domicilioRemitoGlobal,
          condicionVenta: document.getElementById("condicionVenta").value,
          items,
          subtotalBruto: subtotalBrutoGlobal || 0,
          descuentoPct: descuentoPctGlobal || 0,
          descuentoImporte: descuentoImporteGlobal || 0,
          total: totalToSend
        };

        const email = document.getElementById("email").value.trim();
        if (email) payload.emailCliente = email;

        const r = await fetch(`${BASE}/facturar`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });
        const j = await r.json();

        if (!r.ok) throw new Error(j.message || "Error al facturar.");

        let msg = `✅ ${j.mensaje || "Factura autorizada con éxito."}`;
        if (Array.isArray(j.facturas) && j.facturas.length) {
          const pv = String(j.puntoDeVenta || "").padStart(5, "0");
          msg += `\nPV: ${pv}`;
          j.facturas.forEach((f, idx) => {
            const nro = String(f.nroFactura || "").padStart(8, "0");
            msg += `\nParte ${idx + 1}: ${nro} | CAE ${f.cae} | $ ${formatMoneyAR(f.total)}`;
          });
        }

        setMsg(msg, "#10b981", "#dcfce7");

        if (j.waLink) {
          document.getElementById("wa-link").href = j.waLink;
          document.getElementById("wa-container").style.display = "block";
        }

        // refresco preview
        triggerPreviewNow();
        btn.disabled = false;

      } catch (e) {
        setMsg("❌ " + (e.message || "Error"), "red", "#fee2e2");
        btn.disabled = false;
      }
    }

    fileInput.addEventListener("change", leerPDF);
    dropZone.addEventListener("dragover", (e) => { e.preventDefault(); dropZone.classList.add("drag"); });
    dropZone.addEventListener("dragleave", () => dropZone.classList.remove("drag"));
    dropZone.addEventListener("drop", (e) => {
      e.preventDefault();
      dropZone.classList.remove("drag");
      if (e.dataTransfer.files.length > 0) {
        fileInput.files = e.dataTransfer.files;
        leerPDF();
      }
    });

  </script>
</body>
</html>
