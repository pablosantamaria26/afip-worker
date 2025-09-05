const express = require("express");
const fs = require("fs");
const Afip = require("afip.js");

const app = express();
app.use(express.json({ limit: "2mb" }));

// 🔑 Certificados cargados desde Render
const key = fs.readFileSync("/etc/secrets/PabloSantamaria.key", "utf8");
const cert = fs.readFileSync("/etc/secrets/certificado.crt", "utf8");

// 🚀 Configuración AFIP (por ahora homologación)
const afip = new Afip({
  CUIT: 23332382314,   // 👈 tu CUIT real
  production: true,   // false = homologación, true = producción real
  cert,
  key,
});

// 🌐 Ruta de prueba (health check)
app.get("/", (req, res) => res.send("✅ Worker conectado con AFIP y listo"));

// 📑 Endpoint para emitir Factura M con IVA 21%
app.post("/facturar", async (req, res) => {
  try {
    const data = req.body;

    // 🔹 Totales
    const impTotal = Number(data.ImpTotal || 1210.00);
    const impNeto  = +(impTotal / 1.21).toFixed(2);
    const impIVA   = +(impTotal - impNeto).toFixed(2);

    // 🔹 Número de comprobante
    const lastVoucher = await afip.ElectronicBilling.getLastVoucher(1, 51);
    const proxNro = lastVoucher + 1;

    // 🔹 Datos de la factura
    const factura = {
      CantReg: 1,
      PtoVta: 5,
      CbteTipo: 51,   // Factura M
      Concepto: 1,    // Productos
      DocTipo: Number(data.DocTipo || 80),  // 80 = CUIT
      DocNro: Number(data.DocNro || "20111111112"),

      // ⚠️ Campo obligatorio según RG 5616 (lo dejamos EXACTO como vos lo lograste)
      CondicionIVAReceptorId: Number(data.IdIVAReceptor || 5), 

      CbteDesde: proxNro,
      CbteHasta: proxNro,
      CbteFch: parseInt(new Date().toISOString().slice(0, 10).replace(/-/g, "")),

      ImpNeto: impNeto,
      ImpIVA: impIVA,
      ImpTotal: impTotal,

      Iva: [
        {
          Id: 5,            // 21% en AFIP
          BaseImp: impNeto,
          Importe: impIVA,
        }
      ],

      MonId: "PES",
      MonCotiz: 1,
    };

    console.log("📤 Enviando a AFIP:", JSON.stringify(factura, null, 2));

    // 🔹 Emitir comprobante
    const result = await afip.ElectronicBilling.createVoucher(factura);
    console.log("✅ Respuesta AFIP:", result);

    res.json({ ok: true, result: { ...result, CbteDesde: factura.CbteDesde } });
  } catch (e) {
    console.error("❌ Error facturando:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// 🚪 Servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("🚀 Worker AFIP escuchando en puerto", PORT));
