const express = require("express");
const fs = require("fs");
const Afip = require("afip.js");

const app = express();
app.use(express.json({ limit: "2mb" }));

// 🚀 Configuración AFIP (Producción real, usando variables de entorno)
const afip = new Afip({
  CUIT: 23332382314,      // tu CUIT sin guiones
  production: true,       // true = producción
  cert: process.env.AFIP_CERT,
  key: process.env.AFIP_KEY,
});

// 🌐 Ruta de prueba (health check)
app.get("/", (req, res) => res.send("✅ Worker conectado con AFIP y listo"));

// 🧪 Ruta de prueba de conexión con AFIP
app.get("/test-afip", async (req, res) => {
  try {
    const lastVoucher = await afip.ElectronicBilling.getLastVoucher(5, 51);
    res.json({ ok: true, lastVoucher });
  } catch (e) {
    console.error("❌ Error test-afip:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// 📑 Endpoint para emitir Factura M con IVA 21%
app.post("/facturar", async (req, res) => {
  try {
    const data = req.body;

    // 🔹 Totales
    const impTotal = Number(data.ImpTotal || 1210.00);
    const impNeto  = +(impTotal / 1.21).toFixed(2);
    const impIVA   = +(impTotal - impNeto).toFixed(2);

    // 🔹 Número de comprobante
    const lastVoucher = await afip.ElectronicBilling.getLastVoucher(5, 51);
    const proxNro = lastVoucher + 1;

    // 🔹 Datos de la factura
    const factura = {
      CantReg: 1,
      PtoVta: 5,
      CbteTipo: 51,   // Factura M
      Concepto: 1,    // Productos
      DocTipo: Number(data.DocTipo || 80),  // 80 = CUIT
      DocNro: Number(data.DocNro || "20111111112"),
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
