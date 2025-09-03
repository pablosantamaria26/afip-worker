const express = require("express");
const fs = require("fs");
const Afip = require("afip.js");

const app = express();
app.use(express.json({ limit: "2mb" }));

// 🔑 Certificados carregados desde Render
const key = fs.readFileSync("/etc/secrets/PabloSantamaria.key", "utf8");
const cert = fs.readFileSync("/etc/secrets/certificado.crt", "utf8");

// 🚀 Configuração AFIP (homologação por agora)
const afip = new Afip({
  CUIT: 23332382314, // 👈 tu CUIT real
  production: false, // false = homologación, true = producción real
  cert,
  key,
});

// 🌐 Ruta de prueba
app.get("/", (req, res) => res.send("✅ Worker conectado con AFIP y listo"));

// 📑 Endpoint para emitir Factura M con IVA 21%
app.post("/facturar", async (req, res) => {
  try {
    const data = req.body;

    // 🔹 Totales
    const impTotal = Number(data.ImpTotal || 1210.00);
    const impNeto = +(impTotal / 1.21).toFixed(2);
    const impIVA = +(impTotal - impNeto).toFixed(2);

    // 🔹 Número de comprobante
    const lastVoucher = await afip.ElectronicBilling.getLastVoucher(1, 51);
    const proxNro = lastVoucher + 1;

    // 🔹 Datos de la factura
    const factura = {
      CantReg: 1,
      PtoVta: 1,
      CbteTipo: 51,
      Concepto: 1,
      DocTipo: Number(data.DocTipo || 80),
      DocNro: Number(data.DocNro || "20111111112"),
      CondicionIVAReceptorId: Number(data.IdIVAReceptor || 5), // ✅ CAMBIO CRÍTICO: Nome de campo atualizado
      CbteDesde: proxNro,
      CbteHasta: proxNro,
      CbteFch: parseInt(new Date().toISOString().slice(0, 10).replace(/-/g, "")),
      ImpNeto: impNeto,
      ImpIVA: impIVA,
      ImpTotal: impTotal,
      Iva: [{
        Id: 5,
        BaseImp: impNeto,
        Importe: impIVA,
      }],
      MonId: "PES",
      MonCotiz: 1,
    };

    // 🔹 Emitir comprobante
    const result = await afip.ElectronicBilling.createVoucher(factura);
    res.json({ ok: true, result });
  } catch (e) {
    console.error("❌ Error facturando:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// 🚪 Servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("🚀 Worker AFIP escuchando en puerto", PORT));
