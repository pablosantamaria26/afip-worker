const express = require("express");
const fs = require("fs");
const Afip = require("afip.js");

const app = express();
app.use(express.json({ limit: "2mb" }));

// 🔑 Certificados cargados desde Render
const key  = fs.readFileSync("/etc/secrets/PabloSantamaria.key", "utf8");
const cert = fs.readFileSync("/etc/secrets/certificado.crt", "utf8");

// 🚀 Configuración AFIP
const afip = new Afip({
  CUIT: 23332382314,   // tu CUIT real
  production: false,   // false = homologación, true = producción
  cert,
  key,
});

// 🌐 Ruta de prueba
app.get("/", (req, res) => res.send("✅ Worker conectado con AFIP y listo"));

// 📑 Endpoint para emitir Factura M
app.post("/facturar", async (req, res) => {
  try {
    const data = req.body;

    // 🔹 Total de la factura (con IVA incluido)
    const impTotal = Number(data.ImpTotal || 1000.00);
    const impNeto  = +(impTotal / 1.21).toFixed(2);
    const impIVA   = +(impTotal - impNeto).toFixed(2);

    // 🔹 Próximo número de comprobante
    const lastVoucher = await afip.ElectronicBilling.getLastVoucher(1, 51);
    const proxNro = lastVoucher + 1;

    // 🔹 Datos de la factura
    const factura = {
      CantReg: 1,
      PtoVta: 1,
      CbteTipo: 51,       // Factura M
      Concepto: 1,        // Productos
      DocTipo: 80,        // CUIT
      DocNro: Number(data.DocNro || "20111111112"),

      // 👇 Campo obligatorio según RG 5616
      CondicionIvaReceptor: Number(data.IvaCond || 1), // 1 = Responsable Inscripto

      CbteDesde: proxNro,
      CbteHasta: proxNro,
      CbteFch: parseInt(new Date().toISOString().slice(0,10).replace(/-/g,"")),

      ImpNeto: impNeto,
      ImpIVA: impIVA,
      ImpTotal: impTotal,

      Iva: [
        {
          Id: 5,           // 21% en AFIP
          BaseImp: impNeto,
          Importe: impIVA
        }
      ],

      MonId: "PES",
      MonCotiz: 1,
    };

    // 🔹 Emitimos
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
