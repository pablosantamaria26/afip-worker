const express = require("express");
const fs = require("fs");
const Afip = require("afip.js");

const app = express();
app.use(express.json({ limit: "2mb" }));

// 🔑 Certificados cargados desde Render
const cert = fs.readFileSync("/etc/secrets/Mercado Limpio_55a99c47e38f54f3.crt");
const key = fs.readFileSync("/etc/secrets/PabloSantamaria.key");

// 🚀 Configuración AFIP
const afip = new Afip({
  CUIT: 23332382314,   // 👈 tu CUIT real (11 dígitos, sin guiones)
  production: true,    // true = producción, false = homologación
  cert,
  key,
});

// 🌐 Ruta de prueba (health check)
app.get("/", (req, res) => res.send("✅ Worker conectado con AFIP y listo"));

// 📑 Endpoint para emitir Factura M con IVA 21%
app.post("/facturar", async (req, res) => {
  try {
    const data = req.body;

    // 🔹 Tomamos total de la factura (con IVA incluido)
    const impTotal = Number(data.ImpTotal || 1000.00);
    const impNeto = +(impTotal / 1.21).toFixed(2);
    const impIVA = +(impTotal - impNeto).toFixed(2);

    // 🔹 Buscamos el próximo número de comprobante disponible
    const lastVoucher = await afip.ElectronicBilling.getLastVoucher(1, 51); // PtoVta=1, Tipo=51 Factura M
    const proxNro = lastVoucher + 1;

    const factura = {
      CantReg: 1,                // Siempre 1 comprobante por vez
      PtoVta: 1,                 // Punto de venta
      CbteTipo: 51,              // 51 = Factura M
      Concepto: 1,               // 1 = Productos
      DocTipo: 80,               // 80 = CUIT (cliente)
      DocNro: Number(data.DocNro || "20111111112"), // CUIT del cliente
      CbteDesde: proxNro,
      CbteHasta: proxNro,
      CbteFch: parseInt(new Date().toISOString().slice(0,10).replace(/-/g,"")), // AAAAMMDD

      ImpNeto: impNeto,
      ImpIVA: impIVA,
      ImpTotal: impTotal,

      Iva: [
        {
          Id: 5,         // 5 = 21% en AFIP
          BaseImp: impNeto,
          Importe: impIVA
        }
      ],

      MonId: "PES",
      MonCotiz: 1,
    };

    // 🔹 Emitimos el comprobante
    const result = await afip.ElectronicBilling.createVoucher(factura);

    res.json({ ok: true, result });
  } catch (e) {
    console.error("❌ Error facturando:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("🚀 Worker AFIP escuchando en puerto", PORT));
