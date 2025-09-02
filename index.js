const express = require("express");
const fs = require("fs");
const Afip = require("afip.js");

const app = express();
app.use(express.json({ limit: "2mb" }));

// 🔑 Certificados cargados desde Render
const cert = fs.readFileSync("/etc/secrets/certificado.txt");
const key = fs.readFileSync("/etc/secrets/clave.key");

// 🚀 Configuración AFIP
const afip = new Afip({
  CUIT: 23332382314,   // 👈 poné tu CUIT real aquí (sin guiones)
  production: true,    // true = producción, false = homologación
  cert,
  key,
});

// 🌐 Ruta de prueba
app.get("/", (req, res) => res.send("Worker conectado con AFIP ✅"));

// 📑 Endpoint para emitir Factura M con IVA 21%
app.post("/", async (req, res) => {
  try {
    const data = req.body;

    // 🔹 Tomamos total de la factura (con IVA incluido)
    const impTotal = Number(data.ImpTotal || 1000.00);
    const impNeto = +(impTotal / 1.21).toFixed(2);
    const impIVA = +(impTotal - impNeto).toFixed(2);

    const factura = {
      CantReg: 1,
      PtoVta: 1,
      CbteTipo: 51,       // 👈 Factura M
      Concepto: 1,        // 1 = Productos
      DocTipo: 80,        // 80 = CUIT
      DocNro: Number(data.DocNro || "20111111112"),
      CbteDesde: 1,
      CbteHasta: 1,
      CbteFch: parseInt(new Date().toISOString().slice(0,10).replace(/-/g,"")),

      ImpNeto: impNeto,
      ImpIVA: impIVA,
      ImpTotal: impTotal,

      Iva: [
        {
          Id: 5,           // 👈 21% en AFIP
          BaseImp: impNeto,
          Importe: impIVA
        }
      ],

      MonId: "PES",
      MonCotiz: 1,
    };

    const result = await afip.ElectronicBilling.createNextVoucher(factura);

    res.json({ ok: true, result });
  } catch (e) {
    console.error("❌ Error facturando:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Worker AFIP escuchando en", PORT));

