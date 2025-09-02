const express = require("express");
const fs = require("fs");
const Afip = require("afip.js");

const app = express();
app.use(express.json({ limit: "2mb" }));

// 🔑 Certificados cargados desde Render
const cert = fs.readFileSync("/etc/secrets/certificado.txt");
const key = fs.readFileSync("/etc/secrets/clave.txt");

// 🚀 Configuración AFIP
const afip = new Afip({
  CUIT: 23332382314, // 👈 reemplazá por TU CUIT real
  production: true,  // true = AFIP producción, false = homologación
  cert,              // archivo certificado
  key                // archivo clave privada
});

// 🌐 Ruta de prueba
app.get("/", (req, res) => res.send("Worker conectado con AFIP ✅"));

// 📑 Endpoint para emitir factura
app.post("/", async (req, res) => {
  try {
    const data = req.body;

    // Ejemplo de factura A
    const factura = {
      CantReg: 1,
      PtoVta: 1,
      CbteTipo: 1, // 1 = Factura A
      Concepto: 1,
      DocTipo: 80, // 80 = CUIT
      DocNro: Number(data.DocNro || "20111111112"),
      CbteDesde: 1,
      CbteHasta: 1,
      CbteFch: parseInt(new Date().toISOString().slice(0,10).replace(/-/g,"")),
      ImpTotal: Number(data.ImpTotal || 1000.00),
      ImpNeto: Number(data.ImpTotal || 1000.00),
      ImpIVA: 0,
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
