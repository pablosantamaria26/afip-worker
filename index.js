const express = require("express");
const Afip = require("@afipsdk/afip.js");


const app = express();
app.use(express.json({ limit: "2mb" }));

// ⚠️ IMPORTANTE: reemplazá este número por TU CUIT real
const afip = new Afip({
  CUIT: 23332382314,
  cert: "/etc/secrets/certificado.p12",
  key: "/etc/secrets/clave.key",
  production: true
});


// Ruta de prueba
app.get("/", (req, res) => res.send("Worker conectado con AFIP ✅"));

// Ruta para emitir factura
app.post("/", async (req, res) => {
  try {
    const data = req.body;

    // 📄 Ejemplo de factura A (ajustá según lo que necesites)
    const factura = {
      CantReg: 1,
      PtoVta: 1,             // Punto de venta habilitado en AFIP
      CbteTipo: 1,           // Factura A (si sos Responsable Inscripto)
      Concepto: 1,           // Productos
      DocTipo: 80,           // 80 = CUIT, 96 = DNI
      DocNro: Number(data.DocNro || "20111111112"),
      CbteDesde: 1,
      CbteHasta: 1,
      CbteFch: parseInt(new Date().toISOString().slice(0, 10).replace(/-/g, "")),
      ImpTotal: Number(data.ImpTotal || 1000.00),
      ImpNeto: Number(data.ImpTotal || 1000.00),
      ImpIVA: 0,
      MonId: "PES",
      MonCotiz: 1,
    };

    // 📌 Crear el próximo comprobante en AFIP
    const result = await afip.ElectronicBilling.createNextVoucher(factura);

    res.json({ ok: true, result });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Puerto de Render
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Worker AFIP escuchando en", PORT));
