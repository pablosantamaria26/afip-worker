const express = require("express");
const fs = require("fs");
const Afip = require("afip.js");

const app = express();
app.use(express.json({ limit: "2mb" }));

// 🔑 Certificados desde Render Secrets
const cert = fs.readFileSync("/etc/secrets/certificado.txt");
const key = fs.readFileSync("/etc/secrets/clave.key");

// 🚀 Configuración AFIP
const afip = new Afip({
  CUIT: 23332382314,   // 👈 poné tu CUIT emisor real
  production: true,    // true = producción, false = homologación
  cert,
  key,
});

// 🌐 Ruta de prueba
app.get("/", (req, res) => res.send("Worker conectado con AFIP ✅"));

// 📑 Endpoint para emitir factura
app.post("/", async (req, res) => {
  try {
    const data = req.body;

    // ================================
    // 1. Buscar último comprobante
    // ================================
    const lastVoucher = await afip.ElectronicBilling.getLastVoucher(1, 51); 
    // PtoVta=1, Tipo=51 (Factura M)
    const proxNro = lastVoucher + 1;

    // ================================
    // 2. Armar datos de factura
    // ================================
    const today = new Date();
    const cbteFch = parseInt(today.toISOString().slice(0,10).replace(/-/g,""));

    const total = Number(data.ImpTotal || 1000.00);
    const neto = total;   // en M sin discriminar IVA (ajustar según necesidad)

    const factura = {
      CantReg: 1,          // siempre 1 comprobante
      PtoVta: 1,           // 👈 tu punto de venta
      CbteTipo: 51,        // 👈 Factura M
      Concepto: 1,         // 1=Productos
      DocTipo: 80,         // 80=CUIT
      DocNro: Number(data.DocNro || "20111111112"),
      CbteDesde: proxNro,
      CbteHasta: proxNro,
      CbteFch: cbteFch,
      ImpTotal: total,
      ImpTotConc: 0,
      ImpNeto: neto,
      ImpOpEx: 0,
      ImpIVA: 0,
      ImpTrib: 0,
      MonId: "PES",
      MonCotiz: 1,
      Iva: [] // vacío si no discriminás IVA
    };

    // ================================
    // 3. Crear comprobante en AFIP
    // ================================
    const result = await afip.ElectronicBilling.createVoucher(factura);

    res.json({ ok: true, result });
  } catch (e) {
    console.error("❌ Error facturando:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// 🔊 Levantar servidor
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Worker AFIP escuchando en", PORT));
