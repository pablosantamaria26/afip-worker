const express = require("express");
const fs = require("fs");
const Afip = require("afip.js");

const app = express();
app.use(express.json({ limit: "2mb" }));

// 🔑 Certificados cargados desde Render
const key = fs.readFileSync("/etc/secrets/PabloSantamaria.key", "utf8");
const cert = fs.readFileSync("/etc/secrets/certificado.crt", "utf8");

// 🚀 Configuración AFIP (homologación por ahora)
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

    // --- SECCIÓN DE DEPURACIÓN PARA CHEQUEAR EL CUIT ---
    const cuitReceptor = Number(data.DocNro || "20111111112");
    
    // Obtener información del CUIT del cliente desde el servicio de AFIP
    const persona = await afip.ElectronicBilling.getTaxpayerDetails(cuitReceptor);
    
    // Si no se encuentra información, la AFIP lo considerará un error
    if (!persona) {
        console.error(`❌ CUIT ${cuitReceptor} no encontrado o no se pudo obtener información fiscal.`);
        return res.status(400).json({ 
            ok: false, 
            error: `El CUIT ${cuitReceptor} no se encontró en la base de datos de la AFIP.` 
        });
    }

    // Comprobar la condición de IVA y loguear el resultado
    if (persona.hasOwnProperty('iva') && persona.iva === 'Responsable Inscripto') {
        console.log(`✅ CUIT ${cuitReceptor} es Responsable Inscripto según la AFIP. ¡Todo en orden!`);
    } else {
        console.warn(`⚠️ Atención: El CUIT ${cuitReceptor} no es Responsable Inscripto. Su condición es: ${persona.iva}. La factura podría ser rechazada.`);
    }
    // --- FIN DE LA SECCIÓN DE DEPURACIÓN ---

    // 🔹 Totales
    const impTotal = Number(data.ImpTotal || 1000.00);
    const impNeto = +(impTotal / 1.21).toFixed(2);
    const impIVA = +(impTotal - impNeto).toFixed(2);

    // 🔹 Número de comprobante
    const lastVoucher = await afip.ElectronicBilling.getLastVoucher(1, 51);
    const proxNro = lastVoucher + 1;

    // 🔹 Factura
    const factura = {
      CantReg: 1,
      PtoVta: 1,
      CbteTipo: 51, // Factura M
      Concepto: 1, // Productos
      DocTipo: 80, // CUIT
      DocNro: cuitReceptor,

      // Se usa la condición de IVA enviada por el cliente
      IdIVAReceptor: Number(data.IdIVAReceptor || 11),

      CbteDesde: proxNro,
      CbteHasta: proxNro,
      CbteFch: parseInt(new Date().toISOString().slice(0, 10).replace(/-/g, "")),

      ImpNeto: impNeto,
      ImpIVA: impIVA,
      ImpTotal: impTotal,

      Iva: [
        {
          Id: 5, // 21% en AFIP
          BaseImp: impNeto,
          Importe: impIVA,
        },
      ],

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
