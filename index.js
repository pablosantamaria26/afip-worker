// Worker de PRUEBA: responde un CAE falso y nro correlativo
const express = require("express");
const app = express();
app.use(express.json({ limit: "2mb" }));

// Salud para probar en el navegador
app.get("/", (req, res) => res.send("OK"));

let corr = 1000; // correlativo de prueba

// Endpoint que recibe facturas y responde con CAE falso
app.post("/", (req, res) => {
  try {
    const p = req.body || {};
    if (!p.ImpTotal || !p.DocNro) {
      return res.status(400).json({ ok: false, error: "Payload incompleto" });
    }
    const cae = "TEST" + String(Date.now()).slice(-12);
    const vto = "20991231";
    const nro = ++corr;
    res.json({ ok: true, cae, vto, nro });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e) });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Worker PRUEBA escuchando en", PORT));
