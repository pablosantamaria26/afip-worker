const fs = require("fs");
const express = require("express");
const app = express();
app.use(express.json({ limit: "2mb" }));

// === Lectura de los secretos desde Render ===
const cert = fs.readFileSync("/etc/secrets/certificado.txt");
const key = fs.readFileSync("/etc/secrets/clave.txt");

// ⚠️ Por ahora no usamos cert/key, pero los dejamos listos
// más adelante se los pasamos a la librería de AFIP

// Salud para probar en el navegador
app.get("/", (req, res) => res.send("OK"));

// Mock temporal (CAE de prueba)
let corr = 1000; // correlativo de prueba
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
app.listen(PORT, () => console.log("Worker escuchando en", PORT));
