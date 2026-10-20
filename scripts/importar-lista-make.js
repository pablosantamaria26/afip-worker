// Convierte la lista de precios de YASH (hoja "LISTA MAKE") en data/catalogo-make.json,
// que usa el armado de ítems de las facturas del extracto.
// Uso: node scripts/importar-lista-make.js "ruta\NUEVA LISTA DE PRECIOS YASH 2026 -12.xlsx"
const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

const archivo = process.argv[2];
if (!archivo) {
  console.error('Uso: node scripts/importar-lista-make.js "<lista YASH .xlsx>"');
  process.exit(1);
}

// Solo artículos de limpieza (desde la sección LIMPIEZA), sin jardín
const SECCIONES_EXCLUIDAS = new Set(["JARDIN"]);

// La lista trae caracteres rotos donde iban acentos/ñ
const ARREGLOS = [
  [/U�AS/g, "UÑAS"], [/BA�O/g, "BAÑO"], [/PA�O/g, "PAÑO"], [/PI�A/g, "PIÑA"],
  [/PL�STIC/g, "PLÁSTIC"], [/ALGOD�N/g, "ALGODÓN"], [/ACR�LIC/g, "ACRÍLIC"],
  [/N�/g, "N°"], [/�/g, ""],
];

function limpiarDescripcion(s) {
  let d = String(s || "");
  for (const [re, rep] of ARREGLOS) d = d.replace(re, rep);
  d = d.replace(/\*+/g, "").replace(/\s{2,}/g, " ").trim().toLowerCase();
  d = d.charAt(0).toUpperCase() + d.slice(1);
  return `Make ${d}`;
}

const wb = XLSX.readFile(archivo);
const ws = wb.Sheets["LISTA MAKE"];
if (!ws) throw new Error('El archivo no tiene la hoja "LISTA MAKE"');
const filas = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

let enLimpieza = false;
let seccion = "";
const items = [];
for (const r of filas) {
  const [, cod, desc, uxb, precio] = r;
  const esTitulo = cod && !desc && typeof precio !== "number";
  if (esTitulo) {
    seccion = String(cod).trim();
    if (seccion === "LIMPIEZA") enLimpieza = true;
    continue;
  }
  if (!enLimpieza || SECCIONES_EXCLUIDAS.has(seccion)) continue;
  if (!desc || typeof precio !== "number" || precio <= 0) continue;
  items.push({
    codigo: String(cod || "").trim(),
    desc: limpiarDescripcion(desc),
    seccion,
    uxb: Number(uxb) || 1,
    lista: Math.round(precio * 100) / 100,
  });
}

const salida = {
  origen: path.basename(archivo),
  generado: new Date().toISOString().slice(0, 10),
  precioLista: "unitario sin IVA",
  items,
};
const destino = path.join(__dirname, "..", "data", "catalogo-make.json");
fs.mkdirSync(path.dirname(destino), { recursive: true });
fs.writeFileSync(destino, JSON.stringify(salida, null, 1));
console.log(`${items.length} artículos → ${destino}`);
const rotos = items.filter((i) => /[^\x20-\x7EÁÉÍÓÚÑáéíóúñ°]/.test(i.desc));
if (rotos.length) console.log("Revisar caracteres:", rotos.map((i) => i.desc).slice(0, 10));
