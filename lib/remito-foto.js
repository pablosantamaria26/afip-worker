// Lectura de remitos a partir de fotos (Gemini con imagen).
// Devuelve SOLO lo que figura en el documento: lo que no se lee queda en null.
// La validación (dígito verificador del CUIT, cuadre de totales) se hace acá, no en la IA.

const MIMES_OK = new Set(["image/jpeg", "image/png", "image/webp", "image/heic", "image/heif"]);

function prompt(cuitEmisor) {
  return `Sos un auditor que transcribe remitos comerciales argentinos a partir de fotos.
Transcribí EXACTAMENTE lo que figura. Si un dato no aparece o no se lee con seguridad, devolvé null (nunca lo inventes ni lo deduzcas).

El remito lo emite MERCADO LIMPIO (CUIT ${cuitEmisor}). Ese CUIT es del vendedor: NO lo devuelvas como CUIT del cliente.

Devolvé ÚNICAMENTE este JSON:
{
  "legible": true|false,
  "cuit": "11 dígitos del CLIENTE o null",
  "razonSocial": "nombre del cliente o null",
  "fecha": "dd/mm/aaaa o null",
  "nroRemito": "número del remito o null",
  "email": "email del cliente o null",
  "domicilio": "domicilio de entrega o null",
  "items": [{"cantidad": número, "descripcion": "texto", "precioUnitario": número, "subtotal": número|null, "confianza": "alta|media|baja"}],
  "subtotal": número|null,
  "descuentoImporte": número|null,
  "total": número|null,
  "confianza": {"cuit": "alta|media|baja", "total": "alta|media|baja", "items": "alta|media|baja", "fecha": "alta|media|baja", "razonSocial": "alta|media|baja", "nroRemito": "alta|media|baja", "email": "alta|media|baja", "domicilio": "alta|media|baja", "descuento": "alta|media|baja"},
  "observaciones": "texto breve si algo está tapado, cortado, borroso o es ambiguo; si no, null"
}
Reglas:
- Números en formato JSON (19.200,50 → 19200.5). precioUnitario es el precio final por unidad tal como figura (IVA incluido si el remito muestra precios finales).
- "confianza": alta = se lee perfecto; media = se lee pero hay alguna duda; baja = casi ilegible o adivinable.
- Si hay varias fotos, son hojas del MISMO remito: uní los ítems sin duplicar.`;
}

function cuitValido(c) {
  const s = String(c || "").replace(/\D/g, "");
  if (s.length !== 11) return false;
  const pesos = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2];
  const suma = pesos.reduce((acc, p, i) => acc + p * Number(s[i]), 0);
  let dv = 11 - (suma % 11);
  if (dv === 11) dv = 0;
  if (dv === 10) dv = 9;
  return dv === Number(s[10]);
}

const num = (v) => {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const s = String(v).replace(/[$\s]/g, "");
  const n = s.includes(",") ? Number(s.replace(/\./g, "").replace(",", ".")) : Number(s);
  return Number.isFinite(n) ? n : null;
};
const r2 = (n) => Math.round((Number(n) + Number.EPSILON) * 100) / 100;
const CONF = new Set(["alta", "media", "baja"]);

function normalizar(raw, cuitEmisor) {
  const d = raw && typeof raw === "object" ? raw : {};
  const conf = {};
  for (const [k, v] of Object.entries(d.confianza || {})) if (CONF.has(v)) conf[k] = v;
  const obs = [];
  if (d.observaciones) obs.push(String(d.observaciones));

  let cuit = String(d.cuit || "").replace(/\D/g, "");
  if (cuit && cuit === String(cuitEmisor)) { cuit = ""; obs.push("El CUIT leído es el de Mercado Limpio, no el del cliente."); }
  if (cuit && !cuitValido(cuit)) { conf.cuit = "baja"; obs.push(`El CUIT ${cuit} no pasa el dígito verificador: revisalo.`); }

  const items = (Array.isArray(d.items) ? d.items : []).map((it) => {
    const cantidad = num(it.cantidad);
    const precio = num(it.precioUnitario);
    const sub = num(it.subtotal);
    let c = CONF.has(it.confianza) ? it.confianza : "media";
    // Si el subtotal leído no cierra con cantidad × precio, se marca para revisión
    if (cantidad && precio && sub && Math.abs(r2(cantidad * precio) - sub) > Math.max(1, sub * 0.003)) c = "baja";
    return { cantidad: cantidad ?? 0, descripcion: String(it.descripcion || "").trim(), precioUnitario: precio ?? 0, subtotal: sub, confianza: c };
  }).filter((it) => it.descripcion || it.cantidad || it.precioUnitario);

  const total = num(d.total);
  const descuento = num(d.descuentoImporte) || 0;
  const totalCalculado = r2(items.reduce((s, it) => s + r2(it.cantidad * it.precioUnitario), 0) - descuento);
  const diferencia = total == null ? null : r2(totalCalculado - total);
  if (diferencia != null && Math.abs(diferencia) > 1) obs.push(`El total calculado ($${totalCalculado}) no coincide con el total leído ($${total}).`);

  return {
    datos: {
      legible: d.legible !== false,
      cuit: cuit || null,
      razonSocial: d.razonSocial || null,
      fecha: d.fecha || null,
      nroRemito: d.nroRemito ? String(d.nroRemito) : null,
      email: d.email && String(d.email).includes("@") ? String(d.email).trim() : null,
      domicilio: d.domicilio || null,
      items,
      subtotal: num(d.subtotal),
      descuentoImporte: descuento || null,
      total,
      confianza: conf,
    },
    validacion: { totalCalculado, totalLeido: total, diferencia, cuitValido: cuit ? cuitValido(cuit) : false },
    observaciones: obs.join(" ") || null,
  };
}

async function leerRemitoFoto({ archivos, geminiModel, cuitEmisor }) {
  const partes = archivos.map((a) => ({ inlineData: { data: a.buffer.toString("base64"), mimeType: a.mimetype } }));
  const result = await geminiModel.generateContent([prompt(cuitEmisor), ...partes]);
  const txt = result.response.text().replace(/```json/gi, "").replace(/```/g, "").trim();
  let raw;
  try { raw = JSON.parse(txt); } catch { throw new Error("La IA no devolvió un resultado legible. Probá con una foto más nítida."); }
  return normalizar(raw, cuitEmisor);
}

module.exports = { leerRemitoFoto, normalizar, cuitValido, MIMES_OK };
