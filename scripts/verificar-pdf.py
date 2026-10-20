# Verifica los PDFs generados por scripts/probar-pdf.js
# Uso: python scripts/verificar-pdf.py <carpeta> [--png]
import sys, os, re, json
import pymupdf

out = sys.argv[1]
png = "--png" in sys.argv
res = json.load(open(os.path.join(out, "resumen.json"), encoding="utf-8"))
if png: os.makedirs(os.path.join(out, "png"), exist_ok=True)
fallas = 0
for r in res:
    f = [x for x in os.listdir(out) if x.startswith(r["caso"] + "__")][0]
    d = pymupdf.open(os.path.join(out, f))
    n = len(d); probs = []
    W, H = d[0].rect.width, d[0].rect.height
    if abs(W - 595.3) > 2 or abs(H - 841.9) > 2: probs.append(f"tamaño {W:.0f}x{H:.0f} (no A4)")
    textos = [re.sub(r"\s+", " ", p.get_text()) for p in d]
    for i, t in enumerate(textos):
        if f"Página {i+1} de {n}" not in t: probs.append(f"falta 'Página {i+1} de {n}'")
        if "Página" in t and len(t.strip()) < 120: probs.append(f"hoja {i+1} casi vacía")
        tiene_tot = ("IMPORTE TOTAL" in t) or ("TOTAL NOTA DE CRÉDITO" in t)
        if tiene_tot != (i == n - 1): probs.append(f"totales {'falta en' if i == n - 1 else 'en'} hoja {i+1}")
        if ("CAE Nro" in t) != (i == n - 1): probs.append(f"CAE {'falta en' if i == n - 1 else 'en'} hoja {i+1}")
        if i < n - 1 and "SUBTOTAL ACUMULADO" not in t: probs.append(f"hoja {i+1} sin aviso de continuación")
        if i == n - 1 and "continúa en la página siguiente" in t: probs.append("última hoja dice que continúa")
    # cruce de datos: cada renglón aparece una sola vez y en orden; el total coincide
    plano = re.sub(r"\s+", " ", " ".join(p.get_text() for p in d))
    pos = 0
    for k, desc in enumerate(r.get("descripciones", [])):
        desc = re.sub(r"\s+", " ", desc)
        hallado = plano.find(desc, pos)
        if hallado < 0: probs.append(f"renglón {k+1} faltante o fuera de orden: {desc[:30]}"); continue
        # las descripciones numeradas "(n)" son únicas: deben figurar exactamente una vez
        if re.search(r"\(\d+\)$", desc) and plano.count(desc) != 1: probs.append(f"renglón {k+1} repetido")
        pos = hallado + len(desc)
    ar = lambda v: "$ " + f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    if ar(r["total"]) not in textos[-1]: probs.append(f"total {ar(r['total'])} no figura en la última hoja")
    if r.get("neto") is not None and r.get("descripciones") and "B-" not in r["archivo"] and ar(r["neto"]) not in textos[-1]:
        probs.append(f"neto {ar(r['neto'])} no figura")
    # QR en la última hoja (imagen distinta del logo) salvo vista previa
    if "vista-previa" not in r["caso"] and len(d[-1].get_images()) < 2: probs.append("falta QR en la última hoja")
    # textos fuera de la hoja o pisados
    for i, p in enumerate(d):
        lines = []
        for b in p.get_text("dict")["blocks"]:
            for l in b.get("lines", []):
                t = "".join(s["text"] for s in l["spans"]).strip()
                if t: lines.append((pymupdf.Rect(l["bbox"]), t))
        for rc, t in lines:
            if rc.x0 < 0 or rc.y0 < 0 or rc.x1 > W + 1 or rc.y1 > H + 1: probs.append(f"fuera de hoja p{i+1}: {t[:20]}")
        for a in range(len(lines)):
            for b2 in range(a + 1, len(lines)):
                x = lines[a][0] & lines[b2][0]
                if x.is_valid and x.width > 1.5 and x.height > 1.5 and "VISTA PREVIA" not in (lines[a][1] + lines[b2][1]):
                    probs.append(f"se pisan p{i+1}: '{lines[a][1][:18]}' / '{lines[b2][1][:18]}'")
        if png: p.get_pixmap(dpi=72).save(os.path.join(out, "png", f"{r['caso']}-p{i+1}.png"))
    if probs: fallas += 1
    print(f"{r['caso']:34} hojas={n}  " + ("OK" if not probs else " | ".join(sorted(set(probs))[:8])))
print("\nTODO OK" if not fallas else f"\n{fallas} con observaciones")
