// Arma el detalle de ítems de una factura de extracto a partir del monto transferido.
// Criterio mayorista: precios de lista reales (Make = lista YASH × MARGEN_MAKE + IVA),
// cantidades en múltiplos de pack, y el total cierra exacto al centavo ajustando
// la cantidad y el precio de UN renglón dentro de ±DESVIO_MAX de su precio de lista.

const IVA = 1.21;
const MARGEN_MAKE = 2;      // precio de venta neto = precio de lista YASH × 2
const DESVIO_MAX = 0.12;    // el renglón de cierre puede apartarse ±12 % de su precio

let catalogoMake = [];
try {
  catalogoMake = require("../data/catalogo-make.json").items || [];
} catch (e) {
  console.warn("⚠️ [Items] Sin data/catalogo-make.json — se usa el catálogo Make interno");
}

function packMayorista(uxb) {
  if (uxb >= 24) return 12;
  if (uxb >= 12) return 6;
  return Math.max(1, uxb);
}

function armarPools({ otros = [], makeFallback = [] }) {
  const make = catalogoMake.length
    ? catalogoMake.map((p) => ({ desc: p.desc, unitC: Math.round(p.lista * MARGEN_MAKE * IVA * 100), pack: packMayorista(p.uxb) }))
    : makeFallback.map((p) => ({ desc: p.desc, unitC: Math.round(p.precio * 100), pack: p.precio >= 3000 ? 6 : 12 }));
  const resto = otros.map((p) => ({ desc: p.desc, unitC: Math.round(p.precio * 100), pack: p.precio >= 3000 ? 6 : 12 }));
  return { make, otros: resto };
}

const rnd = (a, b) => a + Math.floor(Math.random() * (b - a + 1));
function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// Busca un artículo y cantidad cuyo precio unitario (en centavos, exacto) cierre el resto.
function renglonDeCierre(restoC, pool, usados) {
  const cands = [];
  for (const p of pool) {
    if (usados.has(p.desc)) continue;
    const qMin = Math.max(1, Math.ceil(restoC / (p.unitC * (1 + DESVIO_MAX))));
    const qMax = Math.min(600, Math.floor(restoC / (p.unitC * (1 - DESVIO_MAX))));
    for (let q = qMin; q <= qMax; q++) {
      if (restoC % q !== 0) continue;
      const u = restoC / q;
      const esPack = q % p.pack === 0;
      const score = Math.abs(u / p.unitC - 1) + (esPack ? 0 : 0.06) + (q === 1 && p.pack > 1 ? 0.1 : 0);
      cands.push({ p, q, u, score });
    }
  }
  if (!cands.length) return null;
  cands.sort((a, b) => a.score - b.score);
  return cands[Math.floor(Math.random() * Math.min(5, cands.length))];
}

const gcd = (a, b) => (b ? gcd(b, a % b) : a);

// Cierre con dos renglones: el 1º en múltiplo de pack con su precio movido a lo sumo
// q2 centavos; el 2º absorbe el resto exacto dentro de ±DESVIO_MAX.
function cierreDoble(restoC, pool, usados) {
  const libres = shuffle(pool.filter((p) => !usados.has(p.desc)));
  // 1ª pasada: el 2º renglón también tiene que llegar a un pack; 2ª: cualquier cantidad
  for (const exigirPack of [true, false])
  for (const p1 of libres.slice(0, 40)) {
    for (const p2 of libres) {
      if (p2 === p1) continue;
      const share = 0.35 + Math.random() * 0.3;
      const k1 = Math.max(1, Math.round((restoC * share) / (p1.unitC * p1.pack)));
      const q1 = k1 * p1.pack;
      const r2 = restoC - q1 * p1.unitC;
      if (r2 <= p2.unitC * (1 - DESVIO_MAX)) continue;
      const q2c = r2 / p2.unitC;
      if (exigirPack && q2c < p2.pack) continue;
      const q2s = [];
      for (let q = Math.max(1, Math.floor(q2c * 0.9)); q <= Math.ceil(q2c * 1.1); q++) q2s.push(q);
      q2s.sort((a, b) => (a % p2.pack === 0 ? 0 : 1) - (b % p2.pack === 0 ? 0 : 1));
      for (const q2 of q2s) {
        if (restoC % gcd(q1, q2) !== 0) continue;
        for (let d = 0; d < q2; d++) {
          const u1 = p1.unitC + d;
          const resto2 = restoC - q1 * u1;
          if (resto2 <= 0 || resto2 % q2 !== 0) continue;
          const u2 = resto2 / q2;
          if (Math.abs(u2 / p2.unitC - 1) <= DESVIO_MAX) {
            return [{ p: p1, q: q1, u: u1 }, { p: p2, q: q2, u: u2 }];
          }
        }
      }
    }
  }
  return null;
}

function armarItemsMayoristas(montoTotal, catalogos = {}) {
  const totalC = Math.round(Number(montoTotal) * 100);
  if (!(totalC > 0)) return [];
  const { make, otros } = armarPools(catalogos);

  const nLineas =
    totalC < 100000_00 ? rnd(3, 4) :
    totalC < 300000_00 ? rnd(4, 6) :
    totalC < 800000_00 ? rnd(5, 8) : rnd(7, 10);
  const nOtros = otros.length && Math.random() < 0.6 ? rnd(1, Math.min(2, nLineas - 2)) : 0;
  const reservaC = Math.round(totalC / nLineas);

  const usados = new Set();
  const lineas = [];
  let restoC = totalC;

  for (let i = 0; i < nLineas - 1; i++) {
    const pool = shuffle(i < nOtros ? otros : make);
    const disponibleC = restoC - reservaC;
    if (disponibleC <= 0) break;
    const objetivoC = (totalC - reservaC) / (nLineas - 1) * (0.6 + Math.random() * 0.8);

    let elegido = null;
    for (const p of [...pool, ...shuffle(make)]) {
      if (usados.has(p.desc)) continue;
      const packC = p.unitC * p.pack;
      if (packC <= disponibleC) {
        const k = Math.max(1, Math.min(Math.round(objetivoC / packC), Math.floor(disponibleC / packC)));
        elegido = { p, q: k * p.pack };
        break;
      }
    }
    if (!elegido) {
      // Montos chicos: unidades sueltas de algo que entre
      const p = pool.find((x) => !usados.has(x.desc) && x.unitC <= disponibleC);
      if (!p) break;
      elegido = { p, q: Math.max(1, Math.min(Math.round(objetivoC / p.unitC), Math.floor(disponibleC / p.unitC))) };
    }
    usados.add(elegido.p.desc);
    lineas.push({ desc: elegido.p.desc, q: elegido.q, u: elegido.p.unitC });
    restoC -= elegido.q * elegido.p.unitC;
  }

  // Cierre: un renglón exacto en múltiplo de pack; si no hay, dos renglones; si no, uno exacto cualquiera
  let simple = renglonDeCierre(restoC, make, usados);
  let cierres = simple && simple.q % simple.p.pack === 0 ? [simple] : cierreDoble(restoC, make, usados);
  if (!cierres && simple) cierres = [simple];
  if (!cierres) {
    // Montos ínfimos: una unidad del artículo de precio más cercano
    const p = [...make, ...otros].filter((x) => !usados.has(x.desc))
      .sort((a, b) => Math.abs(a.unitC - restoC) - Math.abs(b.unitC - restoC))[0];
    cierres = [{ p, q: 1, u: restoC }];
  }
  for (const c of cierres) lineas.push({ desc: c.p.desc, q: c.q, u: c.u });

  const items = shuffle(lineas).map((l) => ({
    descripcion: l.desc,
    cantidad: l.q,
    precioConIva: l.u / 100,
    subtotalConIva: (l.u * l.q) / 100,
  }));

  const sumaC = lineas.reduce((s, l) => s + l.u * l.q, 0);
  if (sumaC !== totalC) throw new Error(`Armado de ítems no cierra: ${sumaC} vs ${totalC}`);
  return items;
}

module.exports = { armarItemsMayoristas, MARGEN_MAKE, IVA };
