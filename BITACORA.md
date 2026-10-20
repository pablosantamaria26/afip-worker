# Bitácora — Sistema de Facturación ARCA · Mercado Limpio

Última actualización: **19/10/2026**

Este documento cuenta qué hace el sistema, cómo está armado, qué se cambió y por qué, y cómo se ve y se comporta la app en el celular.

---

## 1. Qué es

Sistema propio de facturación electrónica de **Mercado Limpio Distribuidora** (titular: **SANTAMARIA PABLO DANIEL**, CUIT **23-33238231-4**, IVA Responsable Inscripto, Punto de Venta **5**).

Hace tres cosas:

1. **Factura un remito o un monto** a un cliente (pestaña Facturar).
2. **Factura en lote las transferencias del extracto bancario** de Santander (pestaña Extracto).
3. **Lleva el historial**, emite **notas de crédito**, reenvía PDFs y manda el **resumen mensual** por email.

Cada factura se autoriza en ARCA (obtiene CAE), se genera su PDF con QR, se guarda en la base y se envía por email.

---

## 2. Cómo está armado

| Pieza | Dónde vive | Qué hace |
|---|---|---|
| App del celular (PWA) | Repo `pablosantamaria26/FacturacionML` (`index.html`, `app.js`, `sw.js`) | Pantallas, formularios, subida de archivos. Se instala como app en el celular. |
| Servidor | Repo `pablosantamaria26/afip-worker` (`index.js`) → **Render** (`https://api-mercadolimpio.onrender.com`) | Habla con ARCA, arma los PDFs, guarda en la base, manda emails. Se publica solo al hacer push a `main` (2–4 min). |
| Conexión con ARCA | **AFIPSDK** (`@afipsdk/afip.js`), plan Free | Intermediario para los web services de ARCA (facturación, padrón, constancia). **Límite: 1.000 consultas por período** (el período se renueva el día 24 de cada mes). |
| Base de datos | **Supabase** (proyecto `gjeyvbidomxzofcdycya`) | Tablas `facturas`, `padron_cache`, `afip_vouchers_pendientes`, `pedidos`, `items_pedido`. PDFs en Storage (`facturas-pdf`). RLS activado en todas. |
| Lectura de remitos y extractos | **Gemini** (IA de Google) | Lee el PDF del remito o el extracto y extrae CUIT, ítems, montos y referencias. |
| Emails | **Resend** (`ventas@mercadolimpio.ar`) | Factura al cliente, reporte de conciliación, resumen mensual. |

**Render Free se duerme** a los 15 minutos sin uso. La primera conexión del día tarda unos 30 segundos; la app lo maneja sola ("Servidor iniciando…").

---

## 3. Qué comprobantes emite y cuándo

Desde el **19/10/2026** el sistema decide solo qué letra corresponde, según lo que ARCA tiene registrado del cliente:

| El cliente en ARCA es… | Comprobante | Código ARCA | Condición IVA informada |
|---|---|---|---|
| IVA Responsable Inscripto | **Factura A** | 01 | 1 |
| Responsable Monotributo | **Factura A** (con leyenda Ley 27.618) | 01 | 6 |
| Consumidor Final (sin IVA ni monotributo) | **Factura B** | 06 | 5 |
| IVA Exento | **Factura B** | 06 | 4 |
| No se pudo consultar | Factura A (criterio histórico) | 01 | 1 |

Notas de crédito: misma letra que la factura que anulan — **NC A (03)**, **NC B (08)**, y **NC M (53)** si se anula una factura M vieja.

**Hasta el 14/10/2026 se emitía Factura M (código 51)**, que se mostraba como "A" en el PDF. Esas facturas siguen siendo válidas; en la base ahora figuran con su rótulo real (`M-…` / `FA-M-…`).

### Cómo sabe la condición del cliente
1. Busca en memoria (12 h) → 2. busca en la tabla `padron_cache` de Supabase (30 días) → 3. si no está, consulta la **constancia de inscripción** en ARCA (trae nombre, domicilio fiscal y si tiene IVA o monotributo) → 4. si la constancia no trae nombre o domicilio, completa con el **padrón A13**.

Lo que se confirma con la constancia se guarda en `padron_cache`, así el cliente no vuelve a gastar consultas de AFIPSDK por 30 días.

Para ver qué decide el sistema con un CUIT: `https://api-mercadolimpio.onrender.com/padron/<CUIT>` (agregar `?fresco=1` para forzar la consulta a ARCA).

---

## 4. Qué cumple el PDF (normativa ARCA)

- Letra en recuadro con **código de comprobante** debajo ("A · COD. 01", "B · COD. 06").
- Emisor: nombre de fantasía, **razón social legal** (SANTAMARIA PABLO DANIEL), domicilio comercial, condición IVA, CUIT, Ingresos Brutos, inicio de actividades.
- Receptor: CUIT, nombre o razón social, **condición IVA real según ARCA**, domicilio, condición de venta.
- Detalle de ítems:
  - **Factura A**: precio unitario neto, subtotal neto, alícuota 21 %, subtotal con IVA; totales con neto gravado e IVA discriminado.
  - **Factura B**: precios con IVA incluido (sin discriminar) y bloque **"Régimen de Transparencia Fiscal al Consumidor (Ley 27.743)"** con IVA contenido y otros impuestos nacionales indirectos.
- **CAE, vencimiento del CAE y QR de ARCA** (RG 4892).
- **Condición IVA del receptor enviada a ARCA** en cada comprobante (RG 5.616, obligatoria desde el 01/12/2026).
- Leyendas según el caso:
  - "OPERACIÓN SUJETA A RETENCIÓN": solo en comprobantes **M**.
  - Ley 27.618 (crédito fiscal para monotributistas): solo en **Factura A a monotributistas**.
- En facturas del extracto: **referencia y fecha de la transferencia bancaria**.

Datos a confirmar con el contador: el **inicio de actividades (01/01/2020)** y el **número de Ingresos Brutos (igual al CUIT)** están fijos en el sistema; si alguno no es correcto, avisar para cambiarlo.

---

## 4 bis. Cómo se arma el detalle de las facturas del extracto

La transferencia trae solo el monto, así que el sistema arma el detalle con criterio mayorista:

- **Artículos Make**: salen de la lista de precios de YASH (sección limpieza, 221 artículos, sin jardín). **Precio de venta = precio de lista × 2, más IVA.**
- **Otras marcas** (Romyl, Samantha, Todoesponja): 0 a 2 renglones con los precios fijos del catálogo interno.
- **Cantidades en múltiplos de pack**: 12 si el bulto trae 24 o más; 6 si trae entre 12 y 23; si trae menos, el bulto entero.
- El total **cierra exacto al centavo** ajustando uno o dos renglones, que pueden apartarse como máximo **±12 %** de su precio de lista.
- Cantidad de renglones según el monto: 3–4 (hasta $100.000), 4–6 (hasta $300.000), 5–8 (hasta $800.000), 7–10 (más).
- En facturas chicas (menos de $20.000) puede haber renglones de 1 unidad; en las grandes prácticamente no.

**Cuando llegue una lista nueva de YASH**, desde la carpeta `afip-worker`:

```
node scripts/importar-lista-make.js "C:\ruta\NUEVA LISTA DE PRECIOS YASH 2026 -13.xlsx"
```

Eso regenera `data/catalogo-make.json`. Después hay que subir el cambio (commit + push) para que Render lo tome. El margen (×2) se cambia en `lib/items-extracto.js` (`MARGEN_MAKE`).

---

## 5. Las reglas de fecha (importante para cerrar el mes)

- ARCA acepta facturas con fecha de hasta **5 días corridos hacia atrás**.
- Cada serie (A, B, M) tiene su numeración y **nunca puede retroceder en fecha**: si ya hay una A del 20/10, la siguiente A no puede ser del 18/10.
- El extracto respeta la fecha real de cada transferencia, acotada a esa ventana. Si ARCA rechaza la fecha (error 10016), reintenta con la fecha de hoy y **el PDF, el QR y la base quedan con la fecha que ARCA autorizó**.
- **Regla operativa:** correr el extracto del mes vencido **antes** de emitir cualquier comprobante del mes nuevo, y dentro de los primeros 5 días del mes.

---

## 6. Protección contra facturar dos veces

Al analizar el extracto y al emitir, cada transferencia se compara contra lo ya facturado:

1. **Por referencia bancaria** (exacto): si ya hay una factura con esa referencia, se omite.
2. Para facturas viejas guardadas **sin referencia** (antes de septiembre 2026): por CUIT + monto (±$2) en el mes.
3. Si ARCA autorizó una factura pero se cortó antes de guardarla, queda registrada en `afip_vouchers_pendientes` y la siguiente corrida la omite.

Dos transferencias del mismo cliente en el mes (aunque sean del mismo monto) son **dos facturas distintas** y se facturan las dos.

Volver a subir el mismo extracto es seguro: lo ya facturado aparece como "Ya facturada" y no se repite.

---

## 7. La app en el celular

Estética: fondo gris azulado claro con tarjetas tipo vidrio, acentos naranja (color de la marca) y verde para lo confirmado. Tipografía Sora. Barra de pestañas fija abajo con 4 pestañas y el botón de acción principal encima. Vibra al tocar botones importantes (en celulares que lo soportan).

### Arriba de todo
- Encabezado "🍋 Mercado Limpio" con botón **📊 Resumen**.
- Si el servidor está dormido aparece el aviso **"Servidor iniciando… listo en ~30 seg"**.
- Avisos cortos (toasts) arriba para confirmaciones y errores.

### Pestaña 🧾 Facturar
- **Tarjeta oscura** con la fecha, el estado del servidor (punto verde "Servidor activo" / rojo "Sin conexión") y tres números del mes: **Facturas**, **Total facturado** y **Clientes únicos**.
- **Subir Remito**: se elige uno o varios PDF; la IA lee CUIT, ítems, descuentos, total y domicilio de entrega, y muestra un resumen ("📋 Extracción del remito").
- **Datos de facturación**: CUIT, Monto total, Condición de venta (Transferencia, Efectivo, Cheque, Cuenta Corriente), Email (se recuerda por CUIT), atajos de descripción (Make, Romil, Varios) y Descripción libre.
- **👁 Vista previa del PDF**: se actualiza sola 1,5 s después de escribir; ya muestra la letra que va a salir (A o B según el cliente). Si hay más de 25 ítems la factura se divide en partes y la vista previa deja pasar de parte en parte.
- **⛔ Anular factura de otro mes**: abre un formulario con **Letra (A / B / M)**, Punto de venta, Número, CUIT, Monto, Fecha de la NC (máx. 5 días atrás) y Motivo.
- Botón grande abajo **⚡ EMITIR FACTURA**. Mientras emite cambia de color y va mostrando los pasos: Conectando → Preparando → Enviando a ARCA → ARCA procesando → Solicitando CAE → Generando PDF → Enviando email.
- Al terminar aparece **"¡Factura Autorizada!"** con: enviar resumen por WhatsApp, compartir PDF, abrir PDF, mandar el link del PDF por WhatsApp, **📲 Avisar a Oficina** (WhatsApp con los datos), el detalle de número/CAE/total y **📋 Emitir otra factura**.
- Si se cierra la app a mitad de una emisión, al volver ofrece reintentarla (avisando que si ya se emitió hay que cancelar para no duplicar). Si se emitió hace menos de 30 minutos, ofrece volver a ver el resultado.

### Pestaña 🏦 Extracto
1. **Subir extracto** (Excel de Santander, PDF o foto) → **🔍 ANALIZAR CON IA**. Primero despierta el servidor, sube el archivo y analiza en segundo plano (se puede guardar el celular).
2. **Lista de transferencias**: arriba la cantidad y el total a facturar. Cada tarjeta muestra inicial, nombre, fecha, descripción, monto, el CUIT editable y un botón **✓ Incluida / Excluida**. Las ya facturadas aparecen marcadas "✅ Ya facturada este mes — excluida por defecto". Sin CUIT se pide cargarlo.
   - **⚡ FACTURAR SELECCIONADAS** (verde): factura las incluidas.
   - **🏦 FACTURAR TODO (n)**: todas las del extracto con CUIT y sin facturar; antes abre la hoja **"Revisá antes de facturar"** donde se pueden quitar.
3. **Progreso**: barra verde con porcentaje, "x / total" y el conteo de emitidas, ya facturadas y errores. Se puede cerrar la app: el servidor sigue y al volver se ve el resultado.
4. **Resultado**: facturas emitidas y total; cada comprobante con nombre, rótulo (`A-00005-…` / `B-00005-…`), CAE, monto y botón **📄 PDF**; aparte las que tuvieron error con el motivo. Avisos si el proceso se cortó o si la secuencia de fechas no dejó facturar en el mes vencido.
5. Al terminar llega por email el **reporte de conciliación** (Excel) con la pestaña "Extracto Bancario" que muestra solo lo emitido en esa corrida.

### Pestaña 🔍 Historial
- Navegación por mes (‹ ›) y buscador por nombre o CUIT.
- Barra con cantidad de facturas, total y cantidad de notas de crédito.
- Tarjetas por comprobante con insignias: rótulo, "Nota de Crédito", "Anulada", "Extracto", "PDF ✓".
- Al tocar una se abre una hoja con **FACTURA A / FACTURA B / FACTURA M / NOTA DE CRÉDITO …**, total, fecha, comprobante, CUIT, CAE, condición, email y botones **📄 Ver/Descargar PDF**, **📧 Reenviar por Email** y **⛔ Emitir Nota de Crédito**.

### Pestaña 📧 Email
- Elegís el mes, ves comprobantes, total y clientes de ese mes.
- Destinatarios como tarjetas que se marcan/desmarcan; se pueden agregar emails nuevos.
- **📧 Enviar Resumen** manda el resumen del mes; abajo queda el historial de envíos (enviado ✓ / falló ✗ por destinatario).

### Botón 📊 Resumen (arriba)
- Ventana con el mes navegable: cantidad de comprobantes, total y clientes; lista de comprobantes con PDF, NC y Email; y ranking "Por cliente" con barras de porcentaje. Si no hay conexión usa lo guardado en el celular.

### Actualizaciones de la app
La app guarda una copia para abrir rápido. Cada vez que se cambia el frontend se sube la versión del Service Worker (`sw.js`, hoy **v46**); si después de una actualización algo se ve viejo, cerrar y volver a abrir la app.

---

## 8. Registro de cambios

### 19/10/2026 (noche) — Detalle mayorista realista
- El precio unitario de cada renglón salía de repartir el total al azar (por ejemplo, un escobillón a $64.000). Ahora Make usa la **lista YASH 2026-12 × 2 + IVA** y cantidades en packs (ver sección 4 bis).
- PDF: la tabla de ítems ya no usa un recorte que algunos visores de celular dibujaban mal (se cortaba la primera fila).

### 19/10/2026 — Factura A real + Factura B automática
- El contador confirmó la habilitación para Factura A. **El sistema pasó de Factura M (51) a Factura A (01).**
- **Factura B automática** para consumidores finales y exentos (antes se les hacía M/A como si fueran inscriptos, cosa que ARCA puede objetar).
- La condición IVA del receptor ahora sale de la **constancia de inscripción** de ARCA y se informa con el código correcto (antes se mandaba siempre "Responsable Inscripto").
- PDF: código de comprobante bajo la letra, razón social legal, leyenda de retención solo en M, leyenda Ley 27.618 para monotributistas, Transparencia Fiscal en B.
- **Caché de padrón arreglado**: desde septiembre no guardaba nada por un error; ahora guarda y ahorra consultas de AFIPSDK.
- **205 facturas M viejas** que figuraban en la base como "A-…" se corrigieron a "M-…" / "FA-M-…" (con respaldo) para que no se pisen con la numeración nueva de A. Las facturas ya enviadas a clientes no cambian.
- **Deduplicación arreglada**: comparaba contra monto × 1,21 y nunca detectaba duplicados; ahora compara por referencia bancaria.
- **"Ya facturada" arreglado**: marcaba todas las transferencias de un cliente con solo tener una factura en el mes; ahora compara por referencia.
- Fecha efectiva: si ARCA obliga a usar la fecha de hoy, el PDF y la base usan esa fecha.
- App: el detalle del historial muestra la letra real; la anulación manual pide la letra.
- **Pruebas reales emitidas** (vía extracto, con referencia y fecha de transferencia):
  - **A-00005-00000001** · YAN XIUYU (Resp. Inscripta) · $317.138,75 · CAE 86428519907647 · 16/10/2026 · Ref. 14468573
  - **B-00005-00000009** · FERRUFINO VANESA SOLEDAD (Consumidor Final) · $263.434,00 · CAE 86428519929938 · 17/10/2026 · Ref. 13724964
- Observación: la serie **B del punto de venta 5 empezó en el número 9**, o sea que ya había 8 Facturas B emitidas desde otro lado (por ejemplo, desde la web de ARCA) que no están en este sistema. Conviene confirmarlo con el contador.

### 15/09/2026
- Se agregó la condición IVA del receptor a ARCA (RG 5.616).
- Reintento con fecha de hoy ante error 10016 de fecha.
- Pestaña "Extracto Bancario" del reporte: muestra solo lo emitido en la corrida.
- RLS activado en las tablas de Supabase (alerta de seguridad).
- Se descubrió el límite de 1.000 consultas del plan Free de AFIPSDK (7 facturas fallaron ese día); se creó la tabla `padron_cache` para ahorrar consultas.
- Service Worker v42: la referencia bancaria no llegaba al PDF porque el celular tenía la app vieja en caché.

### Agosto–septiembre 2026
- Fechas del extracto dentro de la ventana de 5 días para cerrar el mes vencido; aviso cuando una factura posterior impide cerrarlo.
- Referencia bancaria y fecha de transferencia guardadas y mostradas en el PDF.
- Arreglo de "Unexpected end of form" en iPhone (el Service Worker ya no intercepta la API).
- Rediseño del PDF.

---

## 9. Cosas a tener en cuenta

- **Límite de AFIPSDK**: 1.000 consultas por período (renueva el 24). Cada factura usa 2 consultas (número + autorización) más 1–2 de padrón si el cliente no está en caché. Si se agota, las facturas del extracto salen con error "Alcanzaste el límite de requests"; se vuelven a facturar cuando se renueva o subiendo de plan en `app.afipsdk.com`.
- **Clientes consumidores finales**: van a salir en Factura B. Si un cliente dice que es inscripto pero sale B, es porque en ARCA no tiene IVA ni monotributo activo; se puede verificar con `/padron/<CUIT>?fresco=1`.
- **Cierre de mes**: correr el extracto del mes vencido en los primeros 5 días del mes nuevo y antes de cualquier otra factura.
- **Respaldo de la corrección de rótulos** (19/10/2026): en `backups/relabel_cambios_2026-10-19.json` (id, rótulo anterior y nuevo de cada una de las 205 filas).
