-- 19/11/2026 · Facturación de remitos desde la Caja de Queruclor.
-- Aditivo: no toca filas existentes. `remito_ref` es la clave del anti-duplicado (un remito = una factura; si la factura se
-- parte por más de 25 ítems, cada parte lleva el mismo remito y el índice es por comprobante, así que se permite más de una
-- fila por remito: la unicidad la garantiza el servidor consultando ANTES de pedir el CAE). `origen` = 'caja' para el historial.
alter table facturas add column if not exists remito_ref text;
alter table facturas add column if not exists origen text;
create index if not exists facturas_remito_ref_idx on facturas (remito_ref) where remito_ref is not null;
-- Reversión: alter table facturas drop column remito_ref, drop column origen;  (pierde solo esas dos columnas)
