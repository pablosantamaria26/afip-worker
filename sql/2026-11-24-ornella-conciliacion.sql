-- 24/11/2026 · SE EJECUTA EN EL PROYECTO DE ORNELLA (Tiempo Libre · Supabase yckoaokosdoculwxuysd), NO en el de Mercado Limpio.
-- Para qué: que el servidor de facturación de Mercado Limpio sepa qué transferencias de la cuenta Santander compartida son
-- alquileres de Ornella (ya facturados por ella con Factura C) y no las facture de nuevo.
-- Qué expone: SOLO fecha, importe, medio y documento del pagador de cada pago no-efectivo en un rango de fechas (máx. 120 días),
-- con los comprobantes C y su estado. Nada se escribe. Sin la clave correcta, no devuelve nada.
-- La clave no está acá: solo su huella SHA-256. La clave va en Render (servicio de Mercado Limpio) como ORNELLA_CLAVE.
create extension if not exists pgcrypto with schema extensions;

create or replace function public.ml_pagos_para_conciliar(p_desde date, p_hasta date, p_clave text)
returns table (pago_id uuid, fecha date, importe numeric, medio text, doc_tipo int, doc_nro text, comprobantes text, estados text)
language plpgsql
stable
security definer
set search_path = public, extensions
as $$
begin
  if p_clave is null or encode(extensions.digest(p_clave, 'sha256'), 'hex') <> '048e59b9f2e1ea9c09c560a0ff09bb4cc84ffc7709f20d27a4a740ab4fa15902' then
    raise exception 'no autorizado' using errcode = '42501';
  end if;
  if p_desde is null or p_hasta is null or p_hasta < p_desde or p_hasta - p_desde > 120 then
    raise exception 'rango de fechas inválido (máximo 120 días)';
  end if;
  return query
    select p.id, p.fecha, p.importe, p.medio,
           coalesce(p.doc_tipo, max(f.doc_tipo)),
           coalesce(p.doc_nro,  max(f.doc_nro)),
           string_agg(f.comprobante, ' + ' order by f.created_at),
           string_agg(f.estado, ',' order by f.created_at)
      from pagos p
      left join facturas f on f.pago_id = p.id and f.cbte_tipo = 11
     where p.fecha between p_desde and p_hasta
       and lower(coalesce(p.medio, '')) <> 'efectivo'
     group by p.id, p.fecha, p.importe, p.medio, p.doc_tipo, p.doc_nro
     order by p.fecha;
end;
$$;

revoke all on function public.ml_pagos_para_conciliar(date, date, text) from public;
grant execute on function public.ml_pagos_para_conciliar(date, date, text) to anon, authenticated;

-- Reversión: drop function public.ml_pagos_para_conciliar(date, date, text);
