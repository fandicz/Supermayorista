FUNCTION FnListaArticulosWS(
  p_bod_codi IN VARCHAR2,
  error OUT VARCHAR2
) RETURN clob
IS

  p_emp_codi VARCHAR2(3) := '1';
  json CLOB := '';
  c_clobs_con_registros BOOLEAN := false;
  -- Inicio () DESCUDERO
  va_vcAplicaDesde   Varchar2(2);
  va_nuValorVenta    Art_Vent.Art_Pvta%Type;
  va_nuValorDesde   Art_Vent.Art_Pvta%Type;
  -- Fin () DESCUDERO

  CURSOR c_clobs (emp_codi VARCHAR2, v_bod_codi VARCHAR2) IS
    WITH venta_dia AS (
      SELECT
        v.cod_barr AS cod_art_frac,
        NVL(f.art_codi,v.cod_barr) AS cod_art_padre,
        v.cant_pos_frac AS venta_frac,
        SUM(cant_pos_frac) OVER(PARTITION BY NVL(f.art_codi,v.cod_barr)) AS venta_art
      FROM
      (
        SELECT
          d.cod_barr,
          SUM(DECODE(d.eve_codi,'0', 1 ,'21', -1) * d.can_Arti * DECODE(d.val_frac,0,1,NULL,1,d.val_frac)) AS cant_pos_frac
        FROM enc_vent e
        INNER JOIN det_vent d ON
          e.bod_codi = d.bod_codi AND
          e.tkt_nmro = d.tkt_nmro AND
          e.caj_codi = d.caj_codi AND
          e.eve_codi = d.eve_codi AND
          e.tkt_esta = 'G' AND
          e.fec_oper = TRUNC(SYSDATE) AND
          e.bod_codi = v_bod_codi AND
          e.eve_codi IN (0,21)
        GROUP BY d.cod_barr
      ) v
      LEFT JOIN art_frac f ON
        v.cod_barr = f.art_codi_frac
    ),
    pedidos_dia AS (
      SELECT
        v.cod_barr AS cod_art_frac,
        NVL(f.art_codi,v.cod_barr) AS cod_art_padre,
        v.cant_pos_frac AS ped_frac,
        SUM(cant_pos_frac) OVER(PARTITION BY NVL(f.art_codi,v.cod_barr)) AS ped_art
      FROM
      (
        SELECT
            d.cod_barr,
            SUM(DECODE(d.eve_codi,'0', 1 ,'21', -1) * d.can_Arti * DECODE(d.val_frac,0,1,NULL,1,d.val_frac)) AS cant_pos_frac
            FROM enc_vent_pedi e
            INNER JOIN det_vent_pedi d ON
              e.bod_codi= d.bod_codi AND
              e.tkt_nmro = d.tkt_nmro AND
              e.eve_codi = d.eve_codi AND
              e.tkt_esta = 'G' AND
              e.bod_codi = v_bod_codi  AND
              e.eve_codi IN (0,21) AND
              e.fec_venc >= TRUNC(SYSDATE)
            GROUP BY d.cod_barr
      ) v
      LEFT JOIN art_frac f ON
        v.cod_barr = f.art_codi_frac
    )

    SELECT
      codigo,
      codigo_barras,
      nombre,
      iva,
      und_embalaje,
      embalaje,
      und_pum,
      pum,
      -- Inicio () DESCUDERO
      Tip_Codi,
      Min_Can_Dife,
      -- Fin () DESCUDERO
      costo,
      ROUND(DECODE(por_incre,0,art_pvta*embalaje_frac,(art_pvta*embalaje_frac)+(art_pvta*embalaje_frac)*(por_incre/100))) AS valor,
      proveedor,
      nombreproveedor,
      estado,
      unidad,
      ipconsumo,
      CASE
        WHEN val_frac >=1
          THEN TRUNC(saldo / val_frac)
        ELSE TRUNC(saldo * val_frac)
      END AS saldo,
      cantidad_desde,
      ROUND(DECODE(por_incre,0,art_pvta*embalaje_frac,(art_pvta*embalaje_frac)+(art_pvta*embalaje_frac)*(por_incre/100))) AS valor_desde
    FROM (
      SELECT
        v.bod_codi,
        a.art_codi AS codigo,
        v.cod_barr AS codigo_barras,
        v.art_desc AS nombre,
        v.iva_porc AS iva,
        a.uni_comp AS und_embalaje,
        a.emb_comp AS embalaje,
        CASE
          WHEN f.art_codi_frac IS NULL
            THEN 1
          ELSE a.emb_comp
        END AS embalaje_frac,
        a.pum_unid AS und_pum,
        a.pum_cant AS pum,
        NVL(v2.art_pvta, v.art_pvta) AS art_pvta,
        -- Inicio () DESCUDERO
        PkInventarios.FnCosto_Art(p_emp_codi,To_Char(Sysdate,'YYYYMM'),v_bod_codi,v.cod_barr,'CP','N') As costo,
        -- (ROUND(COALESCE(mc.art_cosp, mt.art_cost,a.art_cosp,0)) * DECODE(f.val_frac,0,1,NULL,1,f.val_frac)) AS costo,
        pb.Tip_Codi As Tip_Codi,
        v.Min_Can_Dife As Min_Can_Dife,
        -- Fin () DESCUDERO
        COALESCE(tva.por_incr, tvs.por_incr, tv.inc_prec, 0) AS por_incre,
        a.nit_codi AS proveedor,
        DECODE(n.nit_tipo,'NIT',n.nit_desc,n.nit_nom1||' '||n.nit_nom2||' '||n.nit_ape1||' '||n.nit_ape2) nombreproveedor,
        DECODE(a.est_arti, 'A', 'ACTIVO', 'I', 'INACTIVO', 'DESCONTI') AS estado,
        NVL(f.uni_codi, a.uni_codi) AS unidad,
        v.ipo_valo AS ipconsumo,
        i.inv_saldo,
        NVL(vd.venta_art,0) AS venta_art,
        NVL(pd.ped_art, 0) AS ped_art,
        DECODE(f.val_frac,0,1,NULL,1,f.val_frac) AS val_frac,
        NVL(i.inv_saldo,0) - NVL(vd.venta_art,0) - NVL(pd.ped_art, 0) AS saldo,
        1 AS cantidad_desde,
        v.pvp_dife,
        NVL(pb.inv_porc_disp, 100) AS inv_porc_disp
      FROM art_vent v
      INNER JOIN pos_vari_bode pb ON
        v.bod_codi = v_bod_codi AND
        v.est_arti = 'A' AND
        v.bod_codi = pb.bod_codi
      LEFT JOIN art_frac f ON
        v.cod_barr = f.art_codi_frac
      LEFT JOIN art_vent v2 ON
        f.art_codi = v2.cod_barr AND
        v2.bod_codi = pb.bod_codi
      INNER JOIN mae_arti a ON
        NVL(f.art_codi, v.cod_barr) = a.art_codi
        AND a.ENV_SICOE = 'S'
      LEFT JOIN mae_nits n ON
        a.nit_codi = n.nit_codi
      LEFT JOIN mae_cosp mc ON
        mc.emp_codi = emp_codi AND
        mc.per_codi = TO_CHAR(TRUNC(SYSDATE), 'YYYYMM') AND
        mc.bod_codi = v.bod_codi AND
        NVL(f.art_codi, v.cod_barr) = mc.art_codi
      LEFT JOIN mae_tari mt ON
        mc.art_codi IS NULL AND
        NVL(f.art_codi, v.cod_barr) = mt.art_codi AND
        mt.bod_codi  = v.bod_codi
      LEFT JOIN tip_vta_arti tva ON
        NVL(f.art_codi, v.cod_barr) = tva.art_codi AND
        pb.tip_codi = tva.tip_codi
        and pb.bod_codi = tva.bod_codi
      LEFT JOIN tip_vta_sgto tvs ON
        tva.art_codi IS NULL AND
        tvs.tip_codi  = pb.tip_codi AND
        tvs.gru_codi  = v.gru_codi AND
        tvs.lin_codi  = v.lin_codi AND
        tvs.niv3_codi = v.niv3_codi AND
        tvs.niv4_codi = v.niv4_codi
      LEFT JOIN tip_vent tv ON
        tvs.tip_codi IS NULL AND
        pb.tip_codi = tv.tip_codi
      LEFT JOIN (
        SELECT
          art_codi,
          bod_codi,
          SUM(NVL(can_real,0)) as inv_saldo
        FROM inv_sald
        WHERE
          bod_codi = v_bod_codi AND
          per_codi >= TO_CHAR(TRUNC(SYSDATE), 'YYYY')||'00' AND
          per_codi <= TO_CHAR(TRUNC(SYSDATE), 'YYYYMM')
        GROUP BY
          emp_codi,
          bod_codi,
          art_codi
      ) i ON
        a.art_codi = i.art_codi
      LEFT JOIN venta_dia vd ON
        v.cod_barr = vd.cod_art_frac
      LEFT JOIN pedidos_dia pd ON
        v.cod_barr = pd.cod_art_frac
      WHERE v.bod_codi = v_bod_codi
    ) P;

BEGIN
  json := '[';
  FOR r IN c_clobs(p_emp_codi, p_bod_codi)
  LOOP
    c_clobs_con_registros := TRUE;
    IF c_clobs%ROWCOUNT <> 1 THEN
      json := json||',';
    END IF;
     json := json||'{';
     json := CONCAT(json, '"codigo":"'||r.codigo||'",');
     json := CONCAT(json, '"codigo_barras":"'||r.codigo_barras||'",');
     json := CONCAT(json, '"nombre":"'||REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(BOTH '"' FROM r.nombre),CHR(10),''),CHR(13),''),CHR(9),''),CHR(39),'Â´'),'\','\\\\'),'"','\"')||'",');
     json := CONCAT(json, '"iva":'||to_clob(r.iva)||',');
     json := CONCAT(json, '"und_embalaje":"'||r.und_embalaje||'",');
     json := CONCAT(json, '"embalaje":'||
       CASE
        WHEN r.embalaje < 1 AND r.embalaje > 0 THEN '0'
          ELSE ''
       END||to_char(r.embalaje)||',');
     json := CONCAT(json, '"und_pum":"'||r.und_pum||'",');
     json := CONCAT(json, '"pum":'||NVL(to_clob(r.pum),'null')||',');
     json := CONCAT(json, '"costo":'||
      CASE
        WHEN r.costo < 1 AND r.costo > 0 THEN '0'
        ELSE ''
      END||to_char(r.costo)||',');
      -- Inicio () DESCUDERO
      -- precio de venta
      PKUtilidades_POS.ProPrecioArticulo(p_bod_codi        -- bodega
                                         ,r.Codigo_Barras  -- artÃ­culo
                                         ,1                -- cantidad de artÃ­culo
                                         ,r.Tip_Codi       -- tipo de venta
                                         ,'GEN'            -- programa que llama
                                         ,va_vcAplicaDesde -- (S/N) indica si debe mostrar valor desde
                                         ,va_nuValorVenta  -- valor del artÃ­culo
                                         );

      -- precio desde
      PKUtilidades_POS.ProPrecioArticulo(p_bod_codi        -- bodega
                                         ,r.Codigo_Barras  -- artÃ­culo
                                         ,r.Min_Can_Dife   -- cantidad de artÃ­culo
                                         ,r.Tip_Codi       -- tipo de venta
                                         ,'GEN'            -- programa que llama
                                         ,va_vcAplicaDesde -- (S/N) indica si debe mostrar valor desde
                                         ,va_nuValorDesde  -- valor del artÃ­culo
                                         );
      -- Fin () DESCUDERO
     json := CONCAT(json, '"valor":'||to_clob(Nvl(va_nuValorVenta,0))||','); -- LÃ­nea de modificaciÃ³n () DESCUDERO -- r.valor
     json := CONCAT(json, '"proveedor":"'||r.proveedor||'",');
     json := CONCAT(json, '"nombreprovneedor":"'||REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(BOTH '"' FROM r.nombreproveedor),CHR(10),''),CHR(13),''),CHR(9),''),'\','\\\\'),'"','\"')||'",');
     json := CONCAT(json, '"estado":"'||r.estado||'",');
     json := CONCAT(json, '"unidad":"'||r.unidad||'",');
     json := CONCAT(json, '"undconvers":"",');
     json := CONCAT(json, '"ipconsumo":'||to_clob(r.ipconsumo)||',');
     json := CONCAT(json, '"saldo":'||to_clob(r.saldo)||',');
     json := CONCAT(json, '"cantidad_desde":'||to_clob(r.cantidad_desde)||',');
     json := CONCAT(json, '"valor_desde":'||to_clob(Nvl(va_nuValorDesde,0))||'}'); -- LÃ­nea de modificaciÃ³n () DESCUDERO -- r.valor_desde
  END LOOP;
  IF NOT c_clobs_con_registros THEN
      RAISE NO_DATA_FOUND;
  END IF;
  json := json||']';
  RETURN json;
EXCEPTION
  WHEN NO_DATA_FOUND THEN
    error := 'No existen artÃ­culos para la venta o la tienda no esta parametrizada';
    json := '{"error": "'||error||'",';
    json := json || '"cod_bodi": "'||p_bod_codi||'"}';
    RETURN json;
  WHEN OTHERS THEN
    json := '{"error": "Error desconocido: '||sqlerrm||'",';
    json := json || '"cod_barr": "'||p_bod_codi||'"}';
    error := sqlerrm;
    RETURN json;
END FnListaArticulosWS;