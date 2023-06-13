/*                                         
o    o               ooo.    o         o   o         8 
8b   8               8  `8.                8         8 
8`b  8 .oPYo. .oPYo. 8   `8 o8 .oPYo. o8  o8P .oPYo. 8 
8 `b 8 8oooo8 8    8 8    8  8 8    8  8   8  .oooo8 8 
8  `b8 8.     8    8 8   .P  8 8    8  8   8  8    8 8 
8   `8 `Yooo' `YooP' 8ooo'   8 `YooP8  8   8  `YooP8 8 
..:::..:.....::.....:.....:::..:....8 :..::..::.....:..
:::::::::::::::::::::::::::::::::ooP'.:::::::::::::::::
*/


/*=================================================================
=        PROCEDIMIENTO FAC_XML_ARCHIVO para sistema SI3000        =
===================================================================*/

/**
 *
 * Procedimiento almacenado de ORACLE mediante el cual se generan los archivos XML para facturas y notas
 * de cŕedito desde el sistema SI3000 en su ambiente de PRODUCCION y con el formato requerido para ser procesados 
 * por el sistema de facturación electrónica de la DIAN.
 */


/*=====  End of Section comment block  ======*/

CREATE OR REPLACE PROCEDURE FAC_XML_ARCHIVO (
  TIPO_DOCUMENTO NUMBER,
  FECHA_INICIO DATE DEFAULT NULL, 
  FECHA_FIN DATE DEFAULT NULL,
  TIENDA ENC_VENT.BOD_CODI%TYPE, 
  CAJA ENC_VENT.CAJ_CODI%TYPE DEFAULT NULL, 
  NRO_DOCUMENTO ENC_VENT.TKT_NMRO%TYPE DEFAULT NULL,
  ERROR OUT VARCHAR2
) 
IS

/* Variables para manejo de excepciones */

  SIN_PARAMETROS EXCEPTION;
  PARAMETRO_FECHA_INCOMPLETO EXCEPTION;

/* Variables para control de paginación de registros por bloques */
  
  v_lim_inferior_query NUMBER := 1;
  v_lim_superior_query NUMBER := 250;
  v_numero_pagina NUMBER := 1;

/* Variables para el cotrol de nombre de los archivos y validación de parámetros */

  v_nombre_archivo VARCHAR2(50);
  c_clobs_con_registros BOOLEAN := false;

/* Variables para el registro de errores no definidos */

  v_cod_error NUMBER;
  v_msg_error VARCHAR2(1000);

  CURSOR c_clobs (lim_inferior NUMBER, lim_superior NUMBER) IS
    /*======================================================
    =            Tablas temporales desde SI3000            =
    ======================================================*/
    
    /**
     *
     * En este primera parte se hacen las consultas para obtener los datos necesarios
     * desde la base de datos transaccional en ORACLE del sistema SI3000.
     *
     * Las consultas se dividen dentro de sentencias WITH según la tabla origen de los datos y/o su relación logica 
     * con otras tablas, teniendo en cuenta ademas el rendimiento de las consultas.
     *
     * Por lo anterior se hacen primero las consultas desde las tablas de menor nivel de detalle (ej: ENC_VENT), se 
     * limitan su rango de selección con los parámetros entregados al ejecutar el procedimiento, y finalmente se crean 
     * las siguientes consultas de mayor nivel de detalle haciendo siempre uso en los JOIN de las consultas creadas con anterioridad, 
     * para que asi ORACLE materialice estas tablas temporales y evite en lo posible ir a almacenamiento en disco para
     * leer tablas ya consultadas y que se vea afectado el rendimiento.
     * 
     */
    
    
    /*=====  End of Tablas temporales desde SI3000  ======*/
    
    WITH FACT_ENC AS (
      SELECT * FROM (
        SELECT
          ev.BOD_CODI,
          ev.CAJ_CODI,
          ev.EVE_CODI,
          ev.TKT_NMRO,
          ev.NRO_RESO,
          ev.COD_CLTE,
          ev.PRE_RESO,
          ev.FEC_OPER,
          ev.TKT_HORA,
          ev.FEC_VENC,
          ev.CUFE,
          ev.TIPO_DEV,
          em.EMP_NIT,
          em.TIP_DOC_DIAN,
          em.DIAN_REGIMEN,
          em.DIAN_CONTRIBUYENTE,
          em.NOM_COME,
          em.EMP_DIRE,
          em.ACT_ECON,
          em.EMP_CIUD,
          em.MATRICULA_MERC,
          em.DIAN_COD_RESP_FISCAL,
          em.DIAN_NOM_RESP_FISCAL,
          tv.VTA_TIPO,
          mb.COD_MONE_DIAN,         
          rdh.FEC_RESO,
          rdh.NRO_INIC AS NRO_INIC_RES,       
          rdh.NRO_FINA AS NRO_FINA_RES,
          em.MUN_CODI,
          em.PAI_CODI,
          mpe.CODIGO_ALFA_2,
          mpe.NOMBRE_COMUN,
          mme.CIU_CODI,
          nits.NIT_DESC,
          nits.NIT_TELE,
          nits.DIR_EMAIL,
          dep.CIU_DESC,
          cli.TIP_CLTE,
          cli.IDE_TIPO,
          NVL(cli.CEDULA, cli.COD_TARJ) AS CEDULA,
          cli.NIT_DESC AS NIT_DESC_CLI,
          cli.DIV_NIT,
          cli.NOMBRES, 
          cli.APELLIDOS,
          cli.CLI_NOM1,
          cli.CLI_NOM2,
          cli.CLI_APE1,
          cli.CLI_APE2,
          cli.DIRECCION,
          cli.TELEFONO,
          cli.FAC_ELEC_EMAIL,
          mc.CIU_CODI AS CIU_CODI_CLI,
          mc.CIU_DESC AS CIU_DESC_CLI,
          mm.MUN_CODI AS MUN_CODI_CLI,
          mm.MUN_DESC AS MUN_DESC_CLI,
          mpc.CODIGO_ALFA_2 AS PAI_CODI_ISO_CLI,
          mpc.NOMBRE_COMUN AS PAI_DESC_CLI,
          em.FE_VERSION,
          ROW_NUMBER() OVER (ORDER BY ev.TKT_NMRO ASC) AS RN /* RN sirve para numerar cada registro y asi poder limitar la 
          consulta del cursor por bloques de paginación*/
        FROM ENC_VENT ev
        INNER JOIN EMPRESAS em ON 
          em.EMP_CODI = '1'        
        INNER JOIN TIP_VENT tv ON 
          ev.TIP_VENT = tv.TIP_CODI
        INNER JOIN MAE_BODE mb ON 
          ev.BOD_CODI = mb.BOD_CODI
        LEFT JOIN RES_DIAN_HIST rdh ON
          ev.BOD_CODI = rdh.BOD_CODI AND 
          ev.NRO_RESO = rdh.NRO_RESO AND 
          ev.PRE_RESO = rdh.PRE_RESO
        LEFT JOIN MAE_CAJA mcj ON 
          rdh.BOD_CODI = mcj.BOD_CODI AND 
          rdh.CAJ_CODI = mcj.CAJ_CODI
        INNER JOIN MAE_NITS nits ON 
          nits.NIT_CODI = em.EMP_NIT 
        INNER JOIN MAE_CIUD dep ON 
          nits.CIU_CODI = dep.CIU_CODI
        LEFT JOIN MAE_MUNI mme ON 
          em.MUN_CODI = mme.MUN_CODI
        INNER JOIN CLIENTE cli ON 
          ev.COD_CLTE = cli.COD_TARJ
         LEFT JOIN MAE_PAIS mpc ON 
          cli.COD_PAIS = mpc.CODIGO_ALFA_2
        LEFT JOIN MAE_PAIS mpe ON 
          em.PAI_CODI = mpe.CODIGO_ALFA_2
        LEFT JOIN MAE_MUNI mm ON 
          cli.MUN_CODI = mm.MUN_CODI
        LEFT JOIN MAE_CIUD mc ON 
          NVL(TO_CHAR(cli.COD_CIUD), mm.CIU_CODI) = mc.CIU_CODI
        WHERE
          ev.TKT_ESTA != 'X' AND
          ev.EVE_CODI = TIPO_DOCUMENTO AND 
          ev.FEC_OPER BETWEEN NVL(FECHA_INICIO,ev.FEC_OPER) AND NVL(FECHA_FIN,ev.FEC_OPER) AND
          ev.BOD_CODI = NVL(TIENDA, ev.BOD_CODI) AND
          ev.CAJ_CODI = NVL(CAJA, ev.CAJ_CODI) AND 
          ev.TKT_NMRO = NVL(NRO_DOCUMENTO, ev.TKT_NMRO) AND 
          (
            (mcj.FAC_ELEC = 'S') OR
            ev.EVE_CODI=21
          )
      )
      WHERE RN BETWEEN lim_inferior AND lim_superior
    ),
    FACT_DET AS (
      SELECT
        dv.BOD_CODI,
        dv.CAJ_CODI,
        dv.EVE_CODI,
        dv.TKT_NMRO,
        dv.NRO_RESO,
        ROW_NUMBER() OVER (
          PARTITION BY 
            dv.BOD_CODI,
            dv.CAJ_CODI,
            dv.EVE_CODI,
            dv.TKT_NMRO,
            dv.NRO_RESO
          ORDER BY dv.TKT_CONS ASC
        ) AS TKT_CONS_GENERADO,
        dv.TKT_CONS,
        dv.CAN_ARTI,
        dv.VAL_VENT,
        dv.VAL_UNIT,
        ev.COD_MONE_DIAN,
        (dv.VAL_VENT - (dv.IPO_VALO*dv.CAN_ARTI))/(1+(dv.IVA_PORC/100)) AS VAL_BASE_IMP,
        dv.VAL_VENT - (dv.IPO_VALO*dv.CAN_ARTI) - ((dv.VAL_VENT - (dv.IPO_VALO*dv.CAN_ARTI))/(1+(dv.IVA_PORC/100))) AS IVA_VALO,
        dv.IVA_PORC,
        dv.IPO_VALO * dv.CAN_ARTI AS IPO_VALO_TOT,
        ((dv.IPO_VALO*dv.CAN_ARTI)/((dv.VAL_VENT - (dv.IPO_VALO*dv.CAN_ARTI))/(1+(dv.IVA_PORC/100))))*100 AS IPO_PORC,
        dv.VAL_DCTO,        
        dv.COD_BARR,
        NVL(f.ART_CODI_FRAC,ar.ART_CODI) AS ART_CODI,
        NVL(f.ART_DESC_FRAC,ar.ART_DESC) AS ART_DESC,
        ar.UNI_COMP,
        ar.EMB_COMP,
        ar.PUM_UNID,
        ar.PUM_CANT,
        mu.UNI_CODI_DIAN
      FROM
        DET_VENT dv
      INNER JOIN FACT_ENC ev ON /* FACT_ENC es el nombre de la consulta contenida en la sentencia WITH anterior a esta */
        dv.BOD_CODI = ev.BOD_CODI AND
        dv.CAJ_CODI = ev.CAJ_CODI AND
        dv.EVE_CODI = ev.EVE_CODI AND
        dv.TKT_NMRO = ev.TKT_NMRO AND
        dv.NRO_RESO = ev.NRO_RESO
      LEFT JOIN ART_FRAC f ON 
        dv.COD_BARR = f.ART_CODI_FRAC
      INNER JOIN MAE_ARTI ar ON
        NVL(f.ART_CODI, dv.COD_BARR) = ar.ART_CODI
      INNER JOIN MAE_UNID mu ON 
        dv.UNI_CODI = mu.UNI_CODI

    ),
    /**
     *
     * En esta consulta a los datos obtenidos de la tabla DET_VENT en la consulta anterior,
     * se les aplica un UNPIVOT, con el cual se generan registros por cada tipo de impuesto 
     * presente en la columnas que correspondan a estos.
     *
     */
    
    FACT_DET_IMPUESTOS AS (
      SELECT * FROM (
        SELECT
          dv.BOD_CODI,
          dv.CAJ_CODI,
          dv.EVE_CODI,
          dv.TKT_NMRO,
          dv.NRO_RESO,
          dv.TKT_CONS,
          dv.VAL_VENT,
          dv.VAL_BASE_IMP,
          dv.VAL_DCTO,
          dv.IVA_PORC,
          dv.IVA_VALO,
          dv.IPO_PORC,
          dv.IPO_VALO_TOT,
          dv.COD_MONE_DIAN
        FROM FACT_DET dv
      ) P
      /* IVA e Impuesto al consumo (IC) son los dos tipos de impuestos identificados en las columnas de la consulta */
      UNPIVOT ((VALOR_IMPUESTO, PORC_IMPUESTO) FOR TIPO_IMPUESTO IN ((IVA_VALO, IVA_PORC) AS 'IVA', (IPO_VALO_TOT, IPO_PORC) AS 'IC'))
      
    ),
    /*=================================================
    =            Generación de objetos XML            =
    =================================================*/
    
    /**
     *
     * En esta sección se generan los objetos XML con los datos obtenidos de las tablas de la base de datos de SI3000.
     * Por motivos de organización, las generación de estos objetos se divide según las secciones del foramto XML de la DIAN, 
     * seleccionando las del primer nivel del formato y las cuales contienen en varios casos subsecciones, ademas de tener en cuenta 
     * las tablas de las cuales se obtienen los datos que esta requieren.
     * Ademas del objeto(s) XML, cada consulta comparte los mismos campos como son:  
        BOD_CODI,
        CAJ_CODI,
        EVE_CODI,
        TKT_NMRO,
        NRO_RESO
     * Con estos campos cada objeto XML de cada sección se puede relacionar con su correspondiente del resto de secciones
     * según la factura o nota a la que pertenezcan.
     *
     * Para una descripción completa del significado, dominio y obligatoriedad o dependencia de cada uno de los elementos 
     * XML, se recomienda remitirse al archivo "Factura Electr¢nica Colombia - Insumo Carvajal V4-3.xlsx", el cual contiente 
     * la documentación completa de la estructura del formato de facturación electrónica aceptado por la DIAN
     *
     * Nota: Los valores [SV] y [NA] en algunos de los elementos XML son la abreviación correspondiente de "Sin valor" y "No aplica", segun sea el caso,
     * estos valores estan como defecto en elementos no mandatorios, por lo cual no afecta la correcta lectura del archivo final en el sistema de la DIAN
     */
    
    
    /*=====  End of Generación de objetos XML  ======*/
    
    /*===================================================================================================
    =            Sección ENC: Encabezado de la factura (Número, fecha, Nit adquiriente, etc)            =

    =====================================================================================================*/
    
    ENC AS (
      SELECT
        fe.BOD_CODI,
        fe.CAJ_CODI,
        fe.EVE_CODI,
        fe.TKT_NMRO,
        fe.NRO_RESO,
        fe.FEC_OPER,    
        XMLELEMENT("ENC",
          XMLFOREST(
            DECODE(fe.EVE_CODI, 0, 'INVOIC', 21, 'NC') AS "ENC_1", /* El campo EVE_CODI determina si un documento es de tipo factura o nota (0 = factura, 21 = nota crédito), 
            y segun esto cambian los valores que toman varias de los elementos de los objetos XML en adelante */
            REGEXP_SUBSTR(fe.EMP_NIT,'(\d+)',1,1) AS "ENC_2",
            fe.COD_CLTE AS "ENC_3",
            'UBL 2.1' AS "ENC_4",
            DECODE(fe.EVE_CODI, 0, 'DIAN 2.1: Factura Electrónica de Venta', 21, 'DIAN 2.1: Nota Crédito de Factura Electrónica de Venta') AS "ENC_5",
            DECODE(fe.EVE_CODI, 0, fe.PRE_RESO||fe.TKT_NMRO, 21, fe.TKT_NMRO) AS "ENC_6",            
            fe.FEC_OPER AS "ENC_7",
            fe.TKT_HORA || ':00-05:00' AS "ENC_8", /* Se concatena el texto correspondiente a la zona horaria ya que asi lo requiere el formato XML */            
            DECODE(fe.EVE_CODI, '0', '01', '21', '91') AS "ENC_9",
            fe.COD_MONE_DIAN AS "ENC_10",
            (         
              SELECT            
                COUNT(*) AS NRO_LINEAS
              FROM FACT_DET dv
              WHERE 
                dv.BOD_CODI = fe.BOD_CODI AND
                dv.CAJ_CODI = fe.CAJ_CODI AND 
                dv.EVE_CODI = fe.EVE_CODI AND 
                dv.TKT_NMRO = fe.TKT_NMRO AND 
                dv.NRO_RESO = fe.NRO_RESO
              GROUP BY
                dv.BOD_CODI,
                dv.CAJ_CODI,
                dv.EVE_CODI,
                dv.TKT_NMRO,
                dv.NRO_RESO
            ) AS "ENC_15",
            fe.FEC_VENC AS "ENC_16",
            '1' AS "ENC_20",
            DECODE(fe.EVE_CODI, '0', '10', '21', '20') AS "ENC_21"
          )
        ) AS ENC_XML
      FROM FACT_ENC fe
    ),
    /*===================================================================================================
    =            Sección EMI: Información del emisor electrónico del documento                          =
    =====================================================================================================*/
    /*----------  Subsecciones EMI  ----------*/
    /**
     *
     * TAC: Información tributaria, aduanera y cambiaria del emisor
     * DFE: Datos de dirección fiscal del emisor
     * ICC: Información del emisor en la camara de comercio
     * CDE: Datos de contactos del emisor
     * GTE: Detalles tributarios del emisor
     *
     */
    EMI AS (
      SELECT
        fe.BOD_CODI,
        fe.CAJ_CODI,
        fe.EVE_CODI,
        fe.TKT_NMRO,
        fe.NRO_RESO,
        XMLELEMENT("EMI", 
          XMLFOREST(
            '1' AS "EMI_1",
            REGEXP_SUBSTR(fe.EMP_NIT,'(\d+)',1,1) AS "EMI_2",
            fe.TIP_DOC_DIAN AS "EMI_3",
            fe.DIAN_REGIMEN AS "EMI_4",
            fe.NOM_COME AS "EMI_6",
            fe.NOM_COME AS "EMI_7",
            fe.EMP_DIRE AS "EMI_10",
            fe.CIU_CODI AS "EMI_11",
            fe.EMP_CIUD AS "EMI_13",
            fe.MUN_CODI AS "EMI_14", 
            fe.CODIGO_ALFA_2 AS "EMI_15",
            fe.CIU_DESC AS "EMI_19",
            fe.NOMBRE_COMUN AS "EMI_21",
            REGEXP_SUBSTR(fe.EMP_NIT,'(\d+)',1,2) AS "EMI_22",
            fe.MUN_CODI AS "EMI_23",
            fe.NOM_COME AS "EMI_24",
            fe.ACT_ECON AS "EMI_25"
          ),
          XMLELEMENT("TAC",
            XMLFOREST(
              fe.DIAN_CONTRIBUYENTE AS "TAC_1"
            )
          ),
          XMLELEMENT("DFE",
            XMLFOREST(
              fe.MUN_CODI AS "DFE_1",
              fe.CIU_CODI AS "DFE_2",
              fe.PAI_CODI AS "DFE_3",
              fe.MUN_CODI AS "DFE_4",
              fe.NOMBRE_COMUN AS "DFE_5",
              fe.CIU_DESC AS "DFE_6",
              fe.EMP_CIUD AS "DFE_7"
            )
          ),
          XMLELEMENT("ICC",
            XMLFOREST(
              fe.MATRICULA_MERC AS "ICC_1",
              DECODE(fe.EVE_CODI, 0, fe.PRE_RESO, 21, '') AS "ICC_9"
            )
          ),
          XMLELEMENT("CDE",
            XMLFOREST(
              '1' AS "CDE_1",
              fe.NIT_DESC AS "CDE_2",
              fe.NIT_TELE AS "CDE_3",
              fe.DIR_EMAIL AS "CDE_4"
            )
          ),
          XMLELEMENT("GTE",
            XMLFOREST(
              fe.DIAN_COD_RESP_FISCAL AS "GTE_1",
              fe.DIAN_NOM_RESP_FISCAL AS "GTE_2"
            )
          )
        ) AS EMI_XML
      FROM FACT_ENC fe
    ),
    /*================================================================================================================
    =            Sección ADQ: Información del adquiriente (Nombre, NIT o cédula, datos de dirección, etc)            =
    ==================================================================================================================*/ 
    /*----------  Subsecciones ADQ  ----------*/
    /**
     *
     * TCR: Información tributaria, aduanera y cambiaria del adquiriente
     * ILA: Información legal del adquiriente
     * DFA: Datos de dirección fiscal del adquiriente
     * CDA: Datos de contactos del adquiriente
     * GTA: Detalles tributarios del adquiriente
     *
     */          
    ADQ AS (
      SELECT
        fe.BOD_CODI,
        fe.CAJ_CODI,
        fe.EVE_CODI,
        fe.TKT_NMRO,
        fe.NRO_RESO,
        XMLELEMENT("ADQ",
          XMLFOREST(
            DECODE(fe.TIP_CLTE, 'J', '1', 
                      'N', '2') AS "ADQ_1",
            fe.CEDULA AS "ADQ_2",
            CASE
              WHEN fe.IDE_TIPO = 'Registro civil'
                THEN '11'
              WHEN fe.IDE_TIPO = 'Tarjeta de identidad'
                THEN '12'
              WHEN fe.IDE_TIPO = 'CC'
                THEN '13'
              WHEN fe.IDE_TIPO = 'Tarjeta de extranjería'
                THEN '21'
              WHEN fe.IDE_TIPO = 'Cédula de extranjería'
                THEN '22'
              WHEN fe.IDE_TIPO = 'NIT'
                THEN '31'
              WHEN fe.IDE_TIPO = 'Pasaporte'
                THEN '41'
              WHEN fe.IDE_TIPO = 'Documento de identificación extranjero'
                THEN '42'
              WHEN fe.IDE_TIPO = 'Nit de otro país'
                THEn '50'
              WHEN fe.IDE_TIPO = 'NIUP'
                THEN '91'
            END AS "ADQ_3",
            'R-99-PN' AS "ADQ_4",
            fe.CEDULA AS "ADQ_5",
            CASE
              WHEN fe.TIP_CLTE = 'J' OR fe.IDE_TIPO = 'NIT'
                THEN COALESCE(fe.NIT_DESC_CLI, fe.NOMBRES, fe.APELLIDOS)
              ELSE COALESCE(NULLIF(fe.CLI_NOM1||' '||fe.CLI_NOM2||' '||fe.CLI_APE1||' '||fe.CLI_APE2,'   '), NULLIF(fe.NOMBRES||' '||fe.APELLIDOS,' '), 'consumidor final')
            END AS "ADQ_6",
            CASE
              WHEN fe.TIP_CLTE = 'J' OR fe.IDE_TIPO = 'NIT'
                THEN COALESCE(fe.NIT_DESC_CLI, fe.NOMBRES, fe.APELLIDOS)
              ELSE COALESCE(NULLIF(fe.CLI_NOM1||' '||fe.CLI_NOM2||' '||fe.CLI_APE1||' '||fe.CLI_APE2,'   '), NULLIF(fe.NOMBRES||' '||fe.APELLIDOS,' '), 'consumidor final')
            END AS "ADQ_7",
            CASE
              WHEN NVL(fe.TIP_CLTE,'N') = 'N'
                THEN COALESCE(NULLIF(fe.CLI_NOM1||' '||fe.CLI_NOM2,' '), fe.NOMBRES, '[SV]')
              ELSE
                '[NA]'
            END AS "ADQ_8",
            CASE
              WHEN NVL(fe.TIP_CLTE,'N') = 'N'
                THEN COALESCE(NULLIF(fe.CLI_APE1||' '||fe.CLI_APE2,' '), fe.APELLIDOS, '[SV]')
              ELSE '[NA]'
            END AS "ADQ_9",
            fe.DIRECCION AS "ADQ_10", 
            fe.CIU_CODI_CLI AS "ADQ_11", 
            fe.MUN_DESC_CLI AS "ADQ_13", 
            fe.MUN_CODI_CLI AS "ADQ_14", 
            fe.PAI_CODI_ISO_CLI AS "ADQ_15",
            fe.CIU_DESC_CLI AS "ADQ_19", 
            fe.PAI_DESC_CLI AS "ADQ_21",
            fe.DIV_NIT AS "ADQ_22",
            fe.MUN_CODI_CLI AS "ADQ_23",
            fe.CEDULA AS "ADQ_24",
            CASE
              WHEN fe.IDE_TIPO = 'Registro civil'
                THEN '11'
              WHEN fe.IDE_TIPO = 'Tarjeta de identidad'
                THEN '12'
              WHEN fe.IDE_TIPO = 'CC'
                THEN '13'
              WHEN fe.IDE_TIPO = 'Tarjeta de extranjería'
                THEN '21'
              WHEN fe.IDE_TIPO = 'Cédula de extranjería'
                THEN '22'
              WHEN fe.IDE_TIPO = 'NIT'
                THEN '31'
              WHEN fe.IDE_TIPO = 'Pasaporte'
                THEN '41'
              WHEN fe.IDE_TIPO = 'Documento de identificación extranjero'
                THEN '42'
              WHEN fe.IDE_TIPO = 'Nit de otro país'
                THEn '50'
              WHEN fe.IDE_TIPO = 'NIUP'
                THEN '91'
            END AS "ADQ_25",
            fe.DIV_NIT AS "ADQ_26"
          ),
          XMLELEMENT("TCR",
            XMLFOREST(
              'R-99-PN' AS "TCR_1"
            )
          ),
          XMLELEMENT("ILA",
            XMLFOREST(
              CASE
                WHEN fe.TIP_CLTE = 'J' OR fe.IDE_TIPO = 'NIT'
                  THEN COALESCE(fe.NIT_DESC_CLI, fe.NOMBRES, fe.APELLIDOS)
                ELSE COALESCE(NULLIF(fe.CLI_NOM1||' '||fe.CLI_NOM2||' '||fe.CLI_APE1||' '||fe.CLI_APE2,'   '), NULLIF(fe.NOMBRES||' '||fe.APELLIDOS,' '), 'consumidor final')
              END AS "ILA_1",
              fe.CEDULA AS "ILA_2",
              CASE
                WHEN fe.IDE_TIPO = 'Registro civil'
                  THEN '11'
                WHEN fe.IDE_TIPO = 'Tarjeta de identidad'
                  THEN '12'
                WHEN fe.IDE_TIPO = 'CC'
                  THEN '13'
                WHEN fe.IDE_TIPO = 'Tarjeta de extranjería'
                  THEN '21'
                WHEN fe.IDE_TIPO = 'Cédula de extranjería'
                  THEN '22'
                WHEN fe.IDE_TIPO = 'NIT'
                  THEN '31'
                WHEN fe.IDE_TIPO = 'Pasaporte'
                  THEN '41'
                WHEN fe.IDE_TIPO = 'Documento de identificación extranjero'
                  THEN '42'
                WHEN fe.IDE_TIPO = 'Nit de otro país'
                  THEn '50'
                WHEN fe.IDE_TIPO = 'NIUP'
                  THEN '91'
              END AS "ILA_3",
              fe.DIV_NIT AS "ILA_4"
            )
          ),
          XMLELEMENT("DFA",
            XMLFOREST(
              fe.PAI_CODI_ISO_CLI AS "DFA_1",
              fe.CIU_CODI_CLI AS "DFA_2", 
              fe.MUN_CODI_CLI AS "DFA_3", 
              fe.MUN_CODI_CLI AS "DFA_4", 
              fe.PAI_DESC_CLI AS "DFA_5",
              fe.CIU_DESC_CLI AS "DFA_6", 
              fe.MUN_DESC_CLI AS "DFA_7" 
            )
          ),
          XMLELEMENT("CDA",
            XMLFOREST(
              '1' AS "CDA_1",
              CASE
                WHEN fe.TIP_CLTE = 'J' OR fe.IDE_TIPO = 'NIT'
                  THEN COALESCE(fe.NIT_DESC_CLI, fe.NOMBRES, fe.APELLIDOS)
                ELSE COALESCE(NULLIF(fe.CLI_NOM1||' '||fe.CLI_NOM2||' '||fe.CLI_APE1||' '||fe.CLI_APE2,'   '), NULLIF(fe.NOMBRES||' '||fe.APELLIDOS,' '), 'consumidor final')
              END AS "CDA_2",
              fe.TELEFONO AS "CDA_3",              
              fe.FAC_ELEC_EMAIL AS "CDA_4"
            )
          ),
          XMLELEMENT("GTA",
            XMLFOREST(
              '01' AS "GTA_1",
              'IVA' AS "GTA_2"
            )
          )
        ) AS ADQ_XML
      FROM FACT_ENC fe
    ),
    /*==========================================================================================================================
    =            Sección TOT: Importes totales (Suma de totales de valor bruto, impuestos, descuentos, cargos, etc)            =
    ============================================================================================================================*/
    TOT AS (
      SELECT
        fd1.BOD_CODI,
        fd1.CAJ_CODI,
        fd1.EVE_CODI,
        fd1.TKT_NMRO,
        fd1.NRO_RESO,
        XMLELEMENT("TOT",
          XMLFOREST(
            TO_CHAR(ROUND(fd1.VAL_TOTA-fd1.IVA_VALO,2), 'FM99999999999990.00') AS "TOT_1",
            fd1.COD_MONE_DIAN AS "TOT_2",
            TO_CHAR(ROUND(fd1.VAL_BASE_IMP,2), 'FM99999999999990.00') AS "TOT_3",
            fd1.COD_MONE_DIAN AS "TOT_4",
            TO_CHAR(ROUND(fd1.VAL_TOTA,2), 'FM99999999999990.00') AS "TOT_5",
            fd1.COD_MONE_DIAN AS "TOT_6",
            TO_CHAR(ROUND(fd1.VAL_TOTA,2), 'FM99999999999990.00') AS "TOT_7",
            fd1.COD_MONE_DIAN AS "TOT_8",
            '0.00' AS "TOT_11",
            fd1.COD_MONE_DIAN AS "TOT_12",
            '0.00' AS "TOT_13",
            fd1.COD_MONE_DIAN AS "TOT_14"
          )
        ) AS TOT_XML
      FROM (
        SELECT
          fd.BOD_CODI,
          fd.CAJ_CODI,
          fd.EVE_CODI,
          fd.TKT_NMRO,
          fd.NRO_RESO,
          SUM(fd.VAL_VENT) AS VAL_TOTA,
          SUM(fd.VAL_BASE_IMP) AS VAL_BASE_IMP,
          SUM(fd.IVA_VALO) AS IVA_VALO,
          SUM(fd.IPO_VALO_TOT) AS IPO_VALO_TOT,
          SUM(fd.VAL_DCTO) AS VAL_DCTO,
          MAX(fd.COD_MONE_DIAN) AS COD_MONE_DIAN
        FROM FACT_DET fd
        GROUP BY 
          fd.BOD_CODI,
          fd.CAJ_CODI,
          fd.EVE_CODI,
          fd.TKT_NMRO,
          fd.NRO_RESO
      ) fd1
    ),
    /*=========================================================================================================
    =            Sección TIM: Total de impuestos (Totales por cada impuesto aplicado a la factura)            =
    ===========================================================================================================*/
    /*----------  Subsecciones TIM  ----------*/
    /**
     *
     * IMP: Importes a nivel de cada impuesto y tarifa (%) aplicados a la factura
     * 
     */    
    TIM AS (
      SELECT
        fd1.BOD_CODI,
        fd1.CAJ_CODI,
        fd1.EVE_CODI,
        fd1.TKT_NMRO,
        fd1.NRO_RESO,
        XMLAGG(
          XMLELEMENT("TIM",
            XMLFOREST(
              'false' AS "TIM_1",
              TO_CHAR(ROUND(fd1.VALOR_IMPUESTO,2), 'FM99999999999990.00')  AS "TIM_2",
              fd1.COD_MONE_DIAN AS "TIM_3"
            ),
            (
              SELECT
                XMLAGG(
                  XMLELEMENT("IMP",
                    XMLFOREST(            
                      CASE
                        WHEN fd2.TIPO_IMPUESTO = 'IVA'
                          THEN '01'
                        WHEN fd2.TIPO_IMPUESTO = 'IC'
                          THEN '02'
                      END AS "IMP_1",
                      TO_CHAR(ROUND(fd2.VAL_BASE_IMP,2), 'FM99999999999990.00')  AS "IMP_2",
                      fd2.COD_MONE_DIAN AS "IMP_3",
                      TO_CHAR(ROUND(fd2.VALOR_IMPUESTO,2), 'FM99999999999990.00')  AS "IMP_4",
                      fd2.COD_MONE_DIAN AS "IMP_5",
                      TO_CHAR(ROUND(fd2.PORC_IMPUESTO,2), 'FM990.00')  AS "IMP_6"
                    )
                  )
                )
              FROM(
                SELECT
                  fd.BOD_CODI,
                  fd.CAJ_CODI,
                  fd.EVE_CODI,
                  fd.TKT_NMRO,
                  fd.NRO_RESO,
                  fd.TIPO_IMPUESTO,
                  SUM(fd.VAL_BASE_IMP) AS VAL_BASE_IMP,
                  MAX(fd.COD_MONE_DIAN) AS COD_MONE_DIAN,
                  SUM(fd.VALOR_IMPUESTO) AS VALOR_IMPUESTO,
                  fd.PORC_IMPUESTO
                FROM FACT_DET_IMPUESTOS fd 
                WHERE fd.TIPO_IMPUESTO != 'IC' /*En esta implementación específica para Supermayorista, 
                la empresa requiere que no se discrimine el impuesto al consumo*/
                GROUP BY 
                  fd.BOD_CODI,
                  fd.CAJ_CODI,
                  fd.EVE_CODI,
                  fd.TKT_NMRO,
                  fd.NRO_RESO,
                  fd.TIPO_IMPUESTO,
                  fd.PORC_IMPUESTO
              ) fd2
              WHERE
                fd2.BOD_CODI = fd1.BOD_CODI AND
                fd2.CAJ_CODI = fd1.CAJ_CODI AND
                fd2.EVE_CODI = fd1.EVE_CODI AND
                fd2.TKT_NMRO = fd1.TKT_NMRO AND
                fd2.NRO_RESO = fd1.NRO_RESO AND 
                fd2.TIPO_IMPUESTO = fd1.TIPO_IMPUESTO
            ) AS "IMP"  
          )
        ) AS TIM_XML
      FROM (
        SELECT 
          fd.BOD_CODI,
          fd.CAJ_CODI,
          fd.EVE_CODI,
          fd.TKT_NMRO,
          fd.NRO_RESO,
          fd.TIPO_IMPUESTO,
          SUM(fd.VALOR_IMPUESTO) AS VALOR_IMPUESTO,
          MAX(fd.COD_MONE_DIAN) AS COD_MONE_DIAN
        FROM FACT_DET_IMPUESTOS fd
        WHERE fd.TIPO_IMPUESTO != 'IC'
        GROUP BY 
          fd.BOD_CODI,
          fd.CAJ_CODI,
          fd.EVE_CODI,
          fd.TKT_NMRO,
          fd.NRO_RESO,
          fd.TIPO_IMPUESTO
      ) fd1
      GROUP BY 
        fd1.BOD_CODI,
        fd1.CAJ_CODI,
        fd1.EVE_CODI,
        fd1.TKT_NMRO,
        fd1.NRO_RESO
    ),
    /*==============================================================================================
    =            Secciones de DRF a CTS: Secciones con datos adicionales a nivel de factura o nota =
    =            (ej. resolución de factura, medio de pago y referencia a documento origen)        =
    ================================================================================================*/
    DRF_CTS AS (
      SELECT
        fe.BOD_CODI,
        fe.CAJ_CODI,
        fe.EVE_CODI,
        fe.TKT_NMRO,
        fe.NRO_RESO,
        XMLCONCAT(
          XMLELEMENT("DRF",
            XMLFOREST(
              DECODE(fe.EVE_CODI, 0, fe.NRO_RESO, NULL) AS "DRF_1",
              DECODE(fe.EVE_CODI, 0, fe.FEC_RESO, NULL) AS "DRF_2",
              DECODE(fe.EVE_CODI, 0, ADD_MONTHS(fe.FEC_RESO,12), NULL) AS "DRF_3",
              DECODE(fe.EVE_CODI, 0, fe.PRE_RESO, 21, '') AS "DRF_4",
              fe.NRO_INIC_RES AS "DRF_5",
              fe.NRO_FINA_RES AS "DRF_6"
            )
          ),
          XMLELEMENT("NOT",
            XMLELEMENT("NOT_1", '6.-|')
          ),
          (
            SELECT
              XMLAGG(
                XMLELEMENT("REF",
                  XMLELEMENT("REF_1", DECODE(fe.EVE_CODI, '21', 'IV','0','IV')),
                  XMLFOREST(
                    DECODE(cm.EVE_CODI_ORIG, 0, cm.PRE_RESO_ORIG||cm.TKT_ORIG, cm.TKT_ORIG) AS "REF_2",
                    cm.FEC_OPER_ORIG AS "REF_3"
                  ),
                  XMLELEMENT("REF_4", DECODE(fe.EVE_CODI, 21, fe.CUFE, 0,''))
                )
              )
            FROM(
              SELECT  DISTINCT
                cm1.BOD_CODI,
                cm1.CAJ_CODI,
                cm1.EVE_CODI,
                cm1.TKT_NMRO,
                cm1.NRO_RESO,
                cm1.PRE_RESO_ORIG,
                cm1.TKT_ORIG,
                ev.FEC_OPER AS FEC_OPER_ORIG,
                ev.EVE_CODI AS EVE_CODI_ORIG
              FROM CAM_MERC cm1
              INNER JOIN ENC_VENT ev ON 
                cm1.BOD_ORIG = ev.BOD_CODI AND
                cm1.CAJ_ORIG = ev.CAJ_CODI AND
                cm1.TKT_ORIG = ev.TKT_NMRO AND 
                cm1.NRO_RESO_ORIG = ev.NRO_RESO
            ) cm
            WHERE
              cm.BOD_CODI = fe.BOD_CODI AND
              cm.CAJ_CODI = fe.CAJ_CODI AND              
              cm.TKT_NMRO = fe.TKT_NMRO AND
              cm.NRO_RESO = fe.NRO_RESO
          ),
          (
            SELECT
              XMLAGG(
                XMLELEMENT("MEP",
                  XMLFOREST(
                    dp.MED_CODI_DIAN AS "MEP_1",
                    DECODE(fe.VTA_TIPO, 'CO', '1',
                              'CR', '2') AS "MEP_2",
                    fe.FEC_VENC AS "MEP_3"
                  )
                )
              )
            FROM(
              SELECT
                dp1.BOD_CODI,
                dp1.CAJ_CODI,
                dp1.EVE_CODI,
                dp1.TKT_NMRO,
                dp1.NRO_RESO,             
                mp1.MED_CODI_DIAN
              FROM DET_PAGO dp1
              INNER JOIN MED_PAGO mp1 ON 
                dp1.MED_CODI = mp1.MED_CODI
            ) dp
            WHERE
              dp.BOD_CODI = fe.BOD_CODI AND
              dp.CAJ_CODI = fe.CAJ_CODI AND
              dp.EVE_CODI = fe.EVE_CODI AND
              dp.TKT_NMRO = fe.TKT_NMRO AND
              dp.NRO_RESO = fe.NRO_RESO           
          ),
          XMLELEMENT("CTS",
            XMLELEMENT("CTS_1", fe.FE_VERSION)
          )
        ) AS DRF_CTS_XML
      FROM FACT_ENC fe
    ),
    /*==================================================================================================================================================================
    =            Sección ITE: Datos a nivel de items o lineas del documento (ej. artículo, cantidad, unidades de medida, totales de valor e impuestos, etc)            =
    ====================================================================================================================================================================*/
    /*----------  Subsecciones ITE  ----------*/
    /**
     *
     * TII: Total de impuestos a nivel de cada nombre de tributo aplicado a cada item o linea de la factura
     *    IIM: Total de impuestos a nivel de cada nombre de tributo y tarifa (%) apĺicado a cada item o linea de la factura
     *
     */
    
    
    ITE AS (
      SELECT
        fd.BOD_CODI,
        fd.CAJ_CODI,
        fd.EVE_CODI,
        fd.TKT_NMRO,
        fd.NRO_RESO,
        XMLAGG(
          XMLELEMENT("ITE",
            XMLFOREST(
              fd.TKT_CONS_GENERADO AS "ITE_1",
              TO_CHAR(ROUND(fd.CAN_ARTI,2), 'FM99999999999990.00')  AS "ITE_3",
              fd.UNI_CODI_DIAN AS "ITE_4",
              TO_CHAR(ROUND(fd.VAL_VENT - fd.IVA_VALO,2), 'FM99999999999990.00')  AS "ITE_5",
              fd.COD_MONE_DIAN AS "ITE_6",
              TO_CHAR(ROUND(fd.VAL_UNIT - ((fd.IVA_VALO)/fd.CAN_ARTI),2), 'FM99999999999990.00')  AS "ITE_7",
              fd.COD_MONE_DIAN AS "ITE_8",
              CASE
                WHEN fd.IVA_VALO > 0
                  THEN 'Bienes Gravados'
                ELSE 'Bienes Cubiertos'
              END AS "ITE_10",
              fd.ART_DESC AS "ITE_11",
              fd.PUM_CANT AS "ITE_15",
              fd.ART_CODI AS "ITE_18",
              TO_CHAR(ROUND(fd.VAL_VENT,2), 'FM99999999999990.00') AS "ITE_19",
              fd.COD_MONE_DIAN AS "ITE_20",
              TO_CHAR(ROUND(fd.VAL_VENT,2), 'FM99999999999990.00') AS "ITE_21",
              fd.COD_MONE_DIAN AS "ITE_22",
              TO_CHAR(fd.CAN_ARTI, 'FM99999999999990.00') AS "ITE_27",
              fd.UNI_CODI_DIAN AS "ITE_28"
            ),        
            (
              SELECT
                XMLAGG(
                  XMLELEMENT("TII",
                    XMLFOREST(                              
                      TO_CHAR(ROUND(fd1.VALOR_IMPUESTO,2), 'FM99999999999990.00')  AS "TII_1",
                      fd1.COD_MONE_DIAN AS "TII_2",
                      'false' AS "TII_3"
                    ),
                    (
                      SELECT
                        XMLAGG(
                          XMLELEMENT("IIM",
                            XMLFOREST(
                              CASE
                                WHEN fd2.TIPO_IMPUESTO = 'IVA'
                                  THEN '01'
                                WHEN fd2.TIPO_IMPUESTO = 'IC'
                                  THEN '02'
                              END AS "IIM_1",                             
                              TO_CHAR(ROUND(fd2.VALOR_IMPUESTO,2), 'FM99999999999990.00')  AS "IIM_2",
                              fd2.COD_MONE_DIAN AS "IIM_3",
                              TO_CHAR(ROUND(fd2.VAL_BASE_IMP,2), 'FM99999999999990.00')  AS "IIM_4",
                              fd2.COD_MONE_DIAN AS "IIM_5",
                              TO_CHAR(ROUND(fd2.PORC_IMPUESTO,2), 'FM990.00')  AS "IIM_6"
                            )
                          )
                        )
                      FROM(
                        SELECT
                          fd.BOD_CODI,
                          fd.CAJ_CODI,
                          fd.EVE_CODI,
                          fd.TKT_NMRO,
                          fd.NRO_RESO,
                          fd.TKT_CONS,
                          fd.TIPO_IMPUESTO,
                          fd.PORC_IMPUESTO,
                          SUM(fd.VAL_BASE_IMP) AS VAL_BASE_IMP,
                          MAX(fd.COD_MONE_DIAN) AS COD_MONE_DIAN,
                          SUM(fd.VALOR_IMPUESTO) AS VALOR_IMPUESTO
                        FROM FACT_DET_IMPUESTOS fd 
                        WHERE TIPO_IMPUESTO != 'IC' /*En esta implementación específica para Supermayorista, 
                        la empresa requiere que no se discrimine el impuesto al consumo*/
                        GROUP BY 
                          fd.BOD_CODI,
                          fd.CAJ_CODI,
                          fd.EVE_CODI,
                          fd.TKT_NMRO,
                          fd.NRO_RESO,
                          fd.TKT_CONS,
                          fd.TIPO_IMPUESTO,
                          fd.PORC_IMPUESTO
                      ) fd2
                      WHERE
                        fd2.BOD_CODI = fd1.BOD_CODI AND
                        fd2.CAJ_CODI = fd1.CAJ_CODI AND
                        fd2.EVE_CODI = fd1.EVE_CODI AND
                        fd2.TKT_NMRO = fd1.TKT_NMRO AND
                        fd2.NRO_RESO = fd1.NRO_RESO AND 
                        fd2.TKT_CONS = fd1.TKT_CONS AND 
                        fd2.TIPO_IMPUESTO = fd1.TIPO_IMPUESTO
                    ) AS "IIM"
                  )
                )
              FROM(
                SELECT
                  fd.BOD_CODI,
                  fd.CAJ_CODI,
                  fd.EVE_CODI,
                  fd.TKT_NMRO,
                  fd.NRO_RESO,
                  fd.TKT_CONS,
                  fd.TIPO_IMPUESTO,
                  SUM(fd.VAL_BASE_IMP) AS VAL_BASE_IMP,
                  MAX(fd.COD_MONE_DIAN) AS COD_MONE_DIAN,
                  SUM(fd.VALOR_IMPUESTO) AS VALOR_IMPUESTO
                FROM FACT_DET_IMPUESTOS fd
                WHERE TIPO_IMPUESTO != 'IC' /*En esta implementación específica para Supermayorista, 
                la empresa requiere que no se discrimine el impuesto al consumo*/
                GROUP BY 
                  fd.BOD_CODI,
                  fd.CAJ_CODI,
                  fd.EVE_CODI,
                  fd.TKT_NMRO,
                  fd.NRO_RESO,
                  fd.TKT_CONS,
                  fd.TIPO_IMPUESTO
              ) fd1
              WHERE
                fd.BOD_CODI = fd1.BOD_CODI AND
                fd.CAJ_CODI = fd1.CAJ_CODI AND
                fd.EVE_CODI = fd1.EVE_CODI AND
                fd.TKT_NMRO = fd1.TKT_NMRO AND
                fd.NRO_RESO = fd1.NRO_RESO AND 
                fd.TKT_CONS = fd1.TKT_CONS
            ) AS "TII",
            XMLELEMENT("IAE",
              XMLFOREST(
                fd.ART_CODI AS "IAE_1",
                '999' AS "IAE_2"
              )
            ),
            CASE
              WHEN fd.VAL_DCTO > 0
                THEN                
                  XMLELEMENT("IDE",
                    XMLFOREST(
                      'false' AS "IDE_1",
                      TO_CHAR(ROUND(fd.VAL_DCTO,2), 'FM99999999999990.00') AS "IDE_2",
                      fd.COD_MONE_DIAN AS "IDE_3",
                      'Descuento por volumen' AS "IDE_5",
                      TO_CHAR(ROUND(fd.VAL_DCTO/(fd.VAL_BASE_IMP+fd.VAL_DCTO),2), 'FM990.00')  AS "IDE_6",
                      TO_CHAR(ROUND(fd.VAL_BASE_IMP+fd.VAL_DCTO,2), 'FM99999999999990.00') AS "IDE_7",
                      fd.COD_MONE_DIAN AS "IDE_8",          
                      '1' AS "IDE_10"
                    )
                  )
            END
          ) ORDER BY fd.TKT_CONS
        ) AS ITE_XML
      FROM FACT_DET fd
      GROUP BY 
        fd.BOD_CODI,
        fd.CAJ_CODI,
        fd.EVE_CODI,
        fd.TKT_NMRO,
        fd.NRO_RESO
      )
    /*====================================================
    =            Concatenación de objetos XML            =
    ====================================================*/
    /**
     *
     * Luego de generados los objetos XML de cada sección, estos se concatenan mediante un NATURAL JOIN 
     * usando los campos llaves señalados anteriormente.
     * Finalmente se serializa el objeto XML resultante y se genera un campo tipo texto que contiene el archivo
     * XML de cada registro/factura/nota, el cual luego servirá para transformar su contenido en un campó tipo CLOB que pueda ser
     * escrito en el sistema de archivos del sistema operativo.
     *
     */
    SELECT
      BOD_CODI,
      CAJ_CODI,
      EVE_CODI,
      TKT_NMRO,
      NRO_RESO,
      FEC_OPER,
      XMLSERIALIZE( document
        XMLCONCAT(
          XMLROOT(
            /* Dependiendo de si el documento es una factura o una nota, la cabecera o raiz del documento XML se genera al final según 
            ese parámetro */            
            CASE
              WHEN EVE_CODI = '0' THEN 
                XMLELEMENT("FACTURA",
                  XMLATTRIBUTES(
                    'http://www.w3.org/2001/XMLSchema-instance' AS "xmlns:xsi",
                    'http://www.w3.org/2001/XMLSchema' AS "xmlns:xsd"
                  ),
                  ENC_XML,
                  EMI_XML,
                  ADQ_XML,
                  TOT_XML,
                  TIM_XML,
                  DRF_CTS_XML,
                  ITE_XML
                )
              ELSE
                XMLELEMENT("NOTA",
                  XMLATTRIBUTES(
                    'http://www.w3.org/2001/XMLSchema-instance' AS "xmlns:xsi",
                    'http://www.w3.org/2001/XMLSchema' AS "xmlns:xsd"
                  ),
                  ENC_XML,
                  EMI_XML,
                  ADQ_XML,
                  TOT_XML,
                  TIM_XML,
                  DRF_CTS_XML,
                  ITE_XML
                )
            END,
            version '1.0" encoding="utf-8'
          )
        ) AS clob
      ) AS "XML_CLOB"
    FROM ENC 
    NATURAL JOIN EMI 
    NATURAL JOIN ADQ 
    NATURAL JOIN TOT 
    NATURAL JOIN TIM
    NATURAL JOIN DRF_CTS
    NATURAL JOIN ITE;

BEGIN
  /* Validaciones de valores de parámetros del procedmiento */
  IF FECHA_INICIO IS NULL AND FECHA_FIN IS NULL AND TIENDA IS NULL AND CAJA IS NULL AND NRO_DOCUMENTO IS NULL THEN 
    RAISE SIN_PARAMETROS;
  ELSIF (FECHA_INICIO IS NULL AND FECHA_FIN IS NOT NULL) OR (FECHA_INICIO IS NOT NULL AND FECHA_FIN IS NULL) THEN 
    RAISE PARAMETRO_FECHA_INCOMPLETO;
  END IF; 
  /* Se inicia un ciclo en el cual en cada iteración se ejecuta la consulta dentro del cursor limitando el resultado 
  al rango de filas de la página actual */    
  LOOP
    FOR r_clob IN c_clobs (v_lim_inferior_query,v_lim_superior_query)
    LOOP
      c_clobs_con_registros := TRUE;
      /* Se genera el nombre con el que se creará el archivo en el siguiente formato: tipo_documento_fechaemision_codigotienda_codigocaja.xml */      
      SELECT 
        DECODE(r_clob.EVE_CODI, 0, 'FC', 21, 'NC') ||r_clob.TKT_NMRO||TO_CHAR(r_clob.FEC_OPER,'YYYYMMDD')||'_'||r_clob.BOD_CODI||'_'||r_clob.CAJ_CODI||'.xml'        
        INTO v_nombre_archivo
      FROM DUAL;

      DBMS_OUTPUT.PUT_LINE(v_nombre_archivo);
      Dbms_xslprocessor.CLOB2FILE(cl => r_clob.XML_CLOB, flocation => 'FACT_ELECTRONICA_PRD', fname => v_nombre_archivo);
    END LOOP;
      /*En cada iteración se valida si el cursor contiene datos, ya que de no hacerlo indicaría que la iteración anterior fue la última 
      en obtener datos detro de su rango de filas, por lo cual se entiende que se han generado todos archivos .xml segun los parámetros
      ingresados y se da por terminado el ciclo*/
      IF NOT c_clobs_con_registros THEN
      RAISE NO_DATA_FOUND;
    END IF;
    /* Se incrementa la variable que controla el número de la página o bloque de registros a procesar, y con base en ello se recaculan 
    los limites inferior y superior del rango de dicha página */    
    v_numero_pagina := v_numero_pagina+1;
    v_lim_inferior_query := v_lim_superior_query+1;
    v_lim_superior_query := v_lim_superior_query*v_numero_pagina;
    c_clobs_con_registros := FALSE;
  END LOOP;
EXCEPTION
  WHEN SIN_PARAMETROS THEN 
    ERROR := 'Debe específicar parámetros de búsqueda';    
  WHEN PARAMETRO_FECHA_INCOMPLETO THEN 
    ERROR := 'Parámetros de búsqueda por rango de fecha incompletos';      
  WHEN NO_DATA_FOUND THEN
    IF v_numero_pagina = 1 THEN 
      ERROR := 'No se encontraron resultados con los parámetros de búsqueda utilizados';      
    /* Si la excepción NO_DATA_FOUND no se produce por errores en parámetros o falta de datos, ello indica que el ciclo de generación de los archivos XML 
    terminó y se inicia el envio de estos al sistema CEN Financiero usando el job XML_CEN_JOB*/
    ELSE
      DBMS_SCHEDULER.run_job (job_name => 'XML_CEN_PRD_JOB');
    END IF;
  WHEN OTHERS THEN
    v_cod_error := SQLCODE;
    v_msg_error := SQLERRM;
    ERROR := 'Error indefinido: Por favor consultar a soporte técnico';
    INSERT INTO FAC_XML_ARCHIVO_LOG_ERRORES 
    VALUES (
      'PRODUCCION',
      SYSDATE, 
      v_cod_error, 
      v_msg_error,
      (
        'TIPO_DOCUMENTO: '||TIPO_DOCUMENTO|| chr(13) || chr(10) ||
        'FECHA_INICIO: '||FECHA_INICIO|| chr(13) || chr(10) ||
        'FECHA_FIN: '||FECHA_FIN|| chr(13) || chr(10) ||
        'CAJA: '||CAJA|| chr(13) || chr(10) ||
        'TIENDA: '||TIENDA|| chr(13) || chr(10) ||
        'NRO_DOCUMENTO: '||NRO_DOCUMENTO
      )
    );
    commit;
END;