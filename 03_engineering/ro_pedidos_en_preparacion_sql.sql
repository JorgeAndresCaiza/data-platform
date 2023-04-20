-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://storage.googleapis.com/datasets-academy/public-img/notebooks/headers/handytec-header-notebook.png" alt="Databricks Learning" style="width: 100%;">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC **Fuente:** Repositorio Organizacional de Ventas/Pedidos
-- MAGIC 
-- MAGIC **Desarrollador:** Andrés Caiza
-- MAGIC 
-- MAGIC **Detalle de Reglas Aplicadas**
-- MAGIC 
-- MAGIC De la capa gold y silver del delta lake se aplican reglas de negocio a las siguientes tablas y se las guarda en capa Gold:
-- MAGIC 
-- MAGIC * ro_venta_pedido
-- MAGIC * cat_documento_vta_ped
-- MAGIC * cat_almacen
-- MAGIC * cat_mcu
-- MAGIC * ro_cliente_pedido
-- MAGIC * cat_microsite
-- MAGIC * cat_cambio_moneda
-- MAGIC * cat_cliente
-- MAGIC * cat_producto
-- MAGIC * cat_pais
-- MAGIC * ro_costos_web
-- MAGIC 
-- MAGIC **Resultado Final:** Se añade los registros de pedidos faltantes de Bolivia a **ro_ventas** en la capa gold del delta lake.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lógica del Negocio

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_COSTO_WEB as
select fecha,codProducto,costo,bandera,codPais, 'SAP' AS fuente
from db_gold.ro_costos_web w
where  w.etlSourceSystem='SAP' AND w.etlSourceDatabase='PE'
order by 1 desc;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TMP_VW_PEDIDOS_BO as
SELECT 
  VP.fecha,
  PED.numeroDocumento numeroFactura,
  PED.posicionDocumento posFactura,
  VP.almCodigo,
  ALM.almNombre,
  COALESCE(CASE WHEN ALM.almTipoCen = 'NO DEFINIDO' THEN NULL ELSE ALM.almTipoCen END, MCU.concepto) almTipoCen,
  MCU.mcuCod codCentro,
  MCU.mcuNombre centroNombre,
  MCU.comCodEmpresa,
  MCU.mcuCanalDistribucion,
  MCU.concepto,
  MCU.mcuNombreEmpresa,
  MCU.mcuCodHomologado,
  PED.numeroDocumento docPedido,--ID_DOC_PEDIDO
  PED.fechaCreacion fechaCreacionPedido,
  CASE WHEN PED.fechaFacturacion IS NULL THEN VP.fecha
     ELSE PED.fechaFacturacion END fechaFacturacionPedido,
  PED.codClaseDocumento codClaseDocumentoPedido,
  PED.numeroDocumento numeroDocumentoPedido,
  PED.docReferenciaComercial1 ordenPedido,
  PED.codEstadoDocumento codEstadoDocumentoPedido,
  PED.descEstadoDocumento1 descripcionEstadoDocumentoPedido,
  VCP.distrito distritoPedido, 
  VCP.provincia provinciaPedido, 
  VCP.departamento departamentoPedido,
  VCP.idCliente idClientePedido,
  VCP.tipoDocumento tipoDocumentoPedido,
  VCP.domicilioTitular domicilioTitularPedido,
  VCP.ubigeo ubigeoPedido, 
  VCP.codigoPostal codigoPostalPedido,
  VP.mcuCod codCentroPedido,
  MCUPED.mcuNombre centroNombrePedido,
--   CAT_DOCUMENTO.codClaseDocumento codClaseDocumento,
  STRING(NULL) codClaseDocumento,
  PED.docReferenciaComercial1 ordenPedidoVentas,
  CASE WHEN (CASE WHEN  PED.numeroDocumento IS NOT NULL THEN PED.fechaFacturacion ELSE VP.fecha END)<='2020-04-20' 
     THEN 'M'
     ELSE MST.codMicrosite END codMicrositeCalc,
  CASE WHEN (CASE WHEN  PED.numeroDocumento IS NOT NULL THEN PED.fechaFacturacion ELSE VP.fecha END)<='2020-04-20' 
     THEN 'MARATHON'
     ELSE MST.descMicrosite END descMicrositeCalc,

  DOUBLE(CASE WHEN VP.codPais='EC' THEN 1 ELSE MON.valorcambio END) valorcambio,
  
  CL.clirCedula codCliente,
  binary(Null) venCodCi,
  VP.codProducto codMaterial,
  MTRL.proDescProducto,
  COALESCE(MTRL.proCodGrupo,MTRL.codGrupo1) proCodGrupo,
  CASE WHEN COALESCE(MTRL.proCodGrupo,MTRL.codGrupo1)='S' THEN 'SERVICIOS' ELSE MTRL.descGrupo END descGrupo,
  MTRL.codMarca,
  MTRL.descMarca,
  MTRL.descAgrupMarcas,
  MTRL.codActividad,
  MTRL.descActividad,
  MTRL.codequipo,
  MTRL.descequipo,
  VP.codPais,
  PAIS.descPais,
  VP.codMoneda,
  MST.codMicrosite, --ID MICROSITE
  MST.descMicrosite,
  DOUBLE(NULL) valorUnitario,
  DOUBLE(CAST(VP.importeDescuento AS DECIMAL(28,2))) importeDescuento,
  int(NULL) precioPorMayor,
  VP.cantidadFactura cantidadFactura,
  VP.valorFacturaNeto valorNetoFactura,
  double(NULL) costoUnitario,
  VP.costoFactura costoFacturaReal,
  double(NULL) costoUnitarioWeb,
  CASE WHEN VP.fecha >= '2021-01-01' THEN (C.costo*VP.cantidadFactura) ELSE 0 END costoFacturaRealWeb,
  
  VP.importeImpuesto importeImpuesto,
  double(VP.valorFacturaNeto - VP.costoFactura) margen,
  double(NULL) costoUnitarioReal,
  double(NULL) costoTotalReal,
  VP.valorFactura,
  DOUBLE(NULL) cantidadFacturaWh,
  DOUBLE(NULL) valorNetoFacturaWh,
  DOUBLE(NULL) valorVentaTotal,
  VP.costoFactura,
  DOUBLE(NULL) costoFacturaWh,
  DOUBLE(NULL) precioCredito,
  DOUBLE(NULL) costoFacturaRealWh,
  INT(NULL) banderaDevolucion,
  VP.etlSourceDatabase,
  VP.etlSourceSystem,
  VP.etlTimestamp
FROM db_gold.ro_venta_pedido VP
LEFT JOIN db_gold.cat_documento_vta_ped PED ON VP.numeroDocumento= PED.numeroDocumento AND VP.posicionDocumento= PED.posicionDocumento AND PED.etlSourceDatabase='PE' AND PED.fuenteDocumento='PEDIDO'
LEFT JOIN db_silver.cat_almacen ALM ON ALM.etlSourceDatabase = 'PE' AND VP.almCodigo = ALM.almCodigo and VP.mcuCod = ALM.mcuCod
LEFT JOIN db_gold.cat_mcu MCU ON MCU.etlSourceDatabase = 'PE' AND MCU.mcuCod= VP.mcuCod
LEFT JOIN db_silver.ro_cliente_pedido VCP ON PED.numeroDocumento=VCP.numFactura AND PED.docReferenciaComercial1=VCP.idPedido AND PED.etlSourceDatabase = VCP.etlSourceDatabase
LEFT JOIN db_gold.cat_mcu MCUPED on VP.mcuCod= MCUPED.mcuCod and MCUPED.etlSourceDatabase='PE'
LEFT JOIN db_silver.cat_microsite MST ON MST.codPais=VP.codPais AND MST.codProducto=VP.codProducto AND MST.etlSourceDatabase=VP.etlSourceDatabase
LEFT JOIN db_silver.cat_cambio_moneda MON ON MON.codpais=VP.codPais AND MON.fechaInicioVal=VP.fecha
LEFT JOIN db_gold.cat_cliente CL ON CL.etlSourceDatabase = 'PE' AND VP.codcliente = CL.clirCodigo
LEFT JOIN db_gold.cat_producto MTRL ON  MTRL.etlSourceDatabase = 'PE' AND VP.codProducto = MTRL.codProducto
LEFT JOIN db_silver.cat_pais PAIS ON PAIS.etlSourceDatabase = 'PE' AND VP.codPais = PAIS.codPais
LEFT JOIN (SELECT fecha, codProducto, costo, fuente,codPais FROM 
TMP_VW_COSTO_WEB QUALIFY ROW_NUMBER() OVER(PARTITION BY fecha, codProducto,codPais ORDER BY fuente DESC)=1) C ON 
  CASE WHEN PED.fechaFacturacion IS NULL THEN VP.fecha
     ELSE PED.fechaFacturacion END = C.fecha and VP.codProducto = C.codProducto and VP.codPais=C.codPais
WHERE PED.codEstadoDocumento = '02' AND VP.codPais='BO'

-- COMMAND ----------

-- DBTITLE 1,Borrar pedidos "EN PREPARACIÓN" de ro_ventas
DELETE FROM db_gold.ro_ventas
WHERE codEstadoDocumentoPedido = '02' AND codPais IN ('BO','PE') AND etlSourceDatabase='PE' --Se ajusto en rama BugfixCosto

-- COMMAND ----------

-- DBTITLE 1,Insertar pedidos "EN PREPARACIÓN" en ro_ventas
INSERT INTO db_gold.ro_ventas
select * from TMP_VW_PEDIDOS_BO
