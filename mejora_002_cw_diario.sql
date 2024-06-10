Paso 1 query1 maximos y mínimos

SELECT MIN(trx.TRANSACTIONID) as id_transaction_min
FROM  savingstransaction  AS trx 
INNER JOIN customfieldvalue fechcontab ON fechcontab.PARENTKEY = trx.ENCODEDKEY AND fechcontab.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cffb5b2e200af'
WHERE fechcontab.VALUE = ('2024-03-04');

SELECT MAX(trx.TRANSACTIONID) AS id_transaction_max
FROM  savingstransaction  AS trx 
INNER JOIN customfieldvalue fechcontab ON fechcontab.PARENTKEY = trx.ENCODEDKEY AND fechcontab.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cffb5b2e200af'
WHERE fechcontab.VALUE = ('2024-03-04');

Paso 2 query2

 Se obtiene la sumatoria de transacciones de cuentas que si transaccionaron dia habil siguiente y que no transaccionaron la fecha consultada 

SELECT  cta.ID AS numcta,
        SUM(trx.AMOUNT) AS amount,
        prod.ID AS producto     
FROM savingstransaction     AS trx 
INNER JOIN savingsaccount   AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY
INNER JOIN savingsproduct   AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY
INNER JOIN customfieldvalue AS cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cffb5b2e200af'
INNER JOIN customfieldvalue AS adjuststate ON adjuststate.PARENTKEY = trx.ENCODEDKEY AND adjuststate.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cffbc3e1a00bf'
WHERE cfv.VALUE = '2024-03-05'
 AND trx.TRANSACTIONID BETWEEN  3554796 AND 3591520
 AND adjuststate.VALUE = '0'
 AND prod.ID IN ('CCV00002')
 AND cta.ID NOT IN ( SELECT DISTINCT cta.ID 
                     FROM savingstransaction AS trx
                     INNER JOIN savingsaccount AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY
                     INNER JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY
                     INNER JOIN customfieldvalue AS cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cffb5b2e200af'
                     WHERE prod.ID IN ('CCV00002') 
                     AND  (trx.TRANSACTIONID BETWEEN 3554796 AND  3591520)
                     AND cfv.VALUE = '2024-03-04')
GROUP BY cta.ID,prod.ID;
 
Paso 3 query 3 que obtiene las retenciones

SELECT SUM(CASE WHEN tiporetencion.VALUE = '1' THEN aut.AMOUNT ELSE 0 END) AS retencion_1dia,
       SUM(CASE WHEN tiporetencion.VALUE IN ('2','3') THEN aut.AMOUNT ELSE 0 END) AS retencion_2dias,
       (SUM(CASE WHEN tiporetencion.VALUE = 'N' THEN aut.AMOUNT ELSE 0 END) + 
        SUM(CASE WHEN tiporetencion.VALUE IS NULL THEN aut.AMOUNT ELSE 0  END)) AS retencion_ndias,
       aut.CARDREFERENCETOKEN AS nro_cta,
       fechacontab.VALUE AS fechacontab
FROM authorizationhold AS aut
INNER JOIN savingstransaction AS trx ON CONVERT(SUBSTRING(aut.EXTERNALREFERENCEID,1, POSITION("-" IN aut.EXTERNALREFERENCEID)-1),DECIMAL) = trx.TRANSACTIONID
INNER JOIN customfieldvalue AS tiporetencion ON tiporetencion.PARENTKEY = trx.ENCODEDKEY AND tiporetencion.CUSTOMFIELDKEY = '8a5b0cce821fb2e8018222d6afa701a4'
INNER JOIN customfieldvalue AS fechacontab ON fechacontab.PARENTKEY = trx.ENCODEDKEY AND fechacontab.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cffb5b2e200af'
WHERE aut.state = 'PENDING'
  AND DATE(fechacontab.VALUE ) <= '2024-03-04'
GROUP BY aut.CARDREFERENCETOKEN

Paso 4 query 4 
Se obtiene los datos generales de todas las cuentas
 SELECT
      rut.VALUE AS rut,
      cfvtipocli.VALUE AS tipocliente,
      cta.ID AS numcta,
      cfvctabech.VALUE AS numctabech,
      cta.ACCOUNTSTATE AS estado,
      cfvfecrut.VALUE AS fechaaperut,
      cta.ACTIVATIONDATE AS fechaactiv,
      cta.CLOSEDDATE AS fechacierre,
      cta.BLOCKEDBALANCE AS saldo_bloqueado,
      cfvofi.VALUE AS oficina,
      ruttutor.VALUE AS rut_tutor,
      prod.ID AS producto,
      cta.HOLDBALANCE AS saldo_retenido,
      codbloqueo.VALUE AS codbloqueo,
      causalcierre,VALUE AS causalcierre
  FROM
      savingsaccount AS cta
      INNER JOIN client AS cli ON cli.ENCODEDKEY = cta.ACCOUNTHOLDERKEY
      INNER JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY
      INNER JOIN customfieldvalue AS rut ON  rut.PARENTKEY = cli.ENCODEDKEY AND rut.CUSTOMFIELDKEY = '8a5b2bc07d0fe177017d28f3b5f10009'
      INNER JOIN customfieldvalue AS cfvtipocli ON cfvtipocli.PARENTKEY = cta.ENCODEDKEY AND cfvtipocli.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cf183c16e006d'
      INNER JOIN customfieldvalue AS cfvctabech ON cfvctabech.PARENTKEY = cta.ENCODEDKEY AND cfvctabech.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cf18f03f80097'
      INNER JOIN customfieldvalue AS cfvfecrut ON cfvfecrut.PARENTKEY = cta.ENCODEDKEY AND cfvfecrut.CUSTOMFIELDKEY = '8a5b2bc07d0fe177017d2920d0bb0037'
      INNER JOIN customfieldvalue AS cfvofi ON cfvofi.PARENTKEY = cta.ENCODEDKEY AND cfvofi.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cf19049c8009b'
      INNER JOIN customfieldvalue AS ruttutor ON ruttutor.PARENTKEY = cta.ENCODEDKEY AND ruttutor.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cf18895b60089'
      INNER JOIN customfieldvalue AS codbloqueo ON codbloqueo.PARENTKEY = cta.ENCODEDKEY AND  codbloqueo.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cf18478bc0071' THEN VALUE END) AS codbloqueo,
      INNER JOIN customfieldvalue AS causalcierre ON causalcierre.PARENTKEY = cta.ENCODEDKEY AND  causalcierre.CUSTOMFIELDKEY = '8a5b04e77cdc9720017cf187a9a10085' THEN VALUE END) AS causalcierre
 WHERE prod.ID IN ('CCV00002')
  AND cta.ACCOUNTSTATE IN ('ACTIVE','LOCKED','CLOSED')
  
Paso 5 Se obtiene los saldos de todas las transacciones para una fecha contable especifica

