from glue_custom import GLUE_CUSTOM
from glue_bancoestado import GLUE_BANCOESTADO

cw_improvement = " \
    SELECT \
    cfvc.VALUE AS rut, \
    cfvtipocli.VALUE AS tipocliente, \
    trx.BALANCE AS saldo, \
    cta.ID AS numcta, \
    '00000' AS numtrx, \
    cfvctabech.VALUE AS numctabech, \
    cta.ACCOUNTSTATE AS estado, \
    cfvfecrut.VALUE AS fechaaperut, \
    cta.ACTIVATIONDATE AS fechaactiv, \
    cta.CLOSEDDATE AS fechacierre, \
    cta.BLOCKEDBALANCE AS saldo_bloqueado, \
    trx.TRANSACTIONID AS transaccion, \
    cfvofi.VALUE AS oficina, \
    ret.retencion_1dia,  \
    ret.retencion_2dias,  \
    ret.retencion_ndias,  \
    ruttutor.VALUE AS rut_tutor,  \
    cfv.VALUE AS fecha_contable_real,  \
    prod.ID AS producto  \
FROM  \
    savingstransaction trx \
    INNER JOIN savingsaccount cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY  \
    INNER JOIN savingsproduct prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY  \
    INNER JOIN client cli ON cli.ENCODEDKEY = cta.ACCOUNTHOLDERKEY  \
    LEFT JOIN customfieldvalue cfvc ON cfvc.PARENTKEY = cli.ENCODEDKEY AND cfvc.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_rut'] + "' \
    LEFT JOIN customfieldvalue cfvtipocli ON cfvtipocli.PARENTKEY = cta.ENCODEDKEY AND cfvtipocli.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_tipocliente'] + "' \
    LEFT JOIN customfieldvalue cfvctabech ON cfvctabech.PARENTKEY = cta.ENCODEDKEY AND cfvctabech.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_numctabech'] + "' \
    LEFT JOIN customfieldvalue cfvfecrut ON cfvfecrut.PARENTKEY = cta.ENCODEDKEY AND cfvfecrut.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_fechaperut'] + "' \
    LEFT JOIN customfieldvalue cfvofi ON cfvofi.PARENTKEY = cta.ENCODEDKEY AND cfvofi.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_ofiprod'] + "' \
    LEFT JOIN customfieldvalue ruttutor ON ruttutor.PARENTKEY = cta.ENCODEDKEY AND ruttutor.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_ruttutor'] + "' \
    LEFT JOIN customfieldvalue cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_fechcontab'] + "' \
    INNER JOIN (\
        SELECT \
            cta.ID AS nro_cta, \
            MAX(trx.TRANSACTIONID) AS MaxTransactionID \
        FROM  \
            savingstransaction trx \
            INNER JOIN savingsaccount cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY  \
            WHERE trx.TRANSACTIONID <= (SELECT MAX(trx2.TRANSACTIONID)  \
                                         FROM savingstransaction trx2  \
                                         INNER JOIN customfieldvalue cfv2 ON cfv2.PARENTKEY = trx2.ENCODEDKEY  \
                                         WHERE cfv2.VALUE = '" + accountant_date + "') \
        GROUP BY cta.ID \
    ) AS maxtrx ON maxtrx.nro_cta = cta.ID AND trx.TRANSACTIONID = maxtrx.MaxTransactionID \
    LEFT JOIN ( \
        SELECT  \
            cta.ID AS nro_cta,  \
            SUM(CASE WHEN cfvtr.VALUE = '1' THEN trx.AMOUNT ELSE 0 END) AS retencion_1dia,  \
            SUM(CASE WHEN cfvtr.VALUE IN ('2', '3') THEN trx.AMOUNT ELSE 0 END) AS retencion_2dias,  \
            SUM(CASE WHEN cfvtr.VALUE = 'N' THEN trx.AMOUNT ELSE 0 END) AS retencion_ndias \
        FROM  \
            savingstransaction trx \
            INNER JOIN savingsaccount cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY  \
            LEFT JOIN customfieldvalue cfvtr ON cfvtr.PARENTKEY = trx.ENCODEDKEY  \
        WHERE  \
            cfvtr.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_tiporetencion'] + "' AND \
            trx.TRANSACTIONID <= (SELECT MAX(trx2.TRANSACTIONID)  \
                                  FROM savingstransaction trx2  \
                                  INNER JOIN customfieldvalue cfv2 ON cfv2.PARENTKEY = trx2.ENCODEDKEY  \
                                  WHERE cfv2.VALUE = '" + accountant_date + "') \
        GROUP BY cta.ID \
    ) AS ret ON ret.nro_cta = cta.ID \
WHERE  \
    prod.ID IN (" + GLUE_BANCOESTADO['ALL_PRODUCTS'] + ")  \
    AND cta.ACCOUNTSTATE IN ('ACTIVE', 'LOCKED') \
"