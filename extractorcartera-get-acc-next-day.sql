SELECT 
        cta.ID AS numcta, 
        SUM(trx.AMOUNT) AS amount, 
        prod.ID AS producto 
    FROM 
        savingstransaction AS trx 
        LEFT JOIN savingsaccount AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY 
        LEFT JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY 
        LEFT JOIN customfieldvalue AS cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '8a81868f734d3e7001734f990b7122c5' 
    WHERE 
        ( 
            cfv.VALUE = '2024-04-09' AND trx.TRANSACTIONID <= 2680854 
        ) 
        AND prod.ID IN ('CCV00002') 
        AND cta.ACCOUNTSTATE IN ('ACTIVE', 'LOCKED') 
        AND cta.ID NOT IN ( 
            SELECT 
                cta.ID 
            FROM 
                savingstransaction AS trx 
                INNER JOIN customfieldvalue AS cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '8a81868f734d3e7001734f990b7122c5' 
                LEFT JOIN savingsaccount AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY 
                LEFT JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY 
            WHERE 
                prod.ID IN ('CCV00002') 
                AND cta.ACCOUNTSTATE IN ('ACTIVE', 'LOCKED') 
                AND cfv.VALUE = '2024-04-08' 
            ) 
        GROUP BY cta.ID