/*query_temp_1 - Leitura da tabela tabela_contrato*/
SELECT
    *
FROM
    tabela_contrato
WHERE
    vlr_cred_mes > 100;

/*query_temp_2 - Leitura da tabela tabela_contrato*/
SELECT
    num_contrato,
    vlr_cred_mes,
    data_base
FROM
    query_temp_1;