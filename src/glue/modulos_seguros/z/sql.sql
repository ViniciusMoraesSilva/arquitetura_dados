select
  contratos.id_contrato,
  parcelas.id_parcela,
  '${PROCESS_ID}' as process_id
from contratos
left join parcelas on parcelas.id_contrato = contratos.id_contrato
