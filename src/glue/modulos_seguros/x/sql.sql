select
  contratos.*,
  '${PROCESS_ID}' as process_id
from contratos
left join clientes on clientes.id_cliente = contratos.id_cliente
