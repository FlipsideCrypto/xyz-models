-- get chainhead
select ETHEREUM.STREAMLINE.UDF_API('GET','https://rpc-evmos.imperator.co/abci_info',{},{} )

--sample gen for last 1k blocks
 create table sample_block_ids as (
 with gen as (
select
    row_number() over (
                order by
                    seq4()
            ) as block_height
from
    table(generator(rowcount => 100000000)) 
    )

   
    select top 10000 block_height from gen
    -- except select 12872988 from sample_blocks
    where block_height <= 12873408
    order by 1 desc)
    

--pull one block
select 
block_height,
ETHEREUM.STREAMLINE.UDF_JSON_RPC_CALL('https://rpc-evmos.imperator.co/',{},
    [
    {   'id': block_height,   'jsonrpc': '2.0',   'method': 'block',   'params': [  block_height::STRING  ] }
 ]
  ) data,
  getdate() as _inserted_timestamp
  from 
   (select 12889280  as block_height    )


--pull one block's transactions
select 
block_height,
ETHEREUM.STREAMLINE.UDF_JSON_RPC_CALL('https://rpc-evmos.imperator.co/',{},
    [
    {   'id': block_height,   'jsonrpc': '2.0',   'method': 'tx_search',   'params': [  'tx.height='||block_height::STRING , true, '1', '1000', 'asc'   ] }
 ]
  ) data,
  getdate() as _inserted_timestamp
  from 
    (select 12889280  as block_height    )
     