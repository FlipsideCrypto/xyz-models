{% docs tx_id %}

The transaction id. Not this differs from the transaction id for witness transactions.
When the tx is a segwit tx, the calculation of hash does not include the witness data, whereas the txid does.

(Read more)[https://bitcoin.stackexchange.com/questions/77699/whats-the-difference-between-txid-and-hash-getrawtransaction-bitcoind]

{% enddocs %}
