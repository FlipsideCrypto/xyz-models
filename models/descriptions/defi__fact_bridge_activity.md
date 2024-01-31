
{% docs bridge_platform %}

The platform or protocol from which the bridge transaction or event originates.

{% enddocs %}

{% docs bridge_sender %}

The address that initiated the bridge deposit or transfer. This address is the sender of the tokens/assets being bridged to the destination chain.

{% enddocs %}

{% docs bridge_receiver %}

The designated address set to receive the bridged tokens on the target chain after the completion of the bridge transaction. For non-evm chains, the hex address is decoded/encoded to match the data format of the destination chain, where possible.

{% enddocs %}

{% docs destination_chain %}

The name of the blockchain network to which the assets are being bridged.

{% enddocs %}

{% docs destination_chain_id %}

The numeric identifier associated with the destination blockchain network. This is specific to the chain and helps in uniquely identifying it.

{% enddocs %}

{% docs bridge_address %}

The address of the contract responsible for handling the bridge deposit or transfer. This contract mediates the transfer and ensures that assets are sent and received appropriately.

{% enddocs %}


{% docs bridge_token_address %}

The address associated with the token that is being bridged. It provides a unique identifier for the token within its origin blockchain.

{% enddocs %}

{% docs source_chain_id %}

The numeric identifier associated with the source blockchain network. This is specific to the chain and helps in uniquely identifying it.

{% enddocs %}

{% docs source_chain %}

The name of the blockchain network from which the assets are being bridged.

{% enddocs %}
