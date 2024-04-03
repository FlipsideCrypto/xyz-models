# Entity Clustering

## What is Clustering?
We leverage a common Bitcoin heuristic clustering method referred to as 'common spend' to perform our entity clustering. 

In a bitcoin transaction, there are inputs and outputs. Outputs (or the addresses that a transaction sends to) can be owned by anyone or any protocol; however, inputs (or the addresses that are sending bitcoin) are required to sign a transaction at the exact same time in order for the transaction to succeed. This means that every input address, within the same transaction, is almost certainly owned by the same entity in order to sign a transaction at the same time. 

We cluster these input addresses together, and then do a network map across every transaction to try to merge as many of these clusters together as possible. 
The end result is a fully clustered Bitcoin Network! 

## How is clustering applied??
Clustering helps make a bitcoin transaction more readable and understandable. 

We leverage our clusters on top of our transactions table to merge clustered addresses together. Instead of a transaction being: addresses 1,2,3,4,5 sent to addresses 6,7,8; the transaction will be cluster 1 sent to cluster 2. With the addition of our labelling process, these clusters will have entity names - allowing for a nicer transfers table. 



