# Immutable BTree Index


This is a modified version of a Btree that uses copy on write semantics over the paths of the tree when modifying the leaves. Each batch of modifications (for performance and storage optimization sake) generates a new root for the entire tree and copies of all the nodes involved in a transaction are only copied one time – all the remaining updates are updates-in-place (mutable). Every transaction runs in a single thread allowing the system to record a pointer for every root representing the history (versioning) of the BTree. A tuple composed by the timestamp and the pointer to the root is inserted in a separated Btree called history. This approach allows non blocking reads in the data structure (snapshot isolation) and versioning tracking: 

![alt text](imgs/index.jpg "Title")	 