# Report

## Bin Storage
Besides the bin abstraction implemented in lab 2, fault tolerance is achieved by coordinating key translation
between bin clients and keepers. An approach similar to CFS was used to help keepers and servers share similar 
views of the live backends and thus could go to the appropriate ones for operations. These views are 
periodically scanned and updated so that they will have the same view shortly enough upon an event. Then, it is simple
for keepers and clients to agree on which bins should go where by simply applying a hash and choosing the 
next 2 closest live backends.

Additionally, some properties of the lists appended to are guaranteed by making bin clients follow certain algorithms
when appending or fetching from stores. For example, it would always write to 2 backends for each operation and use the 
the notion of an "index" in the "total log" in one of them to have serialization order. This "total log" is the view 
created by concatenating a "prefix log" and "suffix log", which is per key, per replica. In normal operation, there would be
2 replicas for any key. Each "total log" in a replica can be "resolved" to the same state, and thus either is sufficient 
for operations serving. This is achieved by encoding the index in the entry in the "backup" which can not use the backend 
storage as the serialization point since it may differ from the corresponding entries in the "primary"; as well as applying
deduplication on events. This primary and backup view may change depending on the conditions to allow for availability.
Client operations always go to the suffix log and keeper migration/replication will transfer the "total log" from the source 
to the prefix log at the destination. This allows the clients to freely serve requests by writing to the suffix log even if 
migration is in progress to that backend. The timing is done such that the client and keeper are guaranteed to not have missing 
information at the new replica -- clients immediately write to the new replica once their view is refreshed and the keeper's
wait time ensures this is the case. Of course this would lead to possible overlapping of new operations between the prefix 
and suffix log (hence their names), but these are easily deduplicable, since we tag each entries with the clock and primary id to
uniquely identify them. Thanks to these properties idemptotence is achieved on the resolved state of the log, which would be the
same for any replica. There are rules to guarantee deterministic choices to prevent races such as by comparing the resolved log 
state of the replicas to select the "primary" in for the serialization order. 

Keepers communicate with each other (through keeper RPCs) to distributed the backends range they manage in a similar way to how 
bins are mapped to backends. Each keeper also monitors their predecessor's region as well to take over in case it dies while 
handling a backend event. The keeper view scan is simultaneously a clock sync on backends, and keepers share information to 
sync all backends. Keepers do replication to maintain 2 copies of data at all time before the next event occurs.

## Tribbler front end
Tribbler uses per user bins for storege operations. It stores user actions mainly in logs and use them as serialization order 
or encode logical timestamps (such as for tribs) for sorting later on. It holds the bin client described above to 
do these operations. Lab3's tribbler is largely unchanged from lab2.
