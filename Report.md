# Report

## Bin Storage
Since it is easier to maintain data in the same bin are further migration, we hash the value with default_hasher. Every time we try to find the targeting we first extract all the keys and list keys. Also, we consider all the data in  one bin as a group and migrate these data together. To deal with concurrent operation executions, we use two list, which are prefix list and suffix list. After fetching the two log, we truncated the prefix, concatinate the two list as total list and sort total list by the index of the item that just added to primary list. 

## Keeper
We rely on keeper to sync clock and do the migration between backends when new backend joins or backend crashes. One keeper is distrubuted to be responsible for syncing and migration of a range of backends. 
