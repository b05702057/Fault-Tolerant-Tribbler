# Report

## Overview
As mentioned in the lab 2 writeup, the system consists of the BinStorage
and Tribbler components. The design choices largely lie on the Tribbler side 
in terms of how to use BinStorage, since the BinStorage component provides very
specific (and restrictive) interfaces.

## Bin Storage
The bin storage client is created by simply creating a bin client that can 
return a new client StorageClientMapperWrapper struct, which essentially is
a wrapper around lab1's client, for each bin() call. This wrapper client will 
be created with the bin name and uses this to prefix key inputs before passing
them to the appropriate lab1's client functions to provide logically separate 
virtual key value stores for each bin name. This bin name prefix is escaped
from any colons and the "::" separator is thus used to separate the bin name
portion from the actual key when stored in the key value store. The backend
that the client will connect to is chosen by hashing the bin name for load
balancing.

The keeper simply polls the backends using clock() every second and polls them
again with the largest clock value seen across all backends to keep them 
roughly synchronized. It also uses a lab1's client to make this clock RPC.

The bin storage backends are just servers started by lab1's serve_back().

## Tribbler
The AJAX handling logic is already provided in lab2. These http requests are 
able to be served by the tribbler front ends by implementing the Server trait. 

In some cases, the tribbler front end uses the BinStorage to appending 
lists/logs of actions. In other cases, it uses specific keys for a bin as the
commit point (e.g. signing up).

Here are some main points regarding the design choices:
1. Users are mapped to the bin whose name is the username for most 
   operations. This allows a user's own actions to have sequential properties
   consistent with the sequential requests by the user (not necessarily true
   or needed for concurrent posts).
2. The user sign up commit point is also at the user's bin and at a particular
   known key. This allows easy check of whether the user is signed up.
3. Registratrion does extra work at a predefined early_registration_bin for up
   to 20 users. These users can be cached on the frontends avoid repeated calls
   to this particular bin. A temporary early registration list entry is created 
   with a commit status of "uncertain" before committing sign up at the commit 
   point; this is so that if the front end crash here before it can update the 
   temporary entry commit status, a future evaluation can still look up the 
   commit point to figure out the true state.
4. For posts/tribs, a simple per user list (in their bin) is sufficient to
   meet the requirements, as long as they are sorted by the Tribble Order and
   appropriately given a logical clock from calling clock() on their bin.
5. For follow/unfollow actions, a log of these attempted actions is kept per 
   user. The log is consulted and resolved (e.g. ignore outdated actions and 
   only evaluate the most recent one) to figure out the final following list of
   a user. For following logs, the order in the log itself determines the order
   of events used by the front ends i.e. the backend is used as a serialization
   point. Thus concurrent follows on the same user for example can be checked by 
   comparing the state of the log before and after an append to see if that 
   append was beaten by any races.
5. There is occasional garbage collection of tribs and follow logs.
6. There are other design choices not mentioned here and more explanation 
   on them as well as the ones above and how they should meet the requirements
   commented on the TribFront struct in the lab/src/lab2/trib_front.rs file if
   you wish to explore this further.
