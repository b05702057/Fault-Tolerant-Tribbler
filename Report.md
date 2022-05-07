# Report

## Bin Storage:

For Bin Storage, I utilized the Lab1 servers as our backend servers. Since the frontend doesn't know the difference between a bin and a backend server, I also defined a new sturct to wrap the storage and the name of the bin. With this struct, I can reimplement the functions to hide the prefix tricks from both backends and frontends.

As for deciding which backend server a bin belongs to, I used a hash function to hash the string as a number and modded the number to get a backend ID. With this implementation, the bins would be distributed more evenly, and a single backend wouldn't be pressured to much.

## Tribbler:

For the sign_up function, I used the set function to set "signup_username" as "T". In this way, I can use the get function to check if a user has already signed up or not.

For the list_users function, I used the keys function to get all keys with prefixes "signup_". I can then get all users after striping the prefix. To make this function more efficient, I also maintain a cache at a specific bin. This cache will store much fewer usernames, and we can use it directly if its size is 20.

For the post function, I used the list_append function to append the post to the poster's bin.

For the tribs function, I used the list_get function to get the posts of the user. After I sort the tribs, I also do garbage collection to make sure that there are no more than 100 tribs for a user.

For the follow function, I first used the list_append function to append "clock::follow::username" to the log. Then I created a hashmap to maintain the followees of the follower. When we go through the log entry, we would add the followee if it is not in the hashmap when we meet a follow record and remove the followee if it is in the hashmap when we meet an unfollow record. We also check the map size before we add a followee to meet the followee number constraint. Then, when we meet our own log, we return true if the map is not too big and the followee is not in the map. Otherwise, we would return false.

For the unfollow function, the operation is similar to the follow function. The difference is that we return true when the followee is in the map when we meet our own log. Otherwise, we return false.

For the following function, I ran through the log like the above functions and converted the hashmap to a vector to return.

For the is_follow function, I simply called the following function and check if the followee is in the vector.

For the home function, I used list_get to get all the tribs of the user and his followees. Then, I sorted the tribs and get the most recent 100 tribs to return.


## fault tolerance

To make the system fault tolerant, we use the mechanism similar to the Chord file system. Basically, we hash all usernames, keepers, and backends on the ring. When a node joins, the distribution would change for load balancing. When a node crashes, the distribution would also change for fault tolerance. 

## crashes

When a backend crashes, the responsible keeper would copy its data to other backends. It's possible that the keeper than dies, so the keeper would send the messages to the next responsible keeper. Thus, the next keeper would be able to help when the first one dies. It's also possible that the backend dies with the keeper concurrently, so the next responsible keeper would also scan the predecessor's backend to address this case.

## join

When a new backend joins, the keeper should migrate some data to it. This is not only for load balancing, but also for the distribution rule for CFS. If don't do this migration. It would be hard for us to find the correct location for data storage.

## starting phase

The starting phase is different from a normal join, and the keepers should be able to distingusih them. What we do is to let keepers send heatbeat to other keepers during initialization and read the message in the acknowledgement. Based on the acknowledgement, the keeper would make different behaviors.
