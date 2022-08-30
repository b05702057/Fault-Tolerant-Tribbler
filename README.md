# Fault-Tolerant-Tribbler

### A fault-tolerant Twitter-like system that can handle server crashes without losing any data 

## Task
Utilized the Chord hashing mechanism to store data on several backend servers, replicating data on new servers
when old servers crash, and migrating data to recovered servers for load balancing

### Run Locally
* Run this Command: cargo run --bin kv-server
* Run this Command: cargo run --bin kv-client

### Note
* Rust can be downloaded here: https://rustup.rs.
* The detailed tasks are recorded here: https://cseweb.ucsd.edu/classes/sp22/cse223B-a/tribbler/lab/lab3/index.html.
