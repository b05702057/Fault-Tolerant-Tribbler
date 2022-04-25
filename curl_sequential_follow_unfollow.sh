#!/bin/bash

# Note this is sequential

curl -s -X POST -d '{"who":"rkapoor","whom":"fenglu"}'  http://127.0.0.1:8080/api/follow && echo "seq_done1"

curl -s -X POST -d '{"who":"rkapoor","whom":"fenglu"}'  http://127.0.0.1:8080/api/unfollow && echo "seq_done2"

wait