#!/bin/bash

# Concurrent requests to test race condition

curl -s -X POST -d '{"who":"rkapoor","whom":"fenglu"}'  http://127.0.0.1:8080/api/unfollow && echo "done1" &
curl -s -X POST -d '{"who":"rkapoor","whom":"fenglu"}'  http://127.0.0.1:8080/api/unfollow && echo "done2" &

wait