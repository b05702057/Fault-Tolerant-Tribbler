#!/bin/bash


curl -s -X POST -d '{"who":"rkapoor","whom":"fenglu"}'  http://127.0.0.1:8080/api/unfollow && echo "done1" &

wait