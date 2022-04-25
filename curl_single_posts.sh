#!/bin/bash

# Just posting a bunch

curl -s -X POST -d '{"who":"testuser","message":"a single post","clock":3}'  http://localhost:8080/api/post && echo "done1" &

wait