#!/bin/bash


curl -s -X POST -d 'testuser'  http://127.0.0.1:8080/api/add-user && echo "done1" &

wait