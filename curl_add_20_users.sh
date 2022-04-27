#!/bin/bash


curl -s -X POST -d 'testuser1'  http://127.0.0.1:8080/api/add-user && echo "done1" &
curl -s -X POST -d 'testuser2'  http://127.0.0.1:8080/api/add-user && echo "done2" &
curl -s -X POST -d 'testuser3'  http://127.0.0.1:8080/api/add-user && echo "done3" &
curl -s -X POST -d 'testuser4'  http://127.0.0.1:8080/api/add-user && echo "done4" &
curl -s -X POST -d 'testuser5'  http://127.0.0.1:8080/api/add-user && echo "done5" &
curl -s -X POST -d 'testuser6'  http://127.0.0.1:8080/api/add-user && echo "done6" &
curl -s -X POST -d 'testuser7'  http://127.0.0.1:8080/api/add-user && echo "done7" &
curl -s -X POST -d 'testuser8'  http://127.0.0.1:8080/api/add-user && echo "done8" &
curl -s -X POST -d 'testuser9'  http://127.0.0.1:8080/api/add-user && echo "done9" &
curl -s -X POST -d 'testuser10'  http://127.0.0.1:8080/api/add-user && echo "done10" &
curl -s -X POST -d 'testuser11'  http://127.0.0.1:8080/api/add-user && echo "done11" &
curl -s -X POST -d 'testuser12'  http://127.0.0.1:8080/api/add-user && echo "done12" &
curl -s -X POST -d 'testuser13'  http://127.0.0.1:8080/api/add-user && echo "done13" &
curl -s -X POST -d 'testuser14'  http://127.0.0.1:8080/api/add-user && echo "done14" &
curl -s -X POST -d 'testuser15'  http://127.0.0.1:8080/api/add-user && echo "done15" &
curl -s -X POST -d 'testuser16'  http://127.0.0.1:8080/api/add-user && echo "done16" &
curl -s -X POST -d 'testuser17'  http://127.0.0.1:8080/api/add-user && echo "done17" &
curl -s -X POST -d 'testuser18'  http://127.0.0.1:8080/api/add-user && echo "done18" &
curl -s -X POST -d 'testuser19'  http://127.0.0.1:8080/api/add-user && echo "done19" &
curl -s -X POST -d 'testuser20'  http://127.0.0.1:8080/api/add-user && echo "done20" &

wait