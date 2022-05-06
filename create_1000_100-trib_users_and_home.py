# import requests
# import time

# NUM_USERS = 1000
# NUM_POSTS_PER_USER = 100

# # Create users
# for user_idx in range(NUM_USERS):
#     username = "testuser" + str(user_idx)
#     payload = {username: ""}
#     r = requests.post("http://localhost:8080/api/add-user", data=payload)
#     assert(r.status_code == requests.codes.ok)


# # Make who_user follow everyone.
# who_user = "h8liu"
# for user_idx in range(NUM_USERS):
#     whom_user = "testuser" + str(user_idx)    
#     field = '{"who":"' + who_user + '","whom":"' + whom_user + '"}'
#     payload = {field: ""}
#     r = requests.post("http://localhost:8080/api/follow", data=payload)
#     assert(r.status_code == requests.codes.ok)


# # Do 100 posts for each user
# post_start_time = time.time()
# for user_idx in range(NUM_USERS):
#     username = "testuser" + str(user_idx)
#     for post_idx in range(NUM_POSTS_PER_USER):
#         print("looping user_idx: " + str(user_idx) + ", post_idx: " + str(post_idx))
#         post_str = "post number " + str(post_idx) + "."
#         field = '{"who":"' + username + '","message":"' + post_str + '","clock":4}'
#         payload = {field: ""}
#         r = requests.post("http://localhost:8080/api/post", data=payload)
#         assert(r.status_code == requests.codes.ok)
# post_end_time = time.time()

# # Then call home() on who_user
# payload = {who_user: ""}

# start_time = time.time()
# r = requests.post("http://localhost:8080/api/list-home", data=payload)
# end_time = time.time()
# print(r.text)
# print("\nTime taken for home() (seconds):")
# print(end_time - start_time)


# print("\nConcurrent posts time is:")
# print(post_end_time - post_start_time)



######################## TESTING ########################

import concurrent.futures
import requests
import time
import sys

NUM_USERS = 1000
NUM_POSTS_PER_USER = 100

# Create users
for user_idx in range(NUM_USERS):
    username = "testuser" + str(user_idx)
    payload = {username: ""}
    r = requests.post("http://localhost:8080/api/add-user", data=payload)
    assert(r.status_code == requests.codes.ok)


# Make who_user follow everyone.
who_user = "h8liu"
for user_idx in range(NUM_USERS):
    whom_user = "testuser" + str(user_idx)    
    field = '{"who":"' + who_user + '","whom":"' + whom_user + '"}'
    payload = {field: ""}
    r = requests.post("http://localhost:8080/api/follow", data=payload)
    assert(r.status_code == requests.codes.ok)


NUM_CONNECTIONS = 100
MAX_TRIB_LEN = 140
FILLER_STR = "FILLER..."

def post_for_user(user_idx, post_idx):
    username = "testuser" + str(user_idx)
    print("looping user: " + str(user_idx) + ", post_idx: " + str(post_idx))
    post_str = "post number " + str(post_idx) + "."
    post_str += FILLER_STR * ((MAX_TRIB_LEN - len(post_str)) // len(FILLER_STR))  # Fill with FILLER_STR evenly as close as possible to MAX_TRIB_LEN
    field = '{"who":"' + username + '","message":"' + post_str + '","clock":4}'
    payload = {field: ""}
    r = requests.post("http://localhost:8080/api/post", data=payload)
    assert(r.status_code == requests.codes.ok)

# Do 100 posts for each user
with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_CONNECTIONS) as executor:
    futures_x = (executor.submit(post_for_user, user_idx, post_idx) for user_idx in range(NUM_USERS) for post_idx in range(NUM_POSTS_PER_USER))
            
    post_start_time = time.time()
    for future in concurrent.futures.as_completed(futures_x):
        try:
            res = future.result()
        except Exception as exc:
            print("Exception!!")
            res = str(type(exc))
            print(res)
            sys.exit(1)

    post_end_time = time.time()

        
# Then call home() on who_user
payload = {who_user: ""}

start_time = time.time()
r = requests.post("http://localhost:8080/api/list-home", data=payload)
end_time = time.time()
print(r.text)
print("\nTime taken for home() (seconds):")
print(end_time - start_time)


print("\nConcurrent posts time is:")
print(post_end_time - post_start_time)
