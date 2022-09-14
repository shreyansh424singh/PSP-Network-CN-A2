import hashlib
import random
import threading

from collections import OrderedDict


class LRUCache:

 # initialising capacity
 def __init__(self, capacity: int):
  self.cache = OrderedDict()
  self.capacity = capacity

 def get(self, key: int) -> int:
  if key not in self.cache:
   return -1
  else:
   self.cache.move_to_end(key)
   return self.cache[key]

 def put(self, key: int, value: int) -> None:
  self.cache[key] = value
  self.cache.move_to_end(key)
  if len(self.cache) > self.capacity:
   self.cache.popitem(last = False)


# RUNNER
# initializing our cache with the capacity of 2
cache = LRUCache(1)
def main():
    global cache
    cache = LRUCache(2)

# main()
cache.put(1, 1)
print(cache.cache)
cache.put(2, 2)
print(cache.cache)
cache.get(1)
print(cache.cache)
cache.put(3, 3)
print(cache.cache)
cache.get(2)
print(cache.cache)
cache.put(4, 4)
print(cache.cache)
cache.get(1)
print(cache.cache)
cache.get(3)
print(cache.cache)
cache.get(4)
print(cache.cache)

#This code was contributed by Sachin Negi


# data = []

# def set():
#     global data

#     data[3][1]="hello1"
#     data[2][1]="hello2"
#     data[4][1]="hello3"
#     data[1][4]="hello4"
#     data[2][4]="hello5"
#     data[3][4]="hello6"

# def main():
#     global data
#     data = [dict() for x in range(10)]
#     print(data)
#     set()
#     print(data)

# main()


# def out():
#     x = ()
#     y=5
#     print("FCDS")
#     def inn():
#         nonlocal x
#         x = ("Xes", y)
#         print("jiji")
#     inn()
#     print(x)
# out()

# def rand():
#     x = 0
#     while x != 10 : 
#         x = random.randint(1, 10)
#         print(x)
# rand()


# data = []

# def read_file(f):
#     while True:
#         data = f.read(1024)
#         if not data:
#             break
#         yield data

# with open("./A2_small_file.txt", 'r') as f:
#     for piece in read_file(f):
#         data.append(hashlib.md5(piece.encode()).hexdigest())

# print(data)   

# def fetch1(index):
#     print(f"2nd level thread {index}")

# def fetch(index):
#     print(f"1st level thread {index}")
#     y = threading.Thread(target=fetch1, args=(index*index,))
#     y.start()
#     y = threading.Thread(target=fetch1, args=(index*index*index,))
#     y.start()

# for i in range(3):   # for connection 1
#     x = threading.Thread(target=fetch, args=(i,))
#     # threads.append(x)
#     x.start()