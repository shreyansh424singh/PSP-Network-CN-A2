import hashlib

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