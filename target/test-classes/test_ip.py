import struct
import socket
long_ip = struct.unpack("!I", socket.inet_aton("123.126.114.194"))[0]
print long_ip