import array
import json
import socket


def send_msg(sock, msg, *args):
    msg = json.dumps(msg) + "\r\n"
    try:
        sock.sendmsg([msg.encode()], *args)
    except IOError as e:
        print(e)
        print(sock)


def recv_msg(sock, maxfds=10):
    fds = array.array("i")
    msglen = 4096
    msg, ancdata, flags, addr = sock.recvmsg(msglen,
                                             socket.CMSG_LEN(maxfds * fds.itemsize))
    for cmsg_level, cmsg_type, cmsg_data in ancdata:
        if (cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS):
            # Append data, ignoring any truncated integers at the end.
            fds.fromstring(
                cmsg_data[:len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
    return msg, list(fds)
