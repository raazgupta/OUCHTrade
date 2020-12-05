#!/usr/bin/env python3

import socket
import select
import sys


class OUCHSocketHandler:
    sock: socket.socket
    _packet_length: int
    _check_sum_length: int
    _max_potential_message: int

    def __init__(self, sock=None):
        if sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        else:
            self.sock = sock
        self._packet_length = 2
        self._check_sum_length = 7
        self._max_potential_message = 2048

    def connect(self, host, port):
        print("starting connection to", (host, port))
        self.sock.connect((host, port))
        self.sock.setblocking(False)

    def listen(self, host, port):
        self.sock.bind((host, port))
        self.sock.listen()
        print(f'Listening for connection on {host}:{port}...')

    def close(self):
        # print("Closing connection")
        # self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()

    def send(self, message):

        # Is the socket ready to send data?
        _, write_sockets, exception_sockets = select.select([], [self.sock], [self.sock], 0)

        if self.sock in exception_sockets:
            # Check if socket in error state
            raise RuntimeError("socket connection broken")
        elif self.sock in write_sockets:
            # Socket is ready to send data
            total_sent = 0
            while total_sent < len(message):
                try:
                    sent = self.sock.send(message[total_sent:])
                    if sent == 0:
                        raise RuntimeError("socket connection broken")
                except Exception as e:
                    print('Writing error: '.format(str(e)))
                    sys.exit()
                total_sent += sent
            return True
        else:
            print("Unable to send data")
            return False

    def receive(self):
        chunks = []
        received_messages = []

        # Check if the socket has any data to read
        while True:

            try:
                read_sockets, _, exception_sockets = select.select([self.sock], [], [self.sock], 0)

                if self.sock in exception_sockets:
                    # Check if socket in error state
                    raise RuntimeError("socket connection broken")
                elif not read_sockets:
                    # Socket does not have any data left to read
                    return received_messages
                elif self.sock in read_sockets:
                    # Socket has data left to read

                    packet_length_bytes = self.sock.recv(self._packet_length)
                    if packet_length_bytes == b'':
                        return received_messages
                    # Determine the length of the OUCH message
                    packet_length_int = int.from_bytes(packet_length_bytes, byteorder='big')
                    chunks.append(packet_length_bytes)

                    # Receive the packet type and remaining payload
                    body_bytes = self.sock.recv(packet_length_int)
                    chunks.append(body_bytes)

                    received_messages.append(b''.join(chunks))
                    chunks = []

            except Exception as e:
                print('Reading error: '.format(str(e)))
                sys.exit()
