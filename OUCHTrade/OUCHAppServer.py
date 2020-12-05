#!/usr/bin/env python3

import sys
from datetime import datetime
import select
import threading

from OUCHTrade.OUCHSocketHandler import OUCHSocketHandler
from OUCHTrade.OUCHParser import OUCHParser


class OUCHAppServer:

    def __init__(self, host, port):
        self.ouch_server_sock = OUCHSocketHandler()
        self.host = host
        self.port = port
        self.socket_list = []
        self.clients_dict = {}
        self.heartbeat_frequency = 1.0

    def pad_bytes(self, message_tag):
        if message_tag[3] == "alpha":
            message_length = message_tag[2]
            padded_message = message_tag[4].rjust(message_length)
            return bytes(padded_message, encoding='utf-8')
        elif message_tag[3] == "integer":
            message_length = message_tag[2]
            int_value: int
            int_value = message_tag[4]
            padded_message = int_value.to_bytes(message_length, byteorder='big')
            return padded_message

    def create_login_response(self, client_dict):

        login_list = []

        login_list.append(("packet_type", 2, 1, "alpha", "A"))
        login_list.append(("session", 3, 10, "alpha", client_dict["requested_session"]))
        login_list.append(("sequence_number", 13, 20, "alpha", str(client_dict["current_seq_num"]).rjust(20)))

        login_response = b''
        for login_tag in login_list:
            login_response = login_response + self.pad_bytes(login_tag)

        packet_length = len(login_response)
        login_response = packet_length.to_bytes(2, byteorder='big') + login_response

        client_dict["current_seq_num"] += 1

        return login_response

    def create_heartbeat_message(self, client_dict):
        message_list = []

        message_list.append(("packet_type", 2, 1, "alpha", "H"))

        message = b''
        for message_tag in message_list:
            message = message + self.pad_bytes(message_tag)

        packet_length = len(message)
        message = packet_length.to_bytes(2, byteorder='big') + message

        client_dict["current_seq_num"] += 1

        return message

    def create_new_order_ack(self, client_dict, new_order_dict):
        message_list = []

        message_list.append(("packet_type", 2, 1, "alpha", "S"))
        message_list.append(("message_type", 0, 1, "alpha", "A"))
        message_list.append(("timestamp", 1, 8, "integer", self.getSendingTime()))
        message_list.append(("order_token", 9, 4, "integer", new_order_dict["order_token"]))
        message_list.append(("client_reference", 13, 10, "alpha", new_order_dict["client_reference"]))
        message_list.append(("buy_sell_indicator", 23, 1, "alpha", new_order_dict["buy_sell_indicator"]))
        message_list.append(("quantity", 24, 4, "integer", new_order_dict["quantity"]))
        message_list.append(("orderbook_id", 28, 4, "integer", new_order_dict["orderbook_id"]))
        message_list.append(("group", 32, 4, "alpha", new_order_dict["group"]))
        message_list.append(("price", 36, 4, "integer", new_order_dict["price"]))
        message_list.append(("time_in_force", 40, 4, "integer", new_order_dict["time_in_force"]))
        message_list.append(("firm_id", 44, 4, "integer", new_order_dict["firm_id"]))
        message_list.append(("display", 48, 1, "alpha", new_order_dict["display"]))
        message_list.append(("capacity", 49, 1, "alpha", new_order_dict["capacity"]))
        message_list.append(("order_number", 50, 8, "integer", client_dict["current_seq_num"]))
        message_list.append(("minimum_quantity", 58, 4, "integer", new_order_dict["minimum_quantity"]))
        message_list.append(("order_state", 62, 1, "alpha", "L"))
        message_list.append(("order_classification", 63, 1, "alpha", new_order_dict["order_classification"]))
        message_list.append(("cash_margin_type", 61, 1, "alpha", new_order_dict["cash_margin_type"]))

        message = b''
        for message_tag in message_list:
            message = message + self.pad_bytes(message_tag)

        packet_length = len(message)
        message = packet_length.to_bytes(2, byteorder='big') + message

        client_dict["current_seq_num"] += 1

        return message

    def create_replace_ack(self, client_dict, replace_request_dict):
        message_list = []

        message_list.append(("packet_type", 2, 1, "alpha", "S"))
        message_list.append(("message_type", 0, 1, "alpha", "U"))
        message_list.append(("timestamp", 1, 8, "integer", self.getSendingTime()))
        message_list.append(
            ("replacement_order_token", 9, 4, "integer", replace_request_dict["replacement_order_token"]))
        message_list.append(("buy_sell_indicator", 13, 1, "alpha", " "))
        message_list.append(("quantity", 14, 4, "integer", replace_request_dict["quantity"]))
        message_list.append(("orderbook_id", 18, 4, "integer", 0))
        message_list.append(("group", 22, 4, "alpha", " "))
        message_list.append(("price", 26, 4, "integer", replace_request_dict["price"]))
        message_list.append(("time_in_force", 30, 4, "integer", replace_request_dict["time_in_force"]))
        message_list.append(("display", 34, 1, "alpha", replace_request_dict["display"]))
        message_list.append(("order_number", 35, 8, "integer", client_dict["current_seq_num"]))
        message_list.append(("minimum_quantity", 43, 4, "integer", replace_request_dict["minimum_quantity"]))
        message_list.append(("order_state", 47, 1, "alpha", "L"))
        message_list.append(("previous_order_token", 48, 4, "integer", replace_request_dict["existing_order_token"]))

        message = b''
        for message_tag in message_list:
            message = message + self.pad_bytes(message_tag)

        packet_length = len(message)
        message = packet_length.to_bytes(2, byteorder='big') + message

        client_dict["current_seq_num"] += 1

        return message

    def create_cancel_ack(self, client_dict, cancel_request_dict):
        message_list = []

        message_list.append(("packet_type", 2, 1, "alpha", "S"))
        message_list.append(("message_type", 0, 1, "alpha", "C"))
        message_list.append(("timestamp", 1, 8, "integer", self.getSendingTime()))
        message_list.append(("order_token", 9, 4, "integer", cancel_request_dict["order_token"]))
        message_list.append(("decrement_quantity", 13, 4, "integer", cancel_request_dict["quantity"]))
        message_list.append(("order_canceled_reason", 17, 1, "alpha", "U"))

        message = b''
        for message_tag in message_list:
            message = message + self.pad_bytes(message_tag)

        packet_length = len(message)
        message = packet_length.to_bytes(2, byteorder='big') + message

        client_dict["current_seq_num"] += 1

        return message

    def getSendingTime(self):
        now = datetime.now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        nanoseconds = (now - midnight).microseconds * 1000
        return nanoseconds

    def start_sending_heartbeats(self, ouch_client_sock, client_dict):

        heartbeat_thread = threading.Timer(self.heartbeat_frequency, self.start_sending_heartbeats,
                                           [ouch_client_sock, client_dict])
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        heartbeat = self.create_heartbeat_message(client_dict)
        ouch_client_sock.send(heartbeat)
        print(f"Sending Heartbeat to {ouch_client_sock.sock.getpeername()}: {OUCHParser.parse_ouch_bytes(heartbeat)} ")

    def start(self):

        # Listen for connections from OUCH Client
        self.ouch_server_sock.listen(self.host, self.port)
        self.socket_list.append(self.ouch_server_sock.sock)

        try:
            # Check for incoming messages
            while True:

                read_sockets, _, exception_sockets = select.select(self.socket_list, [], self.socket_list, 0)

                for notified_socket in read_sockets:
                    # If notified socket is a server socket - new connection, accept it
                    if notified_socket == self.ouch_server_sock.sock:
                        # Accept new connection
                        client_socket, client_address = self.ouch_server_sock.sock.accept()
                        print(f"Accepting new connection from {client_address}")
                        self.socket_list.append(client_socket)
                    else:
                        # Receive messages from client
                        ouch_client_sock = OUCHSocketHandler(notified_socket)
                        received_messages = ouch_client_sock.receive()

                        for received_message in received_messages:

                            ouch_dict = OUCHParser.parse_ouch_bytes(received_message)

                            if ouch_dict["packet_type"] == "L":
                                # Found a login request, send a login response
                                print("Received Login Request")
                                requested_session = ouch_dict["requested_session"]

                                self.clients_dict = {
                                                    notified_socket:
                                                    {
                                                        "requested_session": requested_session,
                                                        "current_seq_num": 1
                                                    }
                                }

                                login_response = self.create_login_response(self.clients_dict[notified_socket])
                                ouch_client_sock.send(login_response)
                                print("Sent Login Response")
                                # Start sending Heartbeats
                                self.start_sending_heartbeats(ouch_client_sock, self.clients_dict[notified_socket])
                            elif ouch_dict["packet_type"] == "U":
                                if "message_type" in ouch_dict:
                                    if ouch_dict["message_type"] == "O":
                                        # Found a new order, send a new order ack
                                        print("Received New Order:" + str(ouch_dict))
                                        client_dict = self.clients_dict[notified_socket]
                                        new_order_ack = self.create_new_order_ack(client_dict, ouch_dict)
                                        ouch_client_sock.send(new_order_ack)
                                        print(f"Sending New Order Ack to {ouch_client_sock.sock.getpeername()}: {OUCHParser.parse_ouch_bytes(new_order_ack)} ")
                                    elif ouch_dict["message_type"] == "U":
                                        # Found a replace order, send a replace result
                                        print("Received Replace Order:" + str(ouch_dict))
                                        client_dict = self.clients_dict[notified_socket]
                                        replace_result = self.create_replace_ack(client_dict, ouch_dict)
                                        ouch_client_sock.send(replace_result)
                                        print(f"Sending Order Replaced to {ouch_client_sock.sock.getpeername()}: {OUCHParser.parse_ouch_bytes(replace_result)} ")
                                    elif ouch_dict["message_type"] == "X":
                                        # Found a cancel order, send a cancel result
                                        print("Received Cancel Order:" + str(ouch_dict))
                                        client_dict = self.clients_dict[notified_socket]
                                        cancel_result = self.create_cancel_ack(client_dict, ouch_dict)
                                        ouch_client_sock.send(cancel_result)
                                        print(f"Sending Order Canceled to {ouch_client_sock.sock.getpeername()}: {OUCHParser.parse_ouch_bytes(cancel_result)} ")

        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")
        finally:
            for sock in self.socket_list:
                sock.close()


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("usage:", sys.argv[0], "<host> <port>")
        sys.exit(1)

    main_host, main_port = sys.argv[1], int(sys.argv[2])

    ouch_app_server = OUCHAppServer(main_host, main_port)

    # Start the OUCH Server
    ouch_app_server.start()
