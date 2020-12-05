#!/usr/bin/env python3

import sys
import threading

from OUCHTrade.OUCHSocketHandler import OUCHSocketHandler
from OUCHTrade.OUCHParser import OUCHParser


class OUCHAppClient:

    def __init__(self, host, port, username, password, requested_sequence_number):
        self.ouch_client_sock = OUCHSocketHandler()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.send_seq_num = requested_sequence_number
        self.current_seq_num = int(requested_sequence_number)
        self.group = "DAY "
        self.time_in_force = 99999
        self.firm_id = 0
        self.order_classification = "1"
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

    def create_login_request(self):

        login_list = []

        login_list.append(("packet_type", 2, 1, "alpha", "L"))
        login_list.append(("username", 3, 6, "alpha", self.username))
        login_list.append(("password", 9, 10, "alpha", self.password))
        login_list.append(("requested_session", 19, 10, "alpha", " "))
        login_list.append(("requested_sequence_number", 29, 20, "alpha", self.send_seq_num))

        login_request = b''
        for login_tag in login_list:
            login_request = login_request + self.pad_bytes(login_tag)

        packet_length = len(login_request)
        login_request = packet_length.to_bytes(2, byteorder='big') + login_request

        self.current_seq_num += 1

        return login_request

    def create_heartbeat_message(self):
        message_list = []

        message_list.append(("packet_type", 2, 1, "alpha", "R"))

        message = b''
        for message_tag in message_list:
            message = message + self.pad_bytes(message_tag)

        packet_length = len(message)
        message = packet_length.to_bytes(2, byteorder='big') + message

        return message

    def create_new_order(self, symbol, side, quantity, price):

        message_list = []

        message_list.append(("packet_type", 2, 1, "alpha", "U"))
        message_list.append(("message_type", 0, 1, "alpha", "O"))
        message_list.append(("order_token", 1, 4, "integer", self.current_seq_num))
        message_list.append(("client_reference", 5, 10, "alpha", " "))
        message_list.append(("buy_sell_indicator", 15, 1, "alpha", side))
        message_list.append(("quantity", 16, 4, "integer", int(quantity)))
        message_list.append(("orderbook_id", 20, 4, "integer", int(symbol)))
        message_list.append(("group", 24, 4, "alpha", self.group))
        message_list.append(("price", 28, 4, "integer", int(float(price)*10)))
        message_list.append(("time_in_force", 32, 4, "integer", self.time_in_force))
        message_list.append(("firm_id", 36, 4, "integer", self.firm_id))
        message_list.append(("display", 40, 1, "alpha", " "))
        message_list.append(("capacity", 41, 1, "alpha", "A"))
        message_list.append(("minimum_quantity", 42, 4, "integer", 0))
        message_list.append(("order_classification", 46, 1, "alpha", self.order_classification))
        message_list.append(("cash_margin_type", 47, 1, "alpha", "1"))

        message = b''
        for message_tag in message_list:
            message = message + self.pad_bytes(message_tag)

        packet_length = len(message)
        message = packet_length.to_bytes(2, byteorder='big') + message

        self.current_seq_num += 1

        return message

    def create_replace_order(self, existing_order_token, quantity, price):

        message_list = []

        message_list.append(("packet_type", 2, 1, "alpha", "U"))
        message_list.append(("message_type", 0, 1, "alpha", "U"))
        message_list.append(("existing_order_token", 1, 4, "integer", int(existing_order_token)))
        message_list.append(("replacement_order_token", 5, 4, "integer", self.current_seq_num))
        message_list.append(("quantity", 9, 4, "integer", int(quantity)))
        message_list.append(("price", 13, 4, "integer", int(float(price)*10)))
        message_list.append(("time_in_force", 17, 4, "integer", self.time_in_force))
        message_list.append(("display", 21, 1, "alpha", " "))
        message_list.append(("minimum_quantity", 22, 4, "integer", 0))

        message = b''
        for message_tag in message_list:
            message = message + self.pad_bytes(message_tag)

        packet_length = len(message)
        message = packet_length.to_bytes(2, byteorder='big') + message

        self.current_seq_num += 1

        return message

    def create_cancel_order(self, order_token):

        message_list = []

        message_list.append(("packet_type", 2, 1, "alpha", "U"))
        message_list.append(("message_type", 0, 1, "alpha", "X"))
        message_list.append(("order_token", 1, 4, "integer", int(order_token)))
        message_list.append(("quantity", 5, 4, "integer", 0))

        message = b''
        for message_tag in message_list:
            message = message + self.pad_bytes(message_tag)

        packet_length = len(message)
        message = packet_length.to_bytes(2, byteorder='big') + message

        self.current_seq_num += 1

        return message

    def start_sending_heartbeats(self):

        heartbeat_thread = threading.Timer(self.heartbeat_frequency, self.start_sending_heartbeats, [])
        heartbeat_thread.daemon = True
        heartbeat_thread.start()

        heartbeat = self.create_heartbeat_message()
        self.ouch_client_sock.send(heartbeat)

    def start(self):

        # Open Connection to OUCH Server
        self.ouch_client_sock.connect(self.host, self.port)

        # Send login request
        request = self.create_login_request()
        print("Sending Login Request:" + str(OUCHParser.parse_ouch_bytes(request)))
        self.ouch_client_sock.send(request)

        # Start sending Heartbeats
        self.start_sending_heartbeats()

        try:
            while True:
                input_text = input("new / replace / cancel / get / close : ")
                input_list = input_text.split(" ")
                if input_list:
                    if input_list[0] == "get":
                        received_messages = self.ouch_client_sock.receive()
                        if not received_messages:
                            print("No received messages")
                        else:
                            for message in received_messages:
                                ouch_dict = OUCHParser.parse_ouch_bytes(message)
                                if ouch_dict["packet_type"] == "A":
                                    print("Login Accepted: " + str(ouch_dict))
                                elif ouch_dict["packet_type"] == "H":
                                    # print("Heartbeat: " + str(ouch_dict))
                                    continue
                                elif ouch_dict["packet_type"] == "S":
                                    if ouch_dict["message_type"] == "A":
                                        print(f"Order Accepted - Order Token: {ouch_dict['order_token']} {ouch_dict}")
                                    elif ouch_dict["message_type"] == "U":
                                        print(f"Order Replaced - Order Token: {ouch_dict['replacement_order_token']} {ouch_dict}")
                                    elif ouch_dict["message_type"] == "C":
                                        print(f"Order Canceled - Order Token: {ouch_dict['order_token']} {ouch_dict}")
                                    elif ouch_dict["message_type"] == "E":
                                        print(f"Filled - Order Token: {ouch_dict['order_token']} " +
                                              f"Executed Quantity: {ouch_dict['executed_quantity']} Execution Price: {ouch_dict['execution_price']} {ouch_dict}")
                                    elif ouch_dict["message_type"] == "J":
                                        print(f"Order Rejected - Order Token: {ouch_dict['order_token']} {ouch_dict}")
                                    else:
                                        print(str(ouch_dict))
                                else:
                                    print(str(ouch_dict))
                    elif input_list[0] == "new":
                        if len(input_list) == 5:
                            symbol = input_list[1]
                            side = input_list[2]
                            quantity = input_list[3]
                            price = input_list[4]
                            new_order = self.create_new_order(symbol, side, quantity, price)
                            self.ouch_client_sock.send(new_order)
                        else:
                            print("Usage: new <symbol> <side> <quantity> <price>")
                    elif input_list[0] == "replace":
                        if len(input_list) == 4:
                            existing_order_token = input_list[1]
                            quantity = input_list[2]
                            price = input_list[3]
                            amend_order = self.create_replace_order(existing_order_token, quantity, price)
                            self.ouch_client_sock.send(amend_order)
                        else:
                            print("Usage: replace <existing order token> <quantity> <price>")
                    elif input_list[0] == "cancel":
                        if len(input_list) == 2:
                            order_token = input_list[1]
                            cancel_order = self.create_cancel_order(order_token)
                            self.ouch_client_sock.send(cancel_order)
                        else:
                            print("Usage: cancel <order token>")
                    elif input_list[0] == "close":
                        self.ouch_client_sock.close()
                        sys.exit(1)
        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")
        finally:
            self.ouch_client_sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("usage:", sys.argv[0], "<host> <port> <username> <password> <requested sequence number>")
        sys.exit(1)

    main_host, main_port = sys.argv[1], int(sys.argv[2])
    main_username = sys.argv[3]
    main_password = sys.argv[4]
    main_requested_sequence_number = sys.argv[5]

    ouch_app_client = OUCHAppClient(main_host, main_port, main_username, main_password, main_requested_sequence_number)

    # Start the OUCH Client
    ouch_app_client.start()
