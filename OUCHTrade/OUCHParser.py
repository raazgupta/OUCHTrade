#!/usr/bin/env python3

class OUCHParser:

    @staticmethod
    def parse_message(ouch_bytes, message_list, ouch_dict):
        for message_item in message_list:
            if len(ouch_bytes) >= (message_item[1]+message_item[2]):
                if message_item[3] == "alpha":
                    ouch_dict[message_item[0]] = ouch_bytes[message_item[1]:message_item[1]+message_item[2]].decode(encoding='utf-8')
                elif message_item[3] == "integer":
                    ouch_dict[message_item[0]] = int.from_bytes(ouch_bytes[message_item[1]:message_item[1]+message_item[2]], byteorder='big')

        return ouch_dict

    @staticmethod
    def parse_ouch_bytes(ouch_bytes: bytes):
        ouch_dict = {}
        chunks = b''
        packet_length = 2

        message_dicts = {
            'A':  # Login Accepted Packet
            [
                ["session", 3, 10, "alpha"],
                ["sequence_number", 13, 20, "alpha"]
            ],
            'J':  # Login Reject Packet
            [
                ["reject_reason_code", 3, 1, "alpha"]
            ],
            'L':  # Login Request Packet
            [
                ["username", 3, 6, "alpha"],
                ["password", 9, 10, "alpha"],
                ["requested_session", 19, 10, "alpha"],
                ["requested_sequence_number", 29, 20, "alpha"]
            ]
        }

        unsequenced_message_dicts = {
            'O':  # Enter Order Message
            [
                ["message_type", 0, 1, "alpha"],
                ["order_token", 1, 4, "integer"],
                ["client_reference", 5, 10, "alpha"],
                ["buy_sell_indicator", 15, 1, "alpha"],
                ["quantity", 16, 4, "integer"],
                ["orderbook_id", 20, 4, "integer"],
                ["group", 24, 4, "alpha"],
                ["price", 28, 4, "integer"],
                ["time_in_force", 32, 4, "integer"],
                ["firm_id", 36, 4, "integer"],
                ["display", 40, 1, "alpha"],
                ["capacity", 41, 1, "alpha"],
                ["minimum_quantity", 42, 4, "integer"],
                ["order_classification", 46, 1, "alpha"],
                ["cash_margin_type", 47, 1, "alpha"]
            ],
            'U':  # Replace Order Message
            [
                ["message_type", 0, 1, "alpha"],
                ["existing_order_token", 1, 4, "integer"],
                ["replacement_order_token", 5, 4, "integer"],
                ["quantity", 9, 4, "integer"],
                ["price", 13, 4, "integer"],
                ["time_in_force", 17, 4, "integer"],
                ["display", 21, 1, "alpha"],
                ["minimum_quantity", 22, 4, "integer"]
            ],
            'X':  # Cancel Order Message
            [
                ["message_type", 0, 1, "alpha"],
                ["order_token", 1, 4, "integer"],
                ["quantity", 5, 4, "integer"]
            ]
        }

        sequenced_message_dicts = {
            'S':  # System Event Message
            [
                ["message_type", 0, 1, "alpha"],
                ["timestamp", 1, 8, "integer"],
                ["system_event", 9, 1, "alpha"]
            ],
            'A':  # Order Accepted Message
            [
                ["message_type", 0, 1, "alpha"],
                ["timestamp", 1, 8, "integer"],
                ["order_token", 9, 4, "integer"],
                ["client_reference", 13, 10, "alpha"],
                ["buy_sell_indicator", 23, 1, "alpha"],
                ["quantity", 24, 4, "integer"],
                ["orderbook_id", 28, 4, "integer"],
                ["group", 32, 4, "alpha"],
                ["price", 36, 4, "integer"],
                ["time_in_force", 40, 4, "integer"],
                ["firm_id", 44, 4, "integer"],
                ["display", 48, 1, "alpha"],
                ["order_number", 50, 8, "integer"],
                ["minimum_quantity", 58, 4, "integer"],
                ["order_state", 62, 1, "alpha"],
                ["order_classification", 63, 1, "alpha"],
                ["cash_margin_type", 64, 1, "alpha"]
            ],
            'U':  # Order Replaced Message
            [
                ["message_type", 0, 1, "alpha"],
                ["timestamp", 1, 8, "integer"],
                ["replacement_order_token", 9, 4, "integer"],
                ["buy_sell_indicator", 13, 1, "alpha"],
                ["quantity", 14, 4, "integer"],
                ["orderbook_id", 18, 4, "integer"],
                ["group", 22, 4, "alpha"],
                ["price", 26, 4, "integer"],
                ["time_in_force", 30, 4, "integer"],
                ["display", 34, 1, "alpha"],
                ["order_number", 35, 8, "integer"],
                ["minimum_quantity", 43, 4, "integer"],
                ["order_state", 47, 1, "alpha"],
                ["previous_order_token", 48, 4, "integer"]
            ],
            'C':  # Order Canceled Message
            [
                ["message_type", 0, 1, "alpha"],
                ["timestamp", 1, 8, "integer"],
                ["order_token", 9, 4, "integer"],
                ["decrement_quantity", 13, 4, "integer"],
                ["order_canceled_reason", 17, 1, "alpha"]
            ],
            'D':  # Order AIQ Canceled Message
            [
                ["message_type", 0, 1, "alpha"],
                ["timestamp", 1, 8, "integer"],
                ["order_token", 9, 4, "integer"],
                ["decrement_quantity", 13, 4, "integer"],
                ["order_canceled_reason", 17, 1, "alpha"],
                ["quantity_prevented_from_trading", 18, 4, "integer"],
                ["execution_price", 22, 4, "integer"],
                ["liquidity_indicator", 26, 1, "alpha"]
            ],
            'E':  # Order Executed Message
            [
                ["message_type", 0, 1, "alpha"],
                ["timestamp", 1, 8, "integer"],
                ["order_token", 9, 4, "integer"],
                ["executed_quantity", 13, 4, "integer"],
                ["execution_price", 17, 4, "integer"],
                ["liquidity_indicator", 21, 1, "alpha"],
                ["match_number", 22, 8, "integer"]
            ],
            'J':  # Order Rejected Message
            [
                ["message_type", 0, 1, "alpha"],
                ["timestamp", 1, 8, "integer"],
                ["order_token", 9, 4, "integer"],
                ["order_rejected_reason", 13, 1, "alpha"]
            ]
        }

        packet_length_bytes = ouch_bytes[:packet_length]
        packet_length_int = int.from_bytes(packet_length_bytes, byteorder='big')
        packet_type_byte = ouch_bytes[packet_length]
        packet_type_chr = chr(packet_type_byte)
        ouch_dict["packet_type"] = packet_type_chr

        if packet_type_chr == 'H':  # Server Heartbeat
            ouch_dict["packet_type"] = 'H'
        elif packet_type_chr == 'R':  # Client Heartbeat
            ouch_dict["packet_type"] = 'R'
        elif packet_type_chr == 'Z':  # End of Session Packet
            ouch_dict["packet_type"] = 'Z'
        elif packet_type_chr == 'O':  # Logout Request Packet
            ouch_dict["packet_type"] = 'O'
        elif packet_type_chr == '+':  # Debug Packet
            ouch_dict["packet_type"] = '+'
            text = ouch_bytes[3:].decode(encoding='utf-8')
            ouch_dict["text"] = text
        elif packet_type_chr == 'S':  # Sequenced Data Packet
            ouch_dict["packet_type"] = 'S'
            message_type_byte = ouch_bytes[packet_length+1]
            message_type_chr = chr(message_type_byte)
            message_list = sequenced_message_dicts[message_type_chr]
            if message_list:
                ouch_dict = OUCHParser.parse_message(ouch_bytes[packet_length+1:], message_list, ouch_dict)
        elif packet_type_chr == 'U':  # Unsequenced Data Packet
            ouch_dict["packet_type"] = 'U'
            message_type_byte = ouch_bytes[packet_length+1]
            message_type_chr = chr(message_type_byte)
            message_list = unsequenced_message_dicts[message_type_chr]
            if message_list:
                ouch_dict = OUCHParser.parse_message(ouch_bytes[packet_length+1:], message_list, ouch_dict)
        else:
            message_list = message_dicts[packet_type_chr]
            ouch_dict["packet_type"] = packet_type_chr
            if message_list:
                ouch_dict = OUCHParser.parse_message(ouch_bytes, message_list, ouch_dict)

        return ouch_dict
