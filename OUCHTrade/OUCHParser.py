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
            ],
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
            ]
            'X':  # Cancel Order Message
            [
                ["message_type", 0, 1, "alpha"],
                ["order_token", 1, 4, "integer"],
                ["quantity", 5, 4, "integer"]
            ]
        }

        packet_length_bytes = ouch_bytes[:packet_length]
        packet_length_int = int.from_bytes(packet_length_bytes, byteorder='big')
        packet_type_byte = ouch_bytes[packet_length]
        packet_type_chr = chr(packet_type_byte)
        ouch_dict["packet_type"] = packet_type_chr

        if packet_type_chr == 'H':  # Server Heartbeat
            ouch_dict["packet_type"] = 'H'
        if packet_type_chr == 'R':  # Client Heartbeat
            ouch_dict["packet_type"] = 'R'
        if packet_type_chr == 'Z':  # End of Session Packet
            ouch_dict["packet_type"] = 'Z'
        if packet_type_chr == 'O':  # Logout Request Packet
            ouch_dict["packet_type"] = 'O'
        elif packet_type_chr == '+':  # Debug Packet
            ouch_dict["packet_type"] = '+'
            text = ouch_bytes[3:].decode(encoding='utf-8')
            ouch_dict["text"] = text
        elif packet_type_chr == 'S':  # Sequenced Data Packet
            ouch_dict["packet_type"] = 'S'
        elif packet_type_chr == 'U':  # Unsequenced Data Packet
            ouch_dict["packet_type"] = 'U'
            message_type_byte = ouch_bytes[packet_length+1]
            message_type_chr = chr(message_type_byte)
            message_list = message_dicts[message_type_chr]
            if message_list:
                ouch_dict = OUCHParser.parse_message(ouch_bytes[packet_length+1:], message_list, ouch_dict)
        else:
            message_list = message_dicts[packet_type_chr]
            ouch_dict["packet_type"] = packet_type_chr
            if message_list:
                ouch_dict = OUCHParser.parse_message(ouch_bytes, message_list, ouch_dict)

        return ouch_dict
