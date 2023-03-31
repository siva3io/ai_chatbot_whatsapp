import requests
import json
from kafka import KafkaConsumer


def get_conversation_id(mobile_no):
    url = "https://odoo-test.eunimart.com/rasachatbot/whatsapp/conversation_id?db=odoo_2&username=admin&password=admin"
    payload = json.dumps({
    "mobile": mobile_no[2:]
    })
    headers = {
    'Content-Type': 'application/json'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    response = response.json()
    if response.get("result"):
        return response["result"]
    raise Exception("Couldnot get result from odoo!")


def post_message(conversation_id, message):
    url = "https://odoo-test.eunimart.com/rasachatbot/post_message?db=odoo_2&username=admin&password=admin"

    payload = {
        "conversation_id": conversation_id,
        "message_content": message
    }
    # response = requests.request("POST", url, headers=headers, data=payload)
    response = requests.get(url, json=payload)
    result = response.json()
    return result


def button_response(conversation_id, button_id):
    url = "https://odoo-test.eunimart.com/rasachatbot/button_response?db=odoo_2&username=admin&password=admin"
    payload = {
        "conversation_id": conversation_id,
        "button_id": button_id
    }
    response = requests.post(url, json=payload)
    return response.json()


def whatsapp_post_text(mobile_no, message):
    url = "https://graph.facebook.com/v14.0/104164755790012/messages"

    payload = json.dumps({
        "messaging_product": "whatsapp",
        "to": mobile_no,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": message
        }
    })
    headers = {
        'Authorization': 'Bearer EAAFTJxZAqTY0BAOaWI3F32Va7QY7v1KHzZCo2ZAguAT4lpB4xnlrGgnnrNVS21Yc4oSZAYuEpzfdtouZC5xuNhhvWgy3ehKmQVLpott1uLNWXbZAZCnrNq2vd7udeBZAqtvhCCA8bkcMkxsoOzTSGKH8MDurnTkOSLuKm5kr2zjhPZAvETcDZBYVKS',
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code == 200:
        return True
    return False


def whatsapp_post_buttons(mobile_no, message, buttons):
    url = "https://graph.facebook.com/v14.0/104164755790012/messages"

    payload = json.dumps({
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": mobile_no,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {
                "text": message
            }
        }
    })
    headers = {
        'Authorization': 'Bearer EAAFTJxZAqTY0BAOaWI3F32Va7QY7v1KHzZCo2ZAguAT4lpB4xnlrGgnnrNVS21Yc4oSZAYuEpzfdtouZC5xuNhhvWgy3ehKmQVLpott1uLNWXbZAZCnrNq2vd7udeBZAqtvhCCA8bkcMkxsoOzTSGKH8MDurnTkOSLuKm5kr2zjhPZAvETcDZBYVKS',
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code == 200:
        return True
    return False


def main():

    broker = '20.197.41.10:9092'
    topics = ['private.ai.chatbot']
    consumer = KafkaConsumer(topics[0], bootstrap_servers=broker)
    print("Bot Started running...")
    for message in consumer:
        # ConsumerRecord(topic='private.ai.chatbot', partition=1, offset=19291, timestamp=1663652927914, timestamp_type=0, key=None, value=b'{"object": "whatsapp_business_account", "entry": [{"id": "105418702329479", "changes": [{"value": {"messaging_product": "whatsapp", "metadata": {"display_phone_number": "917672023802", "phone_number_id": "104164755790012"}, "contacts": [{"profile": {"name": "Nandhini"}, "wa_id": "919600695726"}], "messages": [{"from": "919600695726", "id": "wamid.HBgMOTE5NjAwNjk1NzI2FQIAEhgUM0VCMDM5QTZERDIyMzJGMzk2MkYA", "timestamp": "1663649498", "text": {"body": "Hi"}, "type": "text"}]}, "field": "messages"}]}]}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=502, serialized_header_size=-1)
        whatsapp_message = json.loads(message.value)
        print("Whatsapp msg: JSON:: ", whatsapp_message)
        try:
            # message_details = whatsapp_message["entry"][0]["changes"][0]["value"]["messages"][-1]
            entry = whatsapp_message.get("entry")
            if not entry:
                raise Exception("Entry is not valid in whatsapp_message")
            value = entry[0].get("changes")
            if not value:
                raise Exception("Messages is not available in whatsapp_message")
            if not len(value):
                raise Exception("Messages is not available in whatsapp_message")
            message_details = value[-1]
            mobile_no = message_details["from"]
            conversation_id = get_conversation_id(mobile_no)
            if message_details["type"] == "text":
                bot_response = post_message(
                    conversation_id, message_details["text"]["body"])
            elif message_details["type"] == "interactive":
                bot_response = button_response(
                    conversation_id, message_details["interactive"]["button_reply"]["id"])
            print(bot_response)
            responses = bot_response["result"]["responses"]
            buttons = bot_response["result"]["buttons"]
            for bot_message in responses:
                    send_status = whatsapp_post_text(
                        mobile_no, bot_message["text"])
                    if not send_status:
                        return {"Error": {"error_message": "Whatsapp api not working.!"}}
            if len(buttons) != 0:
                button_message = responses.pop()
                whatsapp_buttons = []
                for button in buttons:
                    whatsapp_buttons.append({"type": "reply",
                                             "reply": {
                                                 "id": button["button_id"],
                                                 "title": button["text/title"][:20]}}) # max length for title is 20
                send_status = whatsapp_post_buttons(
                    mobile_no, "Choose One", whatsapp_buttons[:3]) # max buttons limit is 3
        except Exception as e:
            print("Exception Occurred", e.__repr__())


main()
