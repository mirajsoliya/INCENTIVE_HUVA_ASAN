import configparser
import requests


def load_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

# Load configuration settings
config_file = './send_msg/config.ini'
config = load_config(config_file)


def send_whatsapp_message():
    token = config['whatsapp']['ultramsg_token']
    to  = "120363361979418877@g.us"
    message = "🚀 *HMNG HUVA ASAAN INCENTIVE SCRIPTS RUNNING... DONE!* ✅"
    """Send a WhatsApp message using UltraMsg API."""
    url = config['whatsapp']['ultramsg_chat_endpoint']
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "token": token,
        "to": to,
        "body": message
    }

    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        print(f"Message sent successfully to {to}.")
    else:
        print(f"Failed to send message to {to}. Status code: {response.status_code}")