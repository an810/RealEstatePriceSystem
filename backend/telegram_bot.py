import asyncio
import httpx
import logging
import os
import json
from enum import Enum
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLOUDFLARE_WORKER_URL = "https://hustrealestate.antp9254.workers.dev"
TELEGRAM_BOT_TOKEN = "8135521232:AAH6i6cIc0LzGLtp_tgXfMmnDH5HV-MDTUc"
TELEGRAM_API = f"{CLOUDFLARE_WORKER_URL}/bot{TELEGRAM_BOT_TOKEN}"
BACKEND_URL = "http://localhost:8000"


PROPERTY_TYPE_MAPPING = {
    'Chung cư': 1,
    'Biệt thự': 2,
    'Nhà riêng': 3,
    'Đất': 4
}

def convert_int_to_property_type(value: int) -> str:
    """Convert property type integer code to text format"""
    for prop_type, prop_id in PROPERTY_TYPE_MAPPING.items():
        if prop_id == value:
            return prop_type
    return "Không xác định"

def convert_int_to_phaply(value: int) -> str:
    """Convert legal status integer code to text format"""
    if value == 0:
        return "Chưa có sổ"
    elif value == 1:
        return "Hợp đồng mua bán"
    elif value == 2:
        return "Đã có sổ"
    else:
        return "Không xác định"


# User states
class UserState(Enum):
    IDLE = "idle"
    PREDICTING = "predicting"
    SEARCHING = "searching"
    SUBSCRIBING = "subscribing"

# Store user states and data
user_states: Dict[int, UserState] = {}
user_data: Dict[int, Dict[str, Any]] = {}

# Reply keyboard markup for /start
def get_start_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Search Properties", "callback_data": "search"},
                {"text": "Get Price Prediction", "callback_data": "predict"},
            ],
            [
                {"text": "Subscribe to Updates", "callback_data": "subscribe"},
                {"text": "Unsubscribe", "callback_data": "unsubscribe"},
            ],
            [
                {"text": "Help", "callback_data": "help"},
            ],
        ]
    }

async def send_telegram_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    async with httpx.AsyncClient() as client:
        response = await client.post(f"{TELEGRAM_API}/sendMessage", json=payload)
        if response.status_code != 200:
            logger.error("Failed to send message: %s", response.text)

async def answer_callback_query(callback_query_id):
    async with httpx.AsyncClient() as client:
        await client.post(f"{TELEGRAM_API}/answerCallbackQuery", json={"callback_query_id": callback_query_id})

def get_property_type_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Chung cư", "callback_data": "type_chungcu"},
                {"text": "Biệt thự", "callback_data": "type_bietthu"},
            ],
            [
                {"text": "Nhà riêng", "callback_data": "type_nharieng"},
                {"text": "Đất", "callback_data": "type_dat"},
            ],
        ]
    }

def get_legal_status_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Sổ đỏ", "callback_data": "legal_sodo"},
                {"text": "Hợp đồng", "callback_data": "legal_hopdong"},
            ],
            [
                {"text": "Chưa có sổ", "callback_data": "legal_chuacoso"},
            ],
        ]
    }

def get_district_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Ba Đình", "callback_data": "district_badinh"},
                {"text": "Cầu Giấy", "callback_data": "district_caugiay"},
            ],
            [
                {"text": "Đống Đa", "callback_data": "district_dongda"},
                {"text": "Hai Bà Trưng", "callback_data": "district_haibatrung"},
            ],
            [
                {"text": "Hoàn Kiếm", "callback_data": "district_hoankiem"},
                {"text": "Hoàng Mai", "callback_data": "district_hoangmai"},
            ],
            [
                {"text": "Long Biên", "callback_data": "district_longbien"},
                {"text": "Tây Hồ", "callback_data": "district_tayho"},
            ],
            [
                {"text": "Thanh Xuân", "callback_data": "district_thanhxuan"},
                {"text": "Hà Đông", "callback_data": "district_hadong"},
            ],
            [
                {"text": "More Districts...", "callback_data": "district_more"},
            ],
        ]
    }

def get_more_districts_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "Ba Vì", "callback_data": "district_bavi"},
                {"text": "Chương Mỹ", "callback_data": "district_chuongmy"},
            ],
            [
                {"text": "Đan Phượng", "callback_data": "district_danphuong"},
                {"text": "Đông Anh", "callback_data": "district_donganh"},
            ],
            [
                {"text": "Gia Lâm", "callback_data": "district_gialam"},
                {"text": "Hoài Đức", "callback_data": "district_hoaiduc"},
            ],
            [
                {"text": "Mê Linh", "callback_data": "district_melinh"},
                {"text": "Mỹ Đức", "callback_data": "district_myduc"},
            ],
            [
                {"text": "Phú Xuyên", "callback_data": "district_phuxuyen"},
                {"text": "Phúc Thọ", "callback_data": "district_phuctho"},
            ],
            [
                {"text": "Back to Main Districts", "callback_data": "district_main"},
            ],
        ]
    }

# District mapping for callback data to actual names
DISTRICT_MAPPING = {
    'district_badinh': 'Ba Đình',
    'district_caugiay': 'Cầu Giấy',
    'district_dongda': 'Đống Đa',
    'district_haibatrung': 'Hai Bà Trưng',
    'district_hoankiem': 'Hoàn Kiếm',
    'district_hoangmai': 'Hoàng Mai',
    'district_longbien': 'Long Biên',
    'district_tayho': 'Tây Hồ',
    'district_thanhxuan': 'Thanh Xuân',
    'district_hadong': 'Hà Đông',
    'district_bavi': 'Ba Vì',
    'district_chuongmy': 'Chương Mỹ',
    'district_danphuong': 'Đan Phượng',
    'district_donganh': 'Đông Anh',
    'district_gialam': 'Gia Lâm',
    'district_hoaiduc': 'Hoài Đức',
    'district_melinh': 'Mê Linh',
    'district_myduc': 'Mỹ Đức',
    'district_phuxuyen': 'Phú Xuyên',
    'district_phuctho': 'Phúc Thọ',
}

async def process_callback_query(callback_query):
    data = callback_query["data"]
    chat_id = callback_query["message"]["chat"]["id"]
    await answer_callback_query(callback_query["id"])

    if data == "search":
        user_states[chat_id] = UserState.SEARCHING
        user_data[chat_id] = {}
        await send_telegram_message(
            chat_id,
            "Let's search for properties! 🏠\n\n"
            "First, enter the minimum price (in billion VND):"
        )
    elif data == "predict":
        user_states[chat_id] = UserState.PREDICTING
        user_data[chat_id] = {}
        await send_telegram_message(
            chat_id,
            "Let's predict a property price! 🏠\n\n"
            "First, enter the area in square meters:"
        )
    elif data == "subscribe":
        user_states[chat_id] = UserState.SUBSCRIBING
        user_data[chat_id] = {}
        await send_telegram_message(
            chat_id,
            "Let's set up your subscription! 📬\n\n"
            "First, enter the minimum price (in billion VND):"
        )
    elif data == "unsubscribe":
        # Make the unsubscribe request
        logger.info(f"Making unsubscribe request to {BACKEND_URL}/unsubscribe/{chat_id}")
        async with httpx.AsyncClient() as client:
            resp = await client.delete(f"{BACKEND_URL}/unsubscribe/{chat_id}")
            if resp.status_code == 200:
                logger.info(f"Unsubscribe response: {json.dumps(resp.json(), indent=2)}")
                await send_telegram_message(chat_id, "Successfully unsubscribed from updates! 🎉")
            elif resp.status_code == 404:
                await send_telegram_message(chat_id, "You don't have any active subscriptions.")
            else:
                logger.error(f"Unsubscribe request failed with status {resp.status_code}: {resp.text}")
                await send_telegram_message(chat_id, "Unsubscribe error, try again later.")
        await send_telegram_message(chat_id, "What would you like to do next?", reply_markup=get_start_keyboard())
    elif data == "help":
        await send_telegram_message(
            chat_id,
            "Here's how to use this bot:\n\n"
            "1. Search Properties: Find properties matching your criteria\n"
            "2. Get Price Prediction: Get an estimated price for a property\n"
            "3. Subscribe to Updates: Get notified about new properties\n"
            "4. Unsubscribe: Remove your subscription to updates\n\n"
            "Use the buttons above to get started!",
            reply_markup=get_start_keyboard(),
        )
    elif data.startswith("type_"):
        property_type = {
            "type_chungcu": "Chung cư",
            "type_bietthu": "Biệt thự",
            "type_nharieng": "Nhà riêng",
            "type_dat": "Đất"
        }[data]
        user_data[chat_id]["property_type"] = property_type
        await send_telegram_message(
            chat_id,
            f"Selected property type: {property_type}\n\n"
            "Now, select the legal status:",
            reply_markup=get_legal_status_keyboard()
        )
    elif data.startswith("legal_"):
        legal_status = {
            "legal_sodo": "Sổ đỏ",
            "legal_hopdong": "Hợp đồng",
            "legal_chuacoso": "Chưa có sổ"
        }[data]
        user_data[chat_id]["legal_status"] = legal_status
        
        if user_states[chat_id] == UserState.SEARCHING:
            await send_telegram_message(
                chat_id,
                f"Selected legal status: {legal_status}\n\n"
                "Now, select the district:",
                reply_markup=get_district_keyboard()
            )
        elif user_states[chat_id] == UserState.PREDICTING:
            await send_telegram_message(
                chat_id,
                f"Selected legal status: {legal_status}\n\n"
                "Now, select the district:",
                reply_markup=get_district_keyboard()
            )
        elif user_states[chat_id] == UserState.SUBSCRIBING:
            await send_telegram_message(
                chat_id,
                f"Selected legal status: {legal_status}\n\n"
                "Now, select the district:",
                reply_markup=get_district_keyboard()
            ) 
    elif data.startswith("district_"):
        if data == "district_more":
            await send_telegram_message(
                chat_id,
                "Select a district:",
                reply_markup=get_more_districts_keyboard()
            )
        elif data == "district_main":
            await send_telegram_message(
                chat_id,
                "Select a district:",
                reply_markup=get_district_keyboard()
            )
        else:
            district = DISTRICT_MAPPING[data]
            user_data[chat_id]["district"] = district
            
            if user_states[chat_id] == UserState.SEARCHING:
                # Make the search request
                search_request = {
                    'price_range': {
                        'min_price': user_data[chat_id]['min_price'],
                        'max_price': user_data[chat_id]['max_price']
                    },
                    'area_range': {
                        'min_area': user_data[chat_id]['min_area'],
                        'max_area': user_data[chat_id]['max_area']
                    },
                    'num_bedrooms': user_data[chat_id].get('number_of_bedrooms', 0),
                    'num_toilets': user_data[chat_id].get('number_of_toilets', 0),
                    'districts': [district],
                    'legal_status': user_data[chat_id]['legal_status'],
                    'property_type': user_data[chat_id]['property_type']
                }
                logger.info(f"Making search request to {BACKEND_URL}/search with data: {json.dumps(search_request, indent=2)}")
                async with httpx.AsyncClient() as client:
                    resp = await client.post(f"{BACKEND_URL}/search", json=search_request)
                    if resp.status_code == 200:
                        results = resp.json()
                        logger.info(f"Search response: {json.dumps(results, indent=2)}")
                        if results['results_by_district']:
                            message = "Here are the properties matching your criteria:\n\n"
                            for district, properties in results['results_by_district'].items():
                                if properties:
                                    message += f"📍 {district}:\n"
                                    for prop in properties[:3]:
                                        message += f"💰 {prop['price']:,.2f} billion VND\n"
                                        message += f"🏠 {convert_int_to_property_type(prop['property_type_id'])}\n"
                                        message += f"📐 {prop['area']}m² | 🛏️ {prop['number_of_bedrooms']} | 🚿 {prop['number_of_toilets']}\n"
                                        message += f"📜 {prop['legal_status']}\n"
                                        message += f"🔗 {prop['url']}\n\n"
                            await send_telegram_message(chat_id, message)
                        else:
                            await send_telegram_message(chat_id, "No properties found matching your criteria.")
                    else:
                        logger.error(f"Search request failed with status {resp.status_code}: {resp.text}")
                        await send_telegram_message(chat_id, "Search error, try again later.")
                user_states[chat_id] = UserState.IDLE
                user_data[chat_id] = {}
                await send_telegram_message(chat_id, "What would you like to do next?", reply_markup=get_start_keyboard())
            
            elif user_states[chat_id] == UserState.PREDICTING:
                # Make the prediction request
                prediction_request = {
                    'area': user_data[chat_id]['area'],
                    'number_of_bedrooms': user_data[chat_id]['number_of_bedrooms'],
                    'number_of_toilets': user_data[chat_id]['number_of_toilets'],
                    'legal': user_data[chat_id]['legal_status'],
                    'district': district,
                    'property_type': user_data[chat_id]['property_type']
                }
                logger.info(f"Making prediction request to {BACKEND_URL}/predict-price with data: {json.dumps(prediction_request, indent=2)}")
                async with httpx.AsyncClient() as client:
                    resp = await client.post(f"{BACKEND_URL}/predict-price", json=prediction_request)
                    if resp.status_code == 200:
                        pred = resp.json()
                        logger.info(f"Prediction response: {json.dumps(pred, indent=2)}")
                        await send_telegram_message(chat_id, f"Estimated price: {pred['predicted_price']:,.2f} billion VND")
                    else:
                        logger.error(f"Prediction request failed with status {resp.status_code}: {resp.text}")
                        await send_telegram_message(chat_id, "Prediction error, try again later.")
                user_states[chat_id] = UserState.IDLE
                user_data[chat_id] = {}
                await send_telegram_message(chat_id, "What would you like to do next?", reply_markup=get_start_keyboard())

            elif user_states[chat_id] == UserState.SUBSCRIBING:
                # Make the subscription request
                subscription_request = {
                    "user_name": callback_query["from"]["first_name"],
                    "price_range": {
                        "min_price": user_data[chat_id]["min_price"],
                        "max_price": user_data[chat_id]["max_price"]
                    },
                    "area_range": {
                        "min_area": user_data[chat_id]["min_area"],
                        "max_area": user_data[chat_id]["max_area"]
                    },
                    "num_bedrooms": user_data[chat_id]['number_of_bedrooms'],
                    "num_toilets": user_data[chat_id]['number_of_toilets'],
                    "districts": [district],
                    "legal_status": user_data[chat_id]["legal_status"],
                    "property_type": user_data[chat_id]["property_type"],
                    "user_id": str(chat_id),
                    "user_type": "telegram"
                }
                logger.info(f"Making subscription request to {BACKEND_URL}/subscribe with data: {json.dumps(subscription_request, indent=2)}")
                async with httpx.AsyncClient() as client:
                    resp = await client.post(f"{BACKEND_URL}/subscribe", json=subscription_request)
                    if resp.status_code == 200:
                        logger.info(f"Subscription response: {json.dumps(resp.json(), indent=2)}")
                        await send_telegram_message(chat_id, "Successfully subscribed to updates! 🎉")
                    else:
                        logger.error(f"Subscription request failed with status {resp.status_code}: {resp.text}")
                        await send_telegram_message(chat_id, "Subscription error, try again later.")
                user_states[chat_id] = UserState.IDLE
                user_data[chat_id] = {}
                await send_telegram_message(chat_id, "What would you like to do next?", reply_markup=get_start_keyboard())

async def process_update(update):
    # Handle callback query (button presses)
    if update.get("callback_query"):
        await process_callback_query(update["callback_query"])
        return

    message = update.get("message") or update.get("edited_message")
    if not message or "text" not in message:
        return
    chat_id = message["chat"]["id"]
    text = message["text"].strip()
    logger.info("Received message: %s", text)

    # Handle commands
    if text == "/start":
        user_states[chat_id] = UserState.IDLE
        await send_telegram_message(
            chat_id,
            'Welcome to the Real Estate Price System Bot! 🏠\n\nWhat would you like to do?',
            reply_markup=get_start_keyboard(),
        )
    elif text == "/help":
        await send_telegram_message(
            chat_id,
            "Here's how to use this bot:\n\n"
            "1. Search Properties: Find properties matching your criteria\n"
            "2. Get Price Prediction: Get an estimated price for a property\n"
            "3. Subscribe to Updates: Get notified about new properties\n"
            "4. Unsubscribe: Remove your subscription to updates\n\n"
            "Use the buttons above to get started!",
            reply_markup=get_start_keyboard(),
        )
    else:
        # Handle state-based input
        state = user_states.get(chat_id, UserState.IDLE)
        user_input = user_data.get(chat_id, {})

        if state == UserState.SEARCHING:
            if "min_price" not in user_input:
                try:
                    min_price = float(text)
                    user_input["min_price"] = min_price
                    await send_telegram_message(chat_id, "Now enter the maximum price (in billion VND):")
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "max_price" not in user_input:
                try:
                    max_price = float(text)
                    user_input["max_price"] = max_price
                    await send_telegram_message(chat_id, "Enter the minimum area (in m²):")
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "min_area" not in user_input:
                try:
                    min_area = float(text)
                    user_input["min_area"] = min_area
                    await send_telegram_message(chat_id, "Now enter the maximum area (in m²):")
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "max_area" not in user_input:
                try:
                    max_area = float(text)
                    user_input["max_area"] = max_area
                    await send_telegram_message(chat_id, "Select the property type:", reply_markup=get_property_type_keyboard())
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "property_type" not in user_input:
                await send_telegram_message(chat_id, "Select the property type:", reply_markup=get_property_type_keyboard())
            elif "legal_status" not in user_input:
                await send_telegram_message(chat_id, "Select the legal status:", reply_markup=get_legal_status_keyboard())

        elif state == UserState.PREDICTING:
            if "area" not in user_input:
                try:
                    area = float(text)
                    user_input["area"] = area
                    await send_telegram_message(chat_id, "Enter the number of bedrooms:")
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "number_of_bedrooms" not in user_input:
                try:
                    bedrooms = int(text)
                    user_input["number_of_bedrooms"] = bedrooms
                    await send_telegram_message(chat_id, "Enter the number of bathrooms:")
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "number_of_toilets" not in user_input:
                try:
                    toilets = int(text)
                    user_input["number_of_toilets"] = toilets
                    await send_telegram_message(chat_id, "Select the property type:", reply_markup=get_property_type_keyboard())
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "property_type" not in user_input:
                await send_telegram_message(chat_id, "Select the property type:", reply_markup=get_property_type_keyboard())
            elif "legal_status" not in user_input:
                await send_telegram_message(chat_id, "Select the legal status:", reply_markup=get_legal_status_keyboard())

        elif state == UserState.SUBSCRIBING:
            if "min_price" not in user_input:
                try:
                    min_price = float(text)
                    user_input["min_price"] = min_price
                    await send_telegram_message(chat_id, "Now enter the maximum price (in billion VND):")
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "max_price" not in user_input:
                try:
                    max_price = float(text)
                    user_input["max_price"] = max_price
                    await send_telegram_message(chat_id, "Enter the minimum area (in m²):")
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "min_area" not in user_input:
                try:
                    min_area = float(text)
                    user_input["min_area"] = min_area
                    await send_telegram_message(chat_id, "Now enter the maximum area (in m²):")
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "max_area" not in user_input:
                try:
                    max_area = float(text)
                    user_input["max_area"] = max_area
                    await send_telegram_message(chat_id, "Enter the number of bedrooms:")
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "num_bedrooms" not in user_input:
                try:
                    num_bedrooms = int(text)
                    if num_bedrooms < 0:
                        await send_telegram_message(chat_id, "Please enter a non-negative number.")
                        return
                    user_input["number_of_bedrooms"] = num_bedrooms
                    await send_telegram_message(chat_id, "Enter the number of toilets:")
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "num_toilets" not in user_input:
                try:
                    num_toilets = int(text)
                    if num_toilets < 0:
                        await send_telegram_message(chat_id, "Please enter a non-negative number.")
                        return
                    user_input["number_of_toilets"] = num_toilets
                    await send_telegram_message(chat_id, "Select the property type:", reply_markup=get_property_type_keyboard())
                except ValueError:
                    await send_telegram_message(chat_id, "Please enter a valid number.")
            elif "property_type" not in user_input:
                await send_telegram_message(chat_id, "Select the property type:", reply_markup=get_property_type_keyboard())
            elif "legal_status" not in user_input:
                await send_telegram_message(chat_id, "Select the legal status:", reply_markup=get_legal_status_keyboard())
            elif "district" not in user_input:
                await send_telegram_message(chat_id, "Select the district:", reply_markup=get_district_keyboard())
            else:
                # Make the subscription request
                subscription_request = {
                    "user_name": message["from"]["first_name"],
                    "price_range": {
                        "min_price": user_input["min_price"],
                        "max_price": user_input["max_price"]
                    },
                    "area_range": {
                        "min_area": user_input["min_area"],
                        "max_area": user_input["max_area"]
                    },
                    "num_bedrooms": user_input["number_of_bedrooms"],
                    "num_toilets": user_input["number_of_toilets"],
                    "districts": [user_input["district"]],
                    "legal_status": user_input["legal_status"],
                    "property_type": user_input["property_type"],
                    "user_id": str(chat_id),
                    "user_type": "telegram"
                }
                logger.info(f"Making subscription request to {BACKEND_URL}/subscribe with data: {json.dumps(subscription_request, indent=2)}")
                async with httpx.AsyncClient() as client:
                    resp = await client.post(f"{BACKEND_URL}/subscribe", json=subscription_request)
                    if resp.status_code == 200:
                        logger.info(f"Subscription response: {json.dumps(resp.json(), indent=2)}")
                        await send_telegram_message(chat_id, "Successfully subscribed to updates! 🎉")
                    else:
                        logger.error(f"Subscription request failed with status {resp.status_code}: {resp.text}")
                        await send_telegram_message(chat_id, "Subscription error, try again later.")
                user_states[chat_id] = UserState.IDLE
                user_data[chat_id] = {}
                await send_telegram_message(chat_id, "What would you like to do next?", reply_markup=get_start_keyboard())

        else:
            await send_telegram_message(
                chat_id,
                "Please use the buttons to start a new action.",
                reply_markup=get_start_keyboard(),
            )

async def polling_loop():
    offset = 0
    while True:
        try:
            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.get(f"{TELEGRAM_API}/getUpdates", params={"timeout": 30, "offset": offset})
                updates = resp.json()["result"]
                for update in updates:
                    offset = update["update_id"] + 1
                    await process_update(update)
        except Exception as e:
            logger.error("Error in polling: %s", e)
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(polling_loop())
