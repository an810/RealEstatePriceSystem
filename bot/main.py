from typing import Final
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler, ConversationHandler
import aiohttp
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN: Final = os.getenv('TELEGRAM_BOT_TOKEN')
BOT_USERNAME: Final = os.getenv('BOT_USERNAME')
API_URL: Final = os.getenv('API_URL', 'http://localhost:8000')

# Conversation states
(
    PRICE_MIN,
    PRICE_MAX,
    AREA_MIN,
    AREA_MAX,
    BEDROOMS,
    TOILETS,
    DISTRICTS,
    LEGAL_STATUS,
    CONFIRM
) = range(9)

# Store user data during conversation
user_data = {}

# Predefined options
PRICE_RANGES = [
    [("500M - 1B", 500000000), ("1B - 2B", 1000000000), ("2B - 3B", 2000000000)],
    [("3B - 5B", 3000000000), ("5B - 7B", 5000000000), ("7B - 10B", 7000000000)],
    [("10B - 15B", 10000000000), ("15B - 20B", 15000000000), ("20B+", 20000000000)]
]

AREA_RANGES = [
    [("30-50m²", 30), ("50-70m²", 50), ("70-90m²", 70)],
    [("90-120m²", 90), ("120-150m²", 120), ("150-200m²", 150)],
    [("200-300m²", 200), ("300-500m²", 300), ("500m²+", 500)]
]

BEDROOM_OPTIONS = [("1", 1), ("2", 2), ("3", 3), ("4+", 4)]
TOILET_OPTIONS = [("1", 1), ("2", 2), ("3", 3), ("4+", 4)]

DISTRICTS = [
    'Ba Đình', 'Ba Vì', 'Cầu Giấy', 'Chương Mỹ', 'Đan Phượng', 'Đông Anh', 'Đống Đa',
    'Gia Lâm', 'Hà Đông', 'Hai Bà Trưng', 'Hoài Đức', 'Hoàn Kiếm', 'Hoàng Mai',
    'Long Biên', 'Mê Linh', 'Mỹ Đức', 'Phú Xuyên', 'Phúc Thọ', 'Quốc Oai', 'Sóc Sơn',
    'Sơn Tây', 'Tây Hồ', 'Thạch Thất', 'Thanh Oai', 'Thanh Trì', 'Thanh Xuân',
    'Thường Tín', 'Từ Liêm', 'Ứng Hòa'
]

LEGAL_STATUSES = ['Chưa có sổ', 'Hợp đồng', 'Sổ đỏ']

def create_price_keyboard():
    keyboard = []
    for row in PRICE_RANGES:
        keyboard_row = []
        for label, value in row:
            keyboard_row.append(InlineKeyboardButton(label, callback_data=f"price_{value}"))
        keyboard.append(keyboard_row)
    keyboard.append([InlineKeyboardButton("Custom Price", callback_data="price_custom")])
    return InlineKeyboardMarkup(keyboard)

def create_area_keyboard():
    keyboard = []
    for row in AREA_RANGES:
        keyboard_row = []
        for label, value in row:
            keyboard_row.append(InlineKeyboardButton(label, callback_data=f"area_{value}"))
        keyboard.append(keyboard_row)
    keyboard.append([InlineKeyboardButton("Custom Area", callback_data="area_custom")])
    return InlineKeyboardMarkup(keyboard)

def create_bedroom_keyboard():
    keyboard = []
    row = []
    for label, value in BEDROOM_OPTIONS:
        row.append(InlineKeyboardButton(label, callback_data=f"bedroom_{value}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)

def create_toilet_keyboard():
    keyboard = []
    row = []
    for label, value in TOILET_OPTIONS:
        row.append(InlineKeyboardButton(label, callback_data=f"toilet_{value}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)

def create_district_keyboard():
    keyboard = []
    row = []
    for district in DISTRICTS:
        row.append(InlineKeyboardButton(district, callback_data=f"district_{district}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("Done Selecting", callback_data="districts_done")])
    return InlineKeyboardMarkup(keyboard)

def create_legal_status_keyboard():
    keyboard = []
    for status in LEGAL_STATUSES:
        keyboard.append([InlineKeyboardButton(status, callback_data=f"legal_{status}")])
    return InlineKeyboardMarkup(keyboard)

# Commands
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Subscribe to Updates", callback_data='subscribe')],
        [InlineKeyboardButton("Predict Price", callback_data='predict')],
        [InlineKeyboardButton("Help", callback_data='help')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Hello! I'm the Real Estate Bot. How can I help you today?",
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """I'm here to help you with your real estate needs. Here are the commands you can use:

/start - Start the bot
/help - Show this help message
/subscribe - Subscribe to real estate updates
/predict - Estimate the price of real estate
/cancel - Cancel your subscription

You can also use the buttons below to navigate."""
    
    keyboard = [
        [InlineKeyboardButton("Subscribe to Updates", callback_data='subscribe')],
        [InlineKeyboardButton("Predict Price", callback_data='predict')],
        [InlineKeyboardButton("Back to Start", callback_data='start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(help_text, reply_markup=reply_markup)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    if query.data == 'start':
        await start_command(update, context)
    elif query.data == 'help':
        await help_command(update, context)
    elif query.data == 'subscribe':
        user_data[update.effective_user.id] = {
            'user_id': str(update.effective_user.id),
            'user_type': 'telegram',
            'districts': []
        }
        await query.message.reply_text(
            "Let's set up your subscription. First, select your minimum price:",
            reply_markup=create_price_keyboard()
        )
        return PRICE_MIN
    elif query.data == 'predict':
        await query.message.reply_text(
            "Please provide the property details in this format:\n"
            "/predict <area> <bedrooms> <toilets> <district> <legal_status>\n"
            "Example: /predict 100 2 2 'Cầu Giấy' 'Sổ đỏ'"
        )
    elif query.data.startswith('price_'):
        if query.data == 'price_custom':
            await query.message.reply_text("Please enter your minimum price in VND:")
            return PRICE_MIN
        else:
            price = float(query.data.split('_')[1])
            user_data[update.effective_user.id]['price_range'] = {'min_price': price}
            await query.message.reply_text(
                "Now select your maximum price:",
                reply_markup=create_price_keyboard()
            )
            return PRICE_MAX
    elif query.data.startswith('area_'):
        if query.data == 'area_custom':
            await query.message.reply_text("Please enter your minimum area in m²:")
            return AREA_MIN
        else:
            area = float(query.data.split('_')[1])
            user_data[update.effective_user.id]['area_range'] = {'min_area': area}
            await query.message.reply_text(
                "Now select your maximum area:",
                reply_markup=create_area_keyboard()
            )
            return AREA_MAX
    elif query.data.startswith('bedroom_'):
        bedrooms = int(query.data.split('_')[1])
        user_data[update.effective_user.id]['num_bedrooms'] = bedrooms
        await query.message.reply_text(
            "Select number of toilets:",
            reply_markup=create_toilet_keyboard()
        )
        return TOILETS
    elif query.data.startswith('toilet_'):
        toilets = int(query.data.split('_')[1])
        user_data[update.effective_user.id]['num_toilets'] = toilets
        await query.message.reply_text(
            "Select districts (you can select multiple):",
            reply_markup=create_district_keyboard()
        )
        return DISTRICTS
    elif query.data.startswith('district_'):
        district = query.data.split('_')[1]
        if district not in user_data[update.effective_user.id]['districts']:
            user_data[update.effective_user.id]['districts'].append(district)
        await query.message.reply_text(
            f"Selected districts: {', '.join(user_data[update.effective_user.id]['districts'])}\n"
            "Select more districts or click 'Done Selecting' when finished.",
            reply_markup=create_district_keyboard()
        )
        return DISTRICTS
    elif query.data == 'districts_done':
        if not user_data[update.effective_user.id]['districts']:
            await query.message.reply_text(
                "Please select at least one district.",
                reply_markup=create_district_keyboard()
            )
            return DISTRICTS
        await query.message.reply_text(
            "Select legal status:",
            reply_markup=create_legal_status_keyboard()
        )
        return LEGAL_STATUS
    elif query.data.startswith('legal_'):
        legal_status = query.data.split('_')[1]
        user_data[update.effective_user.id]['legal_status'] = legal_status
        
        # Show summary and confirmation
        data = user_data[update.effective_user.id]
        summary = f"""Please confirm your subscription details:

Price Range: {data['price_range']['min_price']:,.0f} - {data['price_range']['max_price']:,.0f} VND
Area Range: {data['area_range']['min_area']} - {data['area_range']['max_area']} m²
Bedrooms: {data['num_bedrooms']}
Toilets: {data['num_toilets']}
Districts: {', '.join(data['districts'])}
Legal Status: {data['legal_status']}

Is this correct?"""
        
        keyboard = [
            [
                InlineKeyboardButton("Yes, Subscribe", callback_data='confirm_subscription'),
                InlineKeyboardButton("No, Cancel", callback_data='cancel_subscription')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.reply_text(summary, reply_markup=reply_markup)
        return CONFIRM
    elif query.data == 'confirm_subscription':
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{API_URL}/subscribe", json=user_data[update.effective_user.id]) as response:
                    if response.status == 200:
                        await query.message.reply_text(
                            "Successfully subscribed! You will receive updates about properties matching your criteria."
                        )
                    else:
                        await query.message.reply_text("Failed to subscribe. Please try again later.")
        except Exception as e:
            await query.message.reply_text(f"An error occurred: {str(e)}")
        finally:
            user_data.pop(update.effective_user.id, None)
            return ConversationHandler.END

async def handle_custom_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        price = float(update.message.text)
        if price < 0:
            raise ValueError
        user_data[update.effective_user.id]['price_range'] = {'min_price': price}
        await update.message.reply_text(
            "Now select your maximum price:",
            reply_markup=create_price_keyboard()
        )
        return PRICE_MAX
    except ValueError:
        await update.message.reply_text("Please enter a valid positive number.")
        return PRICE_MIN

async def handle_custom_area(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        area = float(update.message.text)
        if area < 0:
            raise ValueError
        user_data[update.effective_user.id]['area_range'] = {'min_area': area}
        await update.message.reply_text(
            "Now select your maximum area:",
            reply_markup=create_area_keyboard()
        )
        return AREA_MAX
    except ValueError:
        await update.message.reply_text("Please enter a valid positive number.")
        return AREA_MIN

async def predict_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 5:
        await update.message.reply_text(
            "Please provide all required information:\n"
            "/predict <area> <bedrooms> <toilets> <district> <legal_status>\n"
            "Example: /predict 100 2 2 'Cầu Giấy' 'Sổ đỏ'"
        )
        return
    
    try:
        area = float(context.args[0])
        bedrooms = int(context.args[1])
        toilets = int(context.args[2])
        district = context.args[3]
        legal_status = context.args[4]
        
        if area <= 0 or bedrooms < 0 or toilets < 0:
            raise ValueError("Invalid numeric values")
        
        if legal_status not in ['Chưa có sổ', 'Hợp đồng', 'Sổ đỏ']:
            raise ValueError("Invalid legal status")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{API_URL}/predict",
                json={
                    "area": area,
                    "number_of_bedrooms": bedrooms,
                    "number_of_toilets": toilets,
                    "district": district,
                    "legal": legal_status
                }
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    price = result["predicted_price"]
                    response_text = f"""Estimated price for your property:
- Area: {area}m²
- Bedrooms: {bedrooms}
- Toilets: {toilets}
- District: {district}
- Legal Status: {legal_status}
Estimated price: {price:,.2f} VND

Note: This is an estimation based on current market data."""
                    await update.message.reply_text(response_text)
                else:
                    await update.message.reply_text("Failed to get price prediction. Please try again later.")
    except ValueError as e:
        await update.message.reply_text(
            f"Invalid input: {str(e)}\n"
            "Please use: /predict <area> <bedrooms> <toilets> <district> <legal_status>\n"
            "Example: /predict 100 2 2 'Cầu Giấy' 'Sổ đỏ'"
        )
    except Exception as e:
        await update.message.reply_text(f"An error occurred: {str(e)}")

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.delete(f"{API_URL}/unsubscribe/{update.effective_user.id}") as response:
                if response.status == 200:
                    await update.message.reply_text("You have been unsubscribed from notifications.")
                else:
                    await update.message.reply_text("Failed to unsubscribe. Please try again later.")
    except Exception as e:
        await update.message.reply_text(f"An error occurred: {str(e)}")

async def cancel_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_data.pop(update.effective_user.id, None)
    await query.message.reply_text("Subscription cancelled. Use /subscribe to start over.")
    return ConversationHandler.END

def main():
    print('Starting bot...')
    app = ApplicationBuilder().token(TOKEN).build()

    # Create conversation handler for subscription
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('subscribe', lambda u, c: button_handler(u, c)),
            CallbackQueryHandler(button_handler, pattern='^subscribe$')
        ],
        states = {
            PRICE_MIN: [
                CallbackQueryHandler(button_handler, pattern='^price_'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_price)
            ],
            PRICE_MAX: [
                CallbackQueryHandler(button_handler, pattern='^price_'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_price)
            ],
            AREA_MIN: [
                CallbackQueryHandler(button_handler, pattern='^area_'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_area)
            ],
            AREA_MAX: [
                CallbackQueryHandler(button_handler, pattern='^area_'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_area)
            ],
            BEDROOMS: [CallbackQueryHandler(button_handler, pattern='^bedroom_')],
            TOILETS: [CallbackQueryHandler(button_handler, pattern='^toilet_')],
            DISTRICTS: [CallbackQueryHandler(button_handler, pattern='^(district_|districts_done)$')],
            LEGAL_STATUS: [CallbackQueryHandler(button_handler, pattern='^legal_')],
            CONFIRM: [
                CallbackQueryHandler(button_handler, pattern='^confirm_subscription$'),
                CallbackQueryHandler(cancel_subscription, pattern='^cancel_subscription$')
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel_command)]
    )

    # Add handlers
    app.add_handler(CommandHandler('start', start_command))
    app.add_handler(CommandHandler('help', help_command))
    app.add_handler(CommandHandler('predict', predict_command))
    app.add_handler(conv_handler)
    app.add_handler(CallbackQueryHandler(button_handler))

    print('Polling...')
    app.run_polling(poll_interval=3)

if __name__ == '__main__':
    main() 
    
            
            

    
    