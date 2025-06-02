# Real Estate Price System Telegram Bot

This is a Telegram bot that provides access to the Real Estate Price System's backend services through a Cloudflare Worker proxy.

## Features

- Search for properties
- Get price predictions
- Subscribe to updates
- Interactive menu interface

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file with the following variables:
```
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
CLOUDFLARE_WORKER_URL=your_cloudflare_worker_url
```

3. Deploy the Cloudflare Worker:
   - Go to the Cloudflare Workers dashboard
   - Create a new worker
   - Copy the contents of `worker.js` into the worker editor
   - Update the `BACKEND_URL` variable with your actual backend URL
   - Deploy the worker

4. Run the Telegram bot:
```bash
python telegram_bot.py
```

## Usage

1. Start a chat with your bot on Telegram
2. Send `/start` to see the main menu
3. Use the interactive buttons to:
   - Search for properties
   - Get price predictions
   - Subscribe to updates
   - Get help

## Input Formats

### Property Search
```
location: [area]
price_range: [min]-[max]
property_type: [type]
```

### Price Prediction
```
area: [number]
bedrooms: [number]
bathrooms: [number]
location: [area]
```

### Subscription
Simply send your email address to subscribe to updates. 