// testTelegram.js
const axios = require('axios');

const BOT_TOKEN = process.env.REACT_APP_TELEGRAM_BOT_TOKEN;
const CHAT_ID = process.env.REACT_APP_TELEGRAM_CHAT_ID;

axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
  chat_id: CHAT_ID,
  text: '👋 Hello from monitor test!'
})
.then(() => console.log('✅ Message sent!'))
.catch((err) => console.error('❌ Telegram error:', err.response?.data || err.message));
