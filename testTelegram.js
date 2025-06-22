// testTelegram.js
const axios = require('axios');

const BOT_TOKEN = '7811521792:AAHvgJxtalJtQBFqHn71KHj6vUM1yL4nnrE';
const CHAT_ID = '6662566183';

axios.post(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
  chat_id: CHAT_ID,
  text: '👋 Hello from monitor test!'
})
.then(() => console.log('✅ Message sent!'))
.catch((err) => console.error('❌ Telegram error:', err.response?.data || err.message));
