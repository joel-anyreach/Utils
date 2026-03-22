require('dotenv').config();

const required = [
  'INSTANTLY_API_KEY',
  'SLACK_BOT_TOKEN',
  'SLACK_CHANNEL_ID',
];

for (const key of required) {
  if (!process.env[key]) {
    console.error(`[config] ❌ Missing required environment variable: ${key}`);
    console.error(`[config] Please copy .env.example to .env and fill in all values.`);
    process.exit(1);
  }
}

module.exports = {
  instantlyApiKey: process.env.INSTANTLY_API_KEY,
  slackBotToken:   process.env.SLACK_BOT_TOKEN,
  slackChannelId:  process.env.SLACK_CHANNEL_ID,
  cronTimezone:    process.env.CRON_TIMEZONE || 'America/Los_Angeles',
};
