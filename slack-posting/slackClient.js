/**
 * slackClient.js
 * Thin wrapper around @slack/web-api.
 * Posts the weekly report as a single message to the configured channel.
 */

const { WebClient } = require('@slack/web-api');
const config = require('./config');

const slack = new WebClient(config.slackBotToken);

/**
 * Post the report text to the configured Slack channel.
 * @param {string} text  The formatted report message
 * @returns {Object}     Slack API result
 */
async function postReport(text) {
  const result = await slack.chat.postMessage({
    channel:       config.slackChannelId,
    text,
    unfurl_links:  false,
    unfurl_media:  false,
    // mrkdwn is enabled by default; explicit for clarity
    mrkdwn:        true,
  });

  if (!result.ok) {
    throw new Error(`Slack postMessage failed: ${result.error}`);
  }

  return result;
}

module.exports = { postReport };
