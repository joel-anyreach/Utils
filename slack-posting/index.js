/**
 * index.js
 * Entry point for the Instantly Weekly Slack Reporter.
 *
 * Schedules runReport() every Friday at 8:00 AM PST/PDT.
 * Also exports runReport() for manual test runs:
 *   npm run test:report
 *   node -e "require('./index').runReport()"
 */

const cron   = require('node-cron');
const config = require('./config');

const { getPreviousWeekRange, toApiDate }       = require('./dateUtils');
const { getCampaignAnalytics, getCampaignOverview } = require('./instantlyClient');
const { buildReport }                           = require('./reportBuilder');
const { postReport }                            = require('./slackClient');

/**
 * Fetch data from Instantly, build the report, and post it to Slack.
 * Safe to call manually at any time for testing.
 */
async function runReport() {
  const timestamp = new Date().toISOString();
  console.log(`\n[${timestamp}] ▶ Running weekly Instantly report...`);

  try {
    // 1. Determine the previous Mon–Sun date range
    const dateRange = getPreviousWeekRange();
    const start     = toApiDate(dateRange.start);
    const end       = toApiDate(dateRange.end);
    console.log(`[report] Date range: ${start} → ${end}`);

    // 2. Fetch campaign analytics and all-time overview in parallel
    const [campaignData, overviewData] = await Promise.all([
      getCampaignAnalytics(start, end),
      getCampaignOverview(),
    ]);

    console.log(`[report] Received analytics for ${campaignData.length} campaign(s)`);

    // 3. Build the Slack message
    const reportText = buildReport(campaignData, overviewData, dateRange);

    // 4. Post to Slack
    await postReport(reportText);

    console.log(`[report] ✅ Posted to Slack channel ${config.slackChannelId}`);
  } catch (err) {
    console.error(`[report] ❌ ERROR: ${err.message}`);

    // Log Axios response details if available (API errors)
    if (err.response) {
      console.error(`[report]    Status : ${err.response.status}`);
      console.error(`[report]    Body   :`, err.response.data);
    }

    // Do NOT re-throw — keep the process alive for the next scheduled run
  }
}

// ── Cron Schedule ─────────────────────────────────────────────────────────────
// "0 8 * * 1,5" = 08:00 on every Monday and Friday
// timezone: America/Los_Angeles handles PST (UTC-8) and PDT (UTC-7) automatically

cron.schedule('0 8 * * 1,5', () => {
  runReport();
}, {
  timezone: config.cronTimezone,
  scheduled: true,
});

console.log(
  `[scheduler] ✅ Instantly weekly reporter is running.\n` +
  `             Schedule : Every Monday and Friday at 8:00 AM (${config.cronTimezone})\n` +
  `             Channel  : ${config.slackChannelId}\n` +
  `             Tip      : Run  npm run test:report  to post immediately.`
);

// Export for manual/test invocation
module.exports = { runReport };
