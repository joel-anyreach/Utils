/**
 * run-once.js
 * Runs the report a single time and exits.
 * Used by GitHub Actions — does NOT start the cron scheduler.
 */

const { getPreviousWeekRange, toApiDate } = require('./dateUtils');
const { getCampaignAnalytics, getCampaignOverview } = require('./instantlyClient');
const { buildReport } = require('./reportBuilder');
const { postReport } = require('./slackClient');

(async () => {
  console.log(`[${new Date().toISOString()}] ▶ Running weekly Instantly report (one-shot)...`);

  try {
    const dateRange = getPreviousWeekRange();
    const start     = toApiDate(dateRange.start);
    const end       = toApiDate(dateRange.end);

    console.log(`[report] Date range: ${start} → ${end}`);

    const [campaignData, overviewData] = await Promise.all([
      getCampaignAnalytics(start, end),
      getCampaignOverview(),
    ]);

    console.log(`[report] Received analytics for ${campaignData.length} campaign(s)`);

    const reportText = buildReport(campaignData, overviewData, dateRange);
    await postReport(reportText);

    console.log(`[report] ✅ Successfully posted to Slack.`);
    process.exit(0);
  } catch (err) {
    console.error(`[report] ❌ ERROR: ${err.message}`);
    if (err.response) {
      console.error(`[report]    Status : ${err.response.status}`);
      console.error(`[report]    Body   :`, err.response.data);
    }
    process.exit(1); // non-zero exit marks the GitHub Actions run as failed
  }
})();
