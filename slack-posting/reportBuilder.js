/**
 * reportBuilder.js
 * Takes raw Instantly API data and builds the Slack message string.
 * All business logic lives here: rate calculations, reply filtering,
 * bounce alerts, and table formatting.
 */

const { toDisplayRange, getNextFriday } = require('./dateUtils');

/**
 * Build the full weekly report text.
 *
 * @param {Array}  campaignData  Array from getCampaignAnalytics()
 * @param {Object} overviewData  Object from getCampaignOverview()
 * @param {Object} dateRange     { start: Date, end: Date }
 * @returns {string}             Slack-formatted message
 */
function buildReport(campaignData, overviewData, dateRange) {
  const { start, end } = dateRange;
  const dateLabel  = toDisplayRange(start, end);
  const nextFriday = getNextFriday();

  // ── THIS WEEK'S ACTIVITY ──────────────────────────────────────────────────
  let weekSent    = 0;
  let weekOpens   = 0;
  let weekReplies = 0;
  let weekClicks  = 0;

  for (const c of campaignData) {
    weekSent    += c.emails_sent_count     || c.sent_count      || 0;
    weekOpens   += c.emails_opened_count   || c.open_count      || 0;
    weekReplies += c.reply_count                                || 0;
    weekClicks  += c.link_clicks_count     || c.click_count     || 0;
  }

  // ── REPLY AUDIT ───────────────────────────────────────────────────────────
  // genuine = all replies minus auto-replies and out-of-office
  let autoReplies = 0;
  let oooReplies  = 0;

  for (const c of campaignData) {
    const bd = c.reply_breakdown || c.replyBreakdown || {};
    autoReplies += bd.auto_reply      || bd.autoReply      || 0;
    oooReplies  += bd.out_of_office   || bd.outOfOffice    || 0;
  }

  const inboxNoise     = autoReplies + oooReplies;
  const genuineReplies = weekReplies - inboxNoise;

  // ── BOUNCE ALERTS ─────────────────────────────────────────────────────────
  const activeCampaigns  = campaignData.filter(c => (c.emails_sent_count || c.sent_count || 0) > 0);
  const bouncedCampaigns = [];
  let   allAbove10       = activeCampaigns.length > 0;

  for (const c of activeCampaigns) {
    const sent   = c.emails_sent_count  || c.sent_count   || 0;
    const bounced = c.bounced_count     || c.bounce_count  || 0;
    const rate   = sent > 0 ? (bounced / sent) * 100 : 0;

    if (rate > 10) {
      bouncedCampaigns.push({
        name:    c.campaign_name || c.name || 'Unknown',
        rate:    rate.toFixed(0),
        bounced,
        sent,
      });
    } else {
      allAbove10 = false;
    }
  }

  // Sort bounce alerts descending by rate
  bouncedCampaigns.sort((a, b) => parseFloat(b.rate) - parseFloat(a.rate));

  // ── ALL-TIME SNAPSHOT ─────────────────────────────────────────────────────
  const allTimeContacted   = overviewData.total_leads_contacted || overviewData.contacted_count   || 0;
  const allTimeBounced     = overviewData.total_bounced         || overviewData.bounced_count      || 0;
  const allTimeSent        = overviewData.total_sent            || overviewData.emails_sent_count   || 0;
  const allTimeBounceRate  = allTimeSent > 0
    ? ((allTimeBounced / allTimeSent) * 100).toFixed(1)
    : '0.0';
  const allTimeOpps        = overviewData.total_opportunities   || overviewData.opportunities_count || 0;
  const allTimePipeline    = overviewData.pipeline_value        || overviewData.total_pipeline_value || 0;

  // ── ASSEMBLE REPORT ───────────────────────────────────────────────────────
  const lines = [];

  // Header
  lines.push(`*📊 Instantly Weekly Report — ${dateLabel}*`);
  lines.push('');

  // This Week's Activity
  lines.push(`*This Week's Activity:*`);
  lines.push(`>• Emails Sent: *${fmt(weekSent)}*`);
  lines.push(`>• Unique Opens: *${fmt(weekOpens)}*`);
  lines.push(`>• Raw Replies: *${fmt(weekReplies)}*`);
  lines.push(`>• Link Clicks: *${fmt(weekClicks)}*`);
  lines.push('');

  // Reply Audit
  lines.push(`*Reply Audit (${fmt(weekReplies)} received emails):*`);
  lines.push(`>• Personal inbox noise filtered: *${fmt(inboxNoise)}*`);
  lines.push(`>• Genuine outbound replies: *${fmt(genuineReplies)}*`);
  if (autoReplies > 0 || oooReplies > 0) {
    lines.push('>');
    lines.push('> _Breakdown:_');
    if (autoReplies > 0)  lines.push(`>   — auto_reply: ${fmt(autoReplies)}`);
    if (oooReplies > 0)   lines.push(`>   — out_of_office: ${fmt(oooReplies)}`);
  }
  if (genuineReplies === 0) {
    lines.push(`>`);
    lines.push(`> ⚠️ _No net-new interested replies this week._`);
  }
  lines.push('');

  // Bounce Rate Alerts
  lines.push(`*🚨 Bounce Rate Alerts (>10%):*`);
  if (bouncedCampaigns.length === 0) {
    lines.push(`>✅ All campaigns are under 10% bounce rate.`);
  } else {
    for (const c of bouncedCampaigns) {
      lines.push(`>• *${c.name}*: ${c.rate}% (${fmt(c.bounced)}/${fmt(c.sent)})`);
    }
    if (allAbove10) {
      lines.push('>');
      lines.push(`>⚠️ *All ${activeCampaigns.length} active campaigns are above 10% bounce. These lists need cleaning before more sends.*`);
    }
  }
  lines.push('');

  // All-Time Snapshot
  lines.push(`*All-Time Snapshot:*`);
  lines.push(`>• Total Contacted: *${fmt(allTimeContacted)}*`);
  lines.push(`>• All-Time Bounced: *${fmt(allTimeBounced)} (${allTimeBounceRate}%)*`);
  lines.push(`>• Opportunities: *${fmt(allTimeOpps)}*`);
  lines.push(`>• Pipeline Value: *$${fmt(allTimePipeline)}*`);
  lines.push('');

  // Active Campaigns Table
  lines.push(`*Active Campaigns:*`);
  if (activeCampaigns.length === 0) {
    lines.push(`_No campaigns with sends this week._`);
  } else {
    // Sort by sent count descending
    const sorted = [...activeCampaigns].sort((a, b) => {
      const aSent = a.emails_sent_count || a.sent_count || 0;
      const bSent = b.emails_sent_count || b.sent_count || 0;
      return bSent - aSent;
    });

    const COL = { name: 22, sent: 6, opens: 7, openPct: 7, replies: 9, replyPct: 8, clicks: 8, clickPct: 8, opps: 5 };

    lines.push('```');
    lines.push(
      pad('Campaign', COL.name) +
      pad('Sent',    COL.sent)  +
      pad('Opens',   COL.opens) +
      pad('Open%',   COL.openPct) +
      pad('Replies', COL.replies) +
      pad('Reply%',  COL.replyPct) +
      pad('Clicks',  COL.clicks) +
      pad('Click%',  COL.clickPct) +
      'Opps'
    );
    lines.push('─'.repeat(88));

    for (const c of sorted) {
      const name     = truncate(c.campaign_name || c.name || 'Unknown', COL.name - 1);
      const sent     = c.emails_sent_count  || c.sent_count   || 0;
      const opens    = c.emails_opened_count || c.open_count  || 0;
      const replies  = c.reply_count                          || 0;
      const clicks   = c.link_clicks_count  || c.click_count  || 0;
      const opps     = c.opportunities_count                  || 0;

      const openPct  = pct(opens,   sent);
      const replyPct = pct(replies, sent);
      const clickPct = pct(clicks,  sent);

      lines.push(
        pad(name,    COL.name) +
        pad(sent,    COL.sent) +
        pad(opens,   COL.opens) +
        pad(openPct, COL.openPct) +
        pad(replies, COL.replies) +
        pad(replyPct,COL.replyPct) +
        pad(clicks,  COL.clicks) +
        pad(clickPct,COL.clickPct) +
        opps
      );
    }
    lines.push('```');
  }
  lines.push('');

  // Footer
  lines.push(
    `_Auto-generated · Next report: ${nextFriday} · ` +
    `Reply data excludes personal inbox noise and auto-replies_`
  );

  return lines.join('\n');
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/** Format number with commas */
function fmt(n) {
  const num = Number(n);
  if (isNaN(num)) return '—';
  return num.toLocaleString('en-US');
}

/** Calculate a percentage string, guarded against division by zero */
function pct(numerator, denominator) {
  if (!denominator || denominator === 0) return '—';
  return Math.round((numerator / denominator) * 100) + '%';
}

/** Pad/truncate a string to exactly `width` characters */
function pad(val, width) {
  return String(val).slice(0, width).padEnd(width);
}

/** Truncate a string to maxLen, adding ellipsis if needed */
function truncate(str, maxLen) {
  if (!str) return '';
  return str.length > maxLen ? str.slice(0, maxLen - 1) + '…' : str;
}

module.exports = { buildReport };
