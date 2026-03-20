/**
 * instantlyClient.js
 * Wrapper around the Instantly.ai API v2.
 * Handles authentication and raw HTTP calls only — no formatting.
 */

const axios = require('axios');
const config = require('./config');

const BASE_URL = 'https://api.instantly.ai/api/v2';

const client = axios.create({
  baseURL: BASE_URL,
  headers: {
    Authorization: `Bearer ${config.instantlyApiKey}`,
    'Content-Type': 'application/json',
  },
  timeout: 15000,
});

/**
 * Fetch per-campaign analytics for a specific date range.
 * @param {string} startDate  "YYYY-MM-DD"
 * @param {string} endDate    "YYYY-MM-DD"
 * @returns {Array} Array of campaign analytics objects
 */
async function getCampaignAnalytics(startDate, endDate) {
  const response = await client.get('/campaigns/analytics', {
    params: {
      start_date: startDate,
      end_date:   endDate,
    },
  });

  // API may return { data: [...] } or a plain array — handle both
  const body = response.data;
  if (Array.isArray(body)) return body;
  if (body && Array.isArray(body.data)) return body.data;
  return [];
}

/**
 * Fetch the all-time aggregate overview across all campaigns.
 * @returns {Object} Overview stats object
 */
async function getCampaignOverview() {
  const response = await client.get('/campaigns/analytics/overview');
  const body = response.data;
  // May be wrapped in { data: {...} } or returned directly
  if (body && body.data && typeof body.data === 'object') return body.data;
  return body || {};
}

module.exports = { getCampaignAnalytics, getCampaignOverview };
