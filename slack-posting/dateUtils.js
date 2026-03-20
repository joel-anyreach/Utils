/**
 * dateUtils.js
 * All date arithmetic and formatting for the weekly report.
 */

/**
 * Returns the Monday–Sunday range of the previous full week,
 * relative to whatever day runReport() is called.
 *
 * Example: called on Friday Mar 13 →  Mon Mar 3 – Sun Mar 9
 */
function getPreviousWeekRange() {
  const now = new Date();

  // Find the most recent Sunday (day 0)
  const dayOfWeek = now.getDay(); // 0=Sun, 1=Mon … 6=Sat
  const daysToLastSunday = dayOfWeek === 0 ? 7 : dayOfWeek;

  const lastSunday = new Date(now);
  lastSunday.setDate(now.getDate() - daysToLastSunday);
  lastSunday.setHours(23, 59, 59, 0);

  // Monday is 6 days before that Sunday
  const lastMonday = new Date(lastSunday);
  lastMonday.setDate(lastSunday.getDate() - 6);
  lastMonday.setHours(0, 0, 0, 0);

  return { start: lastMonday, end: lastSunday };
}

/**
 * Format a Date as "YYYY-MM-DD" for Instantly API params.
 */
function toApiDate(dateObj) {
  return dateObj.toISOString().split('T')[0];
}

/**
 * Format the date range for the report header.
 * e.g. "Feb 3 – Feb 9, 2026"
 */
function toDisplayRange(start, end) {
  const opts = { month: 'short', day: 'numeric' };
  const startStr = start.toLocaleDateString('en-US', opts);
  const endStr   = end.toLocaleDateString('en-US', { ...opts, year: 'numeric' });
  return `${startStr} – ${endStr}`;
}

/**
 * Returns the date of the next Friday from now,
 * formatted as "Mon DD, YYYY" for the report footer.
 */
function getNextFriday() {
  const now = new Date();
  const day = now.getDay();
  const daysUntilFriday = day === 5 ? 7 : (5 - day + 7) % 7;
  const next = new Date(now);
  next.setDate(now.getDate() + daysUntilFriday);
  return next.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

module.exports = { getPreviousWeekRange, toApiDate, toDisplayRange, getNextFriday };
