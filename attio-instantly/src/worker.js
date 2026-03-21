/**
 * Cloudflare Worker — Instantly → Attio Sync
 *
 * Receives Instantly webhook events and upserts the contact in Attio.
 * Logic: try CREATE → if uniqueness_conflict → PATCH the existing record.
 *
 * Required secret (set via: wrangler secret put ATTIO_API_KEY):
 *   ATTIO_API_KEY — Attio Bearer token
 *
 * Optional secret:
 *   WEBHOOK_SECRET — if set, requests must include header X-Webhook-Secret: <value>
 */

export default {
  async fetch(request, env) {
    if (request.method !== 'POST') {
      return json({ error: 'Method not allowed' }, 405);
    }

    if (env.WEBHOOK_SECRET) {
      const incoming = request.headers.get('X-Webhook-Secret') ?? '';
      if (incoming !== env.WEBHOOK_SECRET) {
        return json({ error: 'Unauthorized' }, 401);
      }
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return json({ error: 'Invalid JSON body' }, 400);
    }

    // Normalise inputs
    const firstName = str(body.firstName);
    const lastName  = str(body.lastName);
    const leadEmail = str(body.lead_email ?? body.email).toLowerCase();
    const jobTitle  = str(body.jobTitle);
    const campaign  = str(body.campaign_name);
    const eventType = str(body.event_type);
    const timestamp = str(body.timestamp) || new Date().toISOString();
    const seqStep4  = Number.isFinite(Number(body.step)) ? Number(body.step) : 0;

    if (!leadEmail) {
      return json({ error: 'Missing required field: lead_email' }, 400);
    }

    const instantlyLeadStatus =
      !eventType                        ? 'Unknown'      :
      eventType === 'email_replied'     ? 'Engaged'      :
      eventType === 'email_opened'      ? 'Interested'   :
      eventType === 'lead_unsubscribed' ? 'Unsubscribed' :
      eventType === 'email_bounced'     ? 'Invalid'      :
                                          'Active';

    const fullName = [firstName, lastName].filter(Boolean).join(' ');

    // Build Attio payloads
    const sharedFields = {
      last_event:            [{ value: eventType || 'email_sent' }],
      last_activity_date:    [{ value: timestamp }],
      seq_step_4:            [{ value: seqStep4 }],
      instantly_lead_status: [{ value: instantlyLeadStatus }],
    };

    const attioCreateBody = {
      data: {
        type: 'object_record',
        values: {
          ...(fullName && { name: [{ first_name: firstName || undefined, last_name: lastName || undefined, full_name: fullName }] }),
          email_addresses: [{ email_address: leadEmail }],
          ...(jobTitle  && { job_title:       [{ value: jobTitle }] }),
          ...(campaign  && { campaign_name_5: [{ value: campaign }] }),
          ...sharedFields,
        },
      },
    };

    const attioUpdateBody = { data: { values: sharedFields } };

    const ATTIO_BASE = 'https://api.attio.com/v2';
    const headers = {
      'Authorization': `Bearer ${env.ATTIO_API_KEY}`,
      'Content-Type':  'application/json',
    };

    // Try CREATE
    const createRes  = await fetch(`${ATTIO_BASE}/objects/people/records`, {
      method: 'POST', headers, body: JSON.stringify(attioCreateBody),
    });
    const createData = await createRes.json();

    if (createRes.ok) {
      console.log(`Created Attio record for ${leadEmail}`);
      return json({ status: 'created', id: createData.data?.id?.record_id });
    }

    // Handle uniqueness conflict
    const errMsg    = createData.error?.message ?? JSON.stringify(createData);
    const isConflict = errMsg.includes('uniqueness_conflict');

    if (!isConflict) {
      console.error(`Attio create failed (non-conflict): ${errMsg}`);
      return json({ status: 'error', error: createData }, 500);
    }

    // Extract record ID from error message
    let recordId = errMsg.match(/Conflicting record IDs:\s*([0-9a-f-]{36})/i)?.[1];

    // Fallback: query by email
    if (!recordId) {
      const queryRes  = await fetch(`${ATTIO_BASE}/objects/people/records/query`, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          filter: { email_addresses: { $email_address: { $eq: leadEmail } } },
          limit: 1,
        }),
      });
      const queryData = await queryRes.json();
      recordId = queryData.data?.[0]?.id?.record_id;
    }

    if (!recordId) {
      return json({ status: 'error', error: 'Could not resolve conflict record ID' }, 500);
    }

    // PATCH existing record
    const updateRes  = await fetch(`${ATTIO_BASE}/objects/people/records/${recordId}`, {
      method: 'PATCH', headers, body: JSON.stringify(attioUpdateBody),
    });
    const updateData = await updateRes.json();

    if (updateRes.ok) {
      console.log(`Updated Attio record ${recordId} for ${leadEmail}`);
      return json({ status: 'updated', id: recordId });
    }

    return json({ status: 'error', error: updateData }, 500);
  },
};

function str(val) { return (val ?? '').toString().trim(); }
function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}
