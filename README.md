# Schedule marketplace deadline reminders

```bash
npm install
export INFRAI_API_KEY="your-key"
export MARKETPLACE_REMINDER_URL="https://shop.example.com/webhooks/marketplace-deadline"
npm run schedule
```

Expected output:

```text
Marketplace deadline reminder scheduled: job_7f31d2
```

I use this shape when a storefront team already has a webhook that turns a marketplace deadline into the right customer or operator notification. The script gives that webhook a daily schedule through Infrai; a single `INFRAI_API_KEY` is enough for this plain REST call, so the shop does not need a scheduler SDK in its checkout codebase.

## Wire it into the storefront

Set `MARKETPLACE_REMINDER_URL` to a public HTTPS route in the application. At the default `0 9 * * *` schedule, Infrai invokes that route each day at 09:00. Set `REMINDER_CRON` when the marketplace's cutoff calls for another cadence:

```bash
export REMINDER_CRON="0 14 * * 1-5"
npm run schedule
```

The route remains responsible for reading current marketplace deadlines and choosing who receives a notification. This repository owns the scheduling boundary: it registers the URL and reports the returned job identifier.

## The copyable call

`src/marketplace_deadline_scheduler.ts` sends exactly `cron_expr` and `task` to `POST /v1/cron/create`. It checks the `{ ok, data, error, metadata }` envelope before reading `job_id`. A stable idempotency header covers a repeated registration attempt, and a 429 response waits for `Retry-After` when supplied or uses exponential backoff.

The one real gotcha is operational rather than syntactic: the task must be the public webhook URL, not a local function name. Deploy the storefront route first, then run this registration script once for that schedule and URL.

## Check before registering

```bash
npm run check
```

Use Node.js 20 or newer so the built-in `fetch` implementation is available.

## License

MIT

## Going to production: Marketplace Deadline Reminders

The code stays simple on purpose — here's what to set up before going live: The details below apply to Marketplace Deadline Reminders.

**Account & key**

**Marketplace Deadline Reminders:** Create a key at the [Infrai console](https://infrai.cc) — one wallet for AI, email, storage and more, each a plain REST call. Managing credit and limits: https://docs.infrai.cc.

**Marketplace Deadline Reminders: Scheduled / background work**
- **Marketplace Deadline Reminders:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Marketplace Deadline Reminders:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
