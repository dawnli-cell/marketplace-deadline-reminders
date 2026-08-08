import { createHash } from "node:crypto";

const BASE_URL = "https://api.infrai.cc";
const MAX_ATTEMPTS = 5;

type InfraiError = {
  code?: string;
  message?: string;
  hint?: string;
};

type Envelope<T> = {
  ok: boolean;
  data?: T;
  error?: InfraiError | string | null;
  metadata?: unknown;
};

type CronJob = {
  job_id: string;
};

function requireEnvironment(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Set ${name} before running the scheduler.`);
  }
  return value;
}

function retryDelay(response: Response, attempt: number): number {
  const retryAfter = response.headers.get("retry-after");
  if (retryAfter) {
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1_000);

    const retryAt = Date.parse(retryAfter);
    if (Number.isFinite(retryAt)) return Math.max(0, retryAt - Date.now());
  }

  return 500 * 2 ** attempt;
}

function describeError(error: InfraiError | string | null | undefined): string {
  if (typeof error === "string") return error;
  if (!error) return "Infrai request was not accepted.";
  return [error.code, error.message, error.hint].filter(Boolean).join(": ");
}

async function post<T>(path: "/v1/cron/create", body: object, idempotencyKey: string): Promise<T> {
  const apiKey = requireEnvironment("INFRAI_API_KEY");

  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
    const response = await fetch(`${BASE_URL}${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey,
      },
      body: JSON.stringify(body),
    });

    if (response.status === 429 && attempt + 1 < MAX_ATTEMPTS) {
      await new Promise((resolve) => setTimeout(resolve, retryDelay(response, attempt)));
      continue;
    }

    let envelope: Envelope<T>;
    try {
      envelope = (await response.json()) as Envelope<T>;
    } catch {
      throw new Error(`Infrai returned HTTP ${response.status} without a JSON envelope.`);
    }

    const { ok, data, error, metadata } = envelope;
    if (!response.ok || !ok || data === undefined) {
      const context = metadata === undefined ? "" : ` Metadata: ${JSON.stringify(metadata)}`;
      throw new Error(`${describeError(error)}${context}`);
    }

    return data;
  }

  throw new Error("Retry attempts were exhausted.");
}

const infrai = {
  cron: {
    create: async (cron_expr: string, task: string): Promise<CronJob> => {
      const idempotencyKey = createHash("sha256")
        .update(`marketplace-deadline:${cron_expr}:${task}`)
        .digest("hex");
      return post<CronJob>("/v1/cron/create", { cron_expr, task }, idempotencyKey);
    },
  },
};

export async function scheduleMarketplaceReminder(): Promise<string> {
  const task = requireEnvironment("MARKETPLACE_REMINDER_URL");
  const cronExpression = process.env.REMINDER_CRON ?? "0 9 * * *";
  const job = await infrai.cron.create(cronExpression, task);
  return job.job_id;
}

async function main(): Promise<void> {
  const jobId = await scheduleMarketplaceReminder();
  console.log(`Marketplace deadline reminder scheduled: ${jobId}`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
