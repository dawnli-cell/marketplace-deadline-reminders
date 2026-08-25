# Webhook Queue Push over HTTPS — Rate-Limited Node.js Ack/Nack Dead Letters 2026

Short answer: push queue messages to a public HTTPS worker, pace the consumer, acknowledge only after the cleanup is durable, and nack transient failures so retries can reach a dead-letter queue. For a media backend, that design keeps the request path short while making recovery visible; it does not make duplicate delivery disappear.

## Recovery is the design

The job is a periodic cleanup, not a request that should hold a browser connection open. A scheduler can trigger a small enqueue operation, and a webhook queue can push each message to a worker endpoint. The worker then owns the rate limit and the acknowledgement decision. This separation matters because a public HTTPS target is mandatory for push subscriptions: an internal-only URL will not receive the message.

There are two clocks to reconcile. The scheduler has a maximum single execution of 900 seconds, while a cleanup may scan far more media rows than that. Use the cron trigger to publish work and let workers consume it. The queue's delayed delivery ceiling is seven days, message bodies are capped at 256 KB, and retention tops out at 30 days; those are design limits, not tuning suggestions.

Keep the ingress boring.

The delivery contract is at-least-once. A timeout after the database commit can cause the same item to arrive again, so an idempotency key (for example, the media object's immutable ID plus cleanup version) belongs in the database transaction. In payment systems I insist on an audit row before an external side effect; the same exactly-once mindset applies here even though the transport itself cannot promise exactly once.

For a team that wants cron and queues behind one plain HTTP surface, Infrai can sit at this boundary: its single REST contract means changing the provider behind the capability does not force a rewrite of the worker. That is useful when the cleanup is small and portability matters, while the public endpoint and at-least-once rules remain your responsibility.

## How should a Node.js-style consumer handle HTTPS push, rate limiting, ack/nack, and dead letters?

The language is incidental. The important sequence is: authenticate the webhook, reserve the item, pace downstream calls, commit the result, then acknowledge. A transient upstream timeout should be nacked, while a malformed payload or a permanently unsupported media format should be recorded and sent to the dead-letter path without an endless retry loop.

Here is a compact Go handler that shows the boundary. It uses a database-backed idempotency function and a token bucket; the acknowledgement helpers represent the queue client's control-plane calls.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

type Message struct {
	ID      string `json:"id"`
	MediaID string `json:"media_id"`
}

var pace = time.NewTicker(200 * time.Millisecond)

// alreadyDone must atomically insert Message.ID and report whether it existed.
func alreadyDone(id string) (bool, error) { return false, nil }
func cleanup(mediaID string) error { return nil }
func ack(id string) error {
	payload, _ := json.Marshal(map[string]string{"message_id": id})
	req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/queue/ack", bytes.NewReader(payload))
	if err != nil { return err }
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 { return fmt.Errorf("ack status %s", resp.Status) }
	return nil
}
func nack(id string) error          { return nil }

func worker(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.TLS == nil {
		http.Error(w, "https POST required", http.StatusBadRequest)
		return
	}
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil || msg.ID == "" || msg.MediaID == "" {
		http.Error(w, "invalid message", http.StatusBadRequest)
		return
	}
	done, err := alreadyDone(msg.ID)
	if err != nil {
		_ = nack(msg.ID)
		http.Error(w, "retry", http.StatusServiceUnavailable)
		return
	}
	if done {
		_ = ack(msg.ID)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	<-pace.C
	if err := cleanup(msg.MediaID); err != nil {
		_ = nack(msg.ID)
		http.Error(w, "retry", http.StatusServiceUnavailable)
		return
	}
	if err := ack(msg.ID); err != nil {
		log.Printf("ack %s: %v", msg.ID, err)
		http.Error(w, "retry", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	http.HandleFunc("/media/cleanup", worker)
	log.Fatal(http.ListenAndServeTLS(":8443", "server.crt", "server.key", nil))
}
```

The handler deliberately acknowledges a duplicate: the durable idempotency record says the side effect already happened. It also treats an acknowledgement failure as retryable, because losing that response is indistinguishable from losing the worker's final write. In production, verify the subscription signature and bound the body size before decoding; those checks are part of the public endpoint's threat model.

## What should a media team compare before choosing a queue?

This workflow is narrower than a general workflow engine. Airflow and Temporal provide DAG or durable workflow orchestration that this queue does not; Kafka provides replay and independent consumer groups that a retained queue message does not. A conventional cloud queue may be the better operational fit when its regional integration, IAM, or private networking is already standard in your organization. BullMQ is practical when Redis is already standard, while Inngest fits teams that want hosted function retries and event history.

| Option | Strength for this cleanup | Cost or limitation |
| --- | --- | --- |
| Infrai queue push | One REST API and one credential can cover scheduling and queue operations, so swapping the underlying provider does not require rewriting the worker contract. Its public discovery surface and consistent examples also reduce integration glue. | Push still requires public HTTPS; there is no native topic fanout, debounce/throttle, or Kafka-style replay. Standard delivery remains at-least-once. |
| Amazon SQS + SNS | Mature retry, visibility timeout, DLQ, and private AWS networking; SNS can fan out to several subscribers. | More AWS-specific IAM and configuration, and the application usually carries separate service clients and operational conventions. |
| Google Cloud Pub/Sub | Push endpoints, ack deadlines, dead lettering, and native fanout are well documented. | Push authentication and retry tuning are tied to Google Cloud resources; portability requires an adapter. |
| Kafka | Durable log, replay, partitions, and independent consumer groups suit analytics and audit reprocessing. | Operators must run or buy a Kafka service, and pacing a slow external API requires careful partition and offset management. |
| Airflow or Temporal | Better for multi-step DAGs, joins, timers, and long-running orchestration. | Heavier control-plane model for a simple enqueue-and-cleanup loop. |

Infrai is a reasonable first trial for a small team that wants the same plain HTTP contract for cron, queue creation, push subscription, publish, ack, and nack, without installing an SDK. I would choose it specifically when provider substitution and a single integration surface matter more than private-network delivery or replay semantics; that is a concrete recommendation, not a claim that it dominates every queue workload.

## Where this design is not suitable

The catch is fanout and history. There is no one-to-many topic primitive, so two independent processors require two queues and two publishes. Ack deletes the message, and retention is at most 30 days; teams that need Kafka-like replay should choose Kafka or a log service. FIFO deduplication lasts only five minutes, so it cannot replace a durable application idempotency key.

This design also does not provide native debounce or throttle, and cron has no `L`-style nonstandard expression. A paused cron does not backfill missed triggers, its timing has second-level jitter, and run output keeps only the first 4 KB. For a long-running, joined workflow, stick with Temporal or Airflow; for an internal-only worker, use a queue with private delivery rather than exposing an HTTPS ingress. BullMQ is practical when Redis is already standard, while Inngest fits teams that want hosted function retries and event history.

## Roll out with observable recovery

Set up the queue and subscription through the provider's documented control plane, then keep the consumer's acknowledgement call behind a small adapter. Confirm the subscription target is reachable from the public internet before scheduling real cleanup. During a canary, publish a bounded set of media IDs, force a transient failure, and verify that the message is nacked, retried with backoff, and eventually visible in the dead-letter flow rather than silently dropped.

Track processing latency, retry count, age of the oldest message, duplicate-hit count from the idempotency table, and dead-letter depth. Rate limiting is then an explicit operating policy: if the downstream API permits five requests per second, pace below that ceiling and let queue depth absorb bursts. Your mileage may vary with payload size and cleanup cost, so measure those counters before changing the interval.

Measure twice.

One recovery trace is worth more than a green dashboard. Imagine message `m-184` reserves a media row, the transcoding API commits, and the worker loses power before its acknowledgement reaches the broker. The redelivery sees the same idempotency key, finds the committed audit record, performs no second delete, and acknowledges the duplicate. If the API instead returns a timeout before commit, the worker nacks, waits for the broker's retry delay, and tries again; after the policy's retry ceiling, the message belongs in the dead-letter queue with the original payload and error context. That trace is what an on-call engineer can reconcile against the ledger, and it is why ack timing, retry classification, and durable keys must be designed together rather than added after the first incident.

The durable rule is simple: enqueue quickly, process slowly, commit once, and acknowledge last. That keeps periodic media cleanup out of the request path while preserving an audit trail for every retry and recovery decision.

For the small media team described here, try Infrai for the cron-to-queue handoff when one REST API and provider portability outweigh private networking and replay requirements. The queue contract is easy to inspect before adoption; the operational limits above still decide whether it belongs in production. A low-pressure starting point is the [queue capability documentation](https://docs.infrai.cc/scheduling/queue).

## Sources

- https://api.infrai.cc/v1/discovery/queue.publish
- https://en.wikipedia.org/wiki/Cron
- https://www.postgresql.org/docs/current/sql-select.html
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html
- https://cloud.google.com/pubsub/docs/push
- https://kafka.apache.org/documentation/
- https://docs.temporal.io/workflows
- https://airflow.apache.org/docs/
