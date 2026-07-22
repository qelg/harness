# Eventing Engine Proposal

This document proposes the next core abstraction for the harness: the eventing engine. The rest of the system should be built around it.

## Core Idea

The core component is an event service:

```text
producer -> event service -> durable SQLite event log -> dispatch/subscription -> plugins/API/stream clients
```

The event service is the only writer to its SQLite event database file. API routes and plugins do not write directly to the event log. They submit one event or a batch of events to the event service. The event service assigns IDs, persists the events, commits the transaction, and only then acknowledges the write.

State tables such as sessions, messages, OAuth tokens, and tool metadata can still exist, but they should be projections or plugin-owned state derived from events. The event log is the source of sequencing and delivery.

## Event ID

Each event ID is assigned by the event service.

Format:

```text
milliseconds_since_epoch
```

The ID is a 64-bit integer. It is timestamp-like, ordered, and unique. If multiple events are created in the same millisecond, the service increments the last assigned ID by 1 until it is unique.

Algorithm:

```python
candidate = current_epoch_ms()
event_id = max(candidate, last_event_id + 1)
last_event_id = event_id
```

On startup, the event service initializes `last_event_id` from:

```sql
SELECT COALESCE(MAX(id), 0) FROM events;
```

This gives stable ordering across restarts while keeping IDs human-inspectable.

## Persistence And Ack

Write semantics:

1. Producer submits an event or batch.
2. Event service assigns IDs to all events.
3. Event service inserts all events into SQLite in one transaction.
4. Transaction commits.
5. Only after commit does the producer receive success.
6. Only after commit does the service dispatch events to subscribers/plugins.

If commit fails, no event is acknowledged.

Batch writes are atomic by default. Either all events in the batch are persisted and acknowledged, or none are.

## Suggested SQLite Schema

The event log should live in its own SQLite database file, for example:

```text
.harness/events.db
```

Only the event service opens this database for writes.

### `events`

```sql
CREATE TABLE events (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL,
  producer TEXT,
  causation_id INTEGER,
  correlation_id INTEGER,
  durable INTEGER NOT NULL DEFAULT 1
);
```

| Column | Meaning |
| --- | --- |
| `id` | Monotone timestamp ID assigned by the event service. |
| `name` | Event type, for example `chat.message.user.created`. |
| `payload_json` | Event payload as JSON object. |
| `created_at_ms` | Wall-clock timestamp in milliseconds. Usually equal to `id` unless ID had to be bumped for uniqueness. |
| `producer` | Component that submitted the event, for example `api`, `llm-provider-runner`, `podman-tool`. |
| `causation_id` | Event that directly caused this event. |
| `correlation_id` | Root event tying a workflow together. |
| `durable` | Whether the event belongs to the durable log. Initial proposal: all rows in this table are durable; this flag leaves room for import/replay policy. |

### `event_tags`

```sql
CREATE TABLE event_tags (
  event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  tag TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (event_id, tag, value)
);

CREATE INDEX idx_event_tags_tag_value_event
  ON event_tags(tag, value, event_id);
```

Tags are first-class filter keys. They should be small strings, not arbitrary large payload fragments.

### `event_subscriptions`

This is optional for the first implementation, but useful for durable plugin cursors.

```sql
CREATE TABLE event_subscriptions (
  subscriber TEXT PRIMARY KEY,
  last_acked_event_id INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL
);
```

Plugins can process from their own cursor after restart. A plugin should acknowledge its cursor only after its side effect or projection write succeeds.

## Tags

Events have a name and zero or more tags. Tags are used for filtering, routing, and stream subscriptions.

Core event names and tag names live in `llm_harness/core/types.py`. Specific core event names have required tags. Unknown plugin event names are allowed and have no core-required tags unless a plugin registers stricter rules later.

New chat sessions should use a generated session tag value from `new_session_id()`, for example:

```text
session=sess_q7sU3FhiynSAhxlCmQgU11IR
chat=sess_q7sU3FhiynSAhxlCmQgU11IR
```

Recommended common tags:

| Tag | Example | Meaning |
| --- | --- | --- |
| `session` | `42` | Session ID. |
| `chat` | `42` | Alias for session-scoped chat workflows if useful for clients. |
| `role` | `user` | Message role. |
| `provider` | `openrouter` | LLM provider. |
| `model` | `openai/gpt-4.1-mini` | LLM model. |
| `tool` | `podman-shell` | Tool name. |
| `container` | `llm-harness-session-42` | Container target/name. |
| `project` | `project-a` | User/project tag inherited from session tags. |
| `oauth_provider` | `openai-codex` | Auth provider. |
| `token_subject` | `user-123` | OAuth/OIDC subject. |
| `stream` | `abc123` | Stream/run ID for transient or chunked workflows. |
| `run` | `llm-run-...` | Worker run ID. |
| `status` | `requested` | High-level state for dashboards. |

Session tags should be copied onto session-scoped events with a namespace like:

```text
session_tag:<tag>=true
```

For example:

```text
session_tag:project-a=true
```

This makes “route all events for project-a” independent of a later lookup.

## Initial Event Types

Names should be stable, dotted, and domain-oriented. Plugins may register more later.

### Event Service

| Name | Durable | Tags | Payload |
| --- | --- | --- | --- |
| `event.batch.accepted` | Yes | `producer` | Batch metadata, count, first/last event ID. |
| `event.subscriber.acked` | Yes | `subscriber` | Subscriber name and acked event ID. |
| `event.subscriber.failed` | Yes | `subscriber` | Subscriber name, failed event ID, error summary. |

`event.batch.accepted` is optional. It can be useful for audit, but it should not create recursive complexity in the first version.

### Sessions

| Name | Durable | Tags | Payload |
| --- | --- | --- | --- |
| `session.created` | Yes | `session`, `session_tag:*` | Session title and tags. |
| `session.updated` | Yes | `session`, `session_tag:*` | Changed fields. |
| `session.tag.added` | Yes | `session`, `session_tag:*` | Added tag. |
| `session.tag.removed` | Yes | `session`, `session_tag:*` | Removed tag. |
| `session.archived` | Yes | `session`, `session_tag:*` | Archive reason, if any. |

### Chat Messages

| Name | Durable | Tags | Payload |
| --- | --- | --- | --- |
| `chat.message.create_requested` | Yes | `session`, `role=user`, `provider`, `model`, `session_tag:*` | User input and requested provider/model. |
| `chat.message.user.created` | Yes | `session`, `role=user`, `session_tag:*` | Persisted user message content/metadata. |
| `chat.message.assistant.created` | Yes | `session`, `role=assistant`, `provider`, `model`, `run`, `session_tag:*` | Persisted final assistant message content/metadata for an LLM run. |
| `chat.message.tool.created` | Yes | `session`, `role=tool`, `tool`, `run`, `session_tag:*` | Persisted tool output message content/metadata. |
| `chat.message.create_failed` | Yes | `session`, `provider`, `model`, `session_tag:*` | Error summary. |

The API should usually emit `chat.message.create_requested`. A projection or message plugin persists the message and emits one of the role-specific `chat.message.*.created` events.

For a simpler first implementation, the API can persist the message projection and emit `chat.message.user.created`, but the target architecture should prefer command/request events first.

### LLM Runs

| Name | Durable | Tags | Payload |
| --- | --- | --- | --- |
| `llm.model.selected` | Yes | `provider`, `model`, optional `session`, `session_tag:*` | Current model selection. Without `session` it is the global default; with `session` it overrides the global selection for that session. |
| `llm.run.requested` | Yes | `session`, `provider`, `model`, `run`, `session_tag:*` | Input message ID/history policy. |
| `llm.run.started` | Yes | `session`, `provider`, `model`, `run`, `session_tag:*` | Provider/model and run metadata. |
| `llm.run.failed` | Yes | `session`, `provider`, `model`, `run`, `session_tag:*` | Error type/message/retryability. |
| `llm.delta` | No by default | `session`, `provider`, `model`, `run`, `session_tag:*` | Text delta and sequence number. |

For the first version, `chat.message.assistant.created` is the successful completion event for an LLM run. A separate `llm.run.completed` event is only worth adding if we later need run-level metadata without creating a message, or if a worker needs to mark completion independently from message projection.

`llm.delta` should normally be transient because writing every token chunk to SQLite is expensive and rarely useful after the stream completes. If a caller wants resumable streams, use chunk coalescing:

- Write every N milliseconds, not every token.
- Or write `llm.output.snapshot` events with cumulative content.
- Or store the final assistant message only, which is the recommended default.

### Tools

| Name | Durable | Tags | Payload |
| --- | --- | --- | --- |
| `tool.call.requested` | Yes | `session`, `tool`, `run`, `session_tag:*` | Tool input. |
| `tool.call.started` | Yes | `session`, `tool`, `run`, `container`, `session_tag:*` | Runtime target metadata. |
| `tool.stdout.delta` | No by default | `session`, `tool`, `run`, `container`, `stream`, `session_tag:*` | Stdout chunk and sequence number. |
| `tool.stderr.delta` | No by default | `session`, `tool`, `run`, `container`, `stream`, `session_tag:*` | Stderr chunk and sequence number. |
| `tool.call.completed` | Yes | `session`, `tool`, `run`, `container`, `session_tag:*` | Exit code, output message ID, metadata. |
| `tool.call.failed` | Yes | `session`, `tool`, `run`, `container`, `session_tag:*` | Error type/message/retryability. |

Like LLM deltas, stdout/stderr deltas should normally be transient unless debugging or audit mode is enabled.

### Containers

| Name | Durable | Tags | Payload |
| --- | --- | --- | --- |
| `container.requested` | Yes | `session`, `container`, `session_tag:*` | Requested image/mount policy. |
| `container.started` | Yes | `session`, `container`, `session_tag:*` | Image, mounts, labels. |
| `container.reused` | Yes | `session`, `container`, `session_tag:*` | Existing container selected. |
| `container.failed` | Yes | `session`, `container`, `session_tag:*` | Error summary. |
| `container.removed` | Yes | `container` | Cleanup reason. |

### Auth

| Name | Durable | Tags | Payload |
| --- | --- | --- | --- |
| `auth.oauth.login_requested` | Yes | `oauth_provider` | Flow type and redirect/device metadata. |
| `auth.oauth.device_code.created` | Yes | `oauth_provider` | Device code row ID, user code, verification URL, expiry. |
| `auth.oauth.token.created` | Yes | `oauth_provider`, `token_subject` | Token row ID, scopes, expiry. No token secret. |
| `auth.oauth.token.refreshed` | Yes | `oauth_provider`, `token_subject` | Token row ID, new expiry. No token secret. |
| `auth.oauth.token.refresh_failed` | Yes | `oauth_provider`, `token_subject` | Error summary, whether re-login is required. |
| `auth.oauth.revoked` | Yes | `oauth_provider`, `token_subject` | Revocation metadata. |

Never put access tokens or refresh tokens in event payloads. Store secrets in plugin-owned token tables or an external secret store.

### API

If the HTTP API remains a plugin, its events should describe accepted commands rather than internal handler details.

| Name | Durable | Tags | Payload |
| --- | --- | --- | --- |
| `api.request.accepted` | Yes | `route`, `session?`, `producer=api` | Optional request metadata, batch ID. |
| `api.request.rejected` | Yes | `route`, `producer=api` | Validation error summary. |

These are optional for normal operation. They are useful for audit mode.

## Plugin Extension Model

Plugins can extend the event system by registering:

- event type metadata
- tag extraction rules
- handlers/subscribers
- projections
- durability preferences for their own events

Suggested plugin registration shape:

```python
def register(registry):
    registry.add_event_type(
        name="my_plugin.thing.created",
        durable=True,
        required_tags=["session", "my_plugin_kind"],
    )
    registry.add_event_handler(
        name="my-plugin-worker",
        filters={"name": ["my_plugin.thing.created"], "tag:session": "*"},
        handler=handle_event,
    )
```

The core should not require all possible event types to be known ahead of time. It should enforce only generic event invariants:

- valid name
- JSON object payload
- valid tags
- service-assigned ID
- durable write before ack

## Filtering

A stream or subscriber should be able to filter by:

- event name exact match
- event name prefix, for example `llm.*`
- minimum event ID, for replay
- tag equality, for example `session=42`
- multiple tag constraints, for example `session=42 AND provider=openrouter`

Suggested stream API:

```http
GET /events/stream?since=1784740000000&name=llm.*&tag=session:42
```

Suggested batch write API:

```http
POST /events
{
  "events": [
    {
      "name": "chat.message.create_requested",
      "tags": {"session": "42", "provider": "mock-llm", "model": "test"},
      "payload": {"content": "hello"}
    }
  ]
}
```

Response:

```json
{
  "accepted": [
    {"id": 1784740000001, "name": "chat.message.create_requested"}
  ]
}
```

## Durable Versus Transient

Recommended rule:

- Persist decisions, requests, starts, completions, failures, and final outputs.
- Do not persist high-frequency deltas by default.
- Allow plugins to opt into durable chunks for audit/debug modes.

This keeps restarts graceful without turning SQLite into a token-stream sink.

For restart recovery:

- Durable events are replayed from the event DB by ID.
- Plugin subscribers resume from `event_subscriptions.last_acked_event_id`.
- In-flight transient streams are not recovered.
- Any workflow that matters must have a durable `*.requested`, `*.started`, and terminal `*.completed` or `*.failed` event.

## First Implementation Milestone

1. Introduce `EventService` with `append`, `append_batch`, `stream`, and `replay`.
2. Move event persistence into `events.db`.
3. Keep existing state DB temporarily as projections.
4. Convert API plugin to submit event batches instead of writing state directly.
5. Convert LLM and tool workers to subscribed event handlers with cursors.
6. Keep `llm.delta` and `tool.*.delta` transient unless explicitly configured durable.
