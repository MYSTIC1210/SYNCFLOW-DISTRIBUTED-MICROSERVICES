# Sample Output — SyncFlow Distributed Microservices

## Gateway Startup

```
[otel] tracer provider initialized — exporting to http://jaeger:4318
[db] connected — pool: 20 max conns
[gateway] listening on :8080
[worker] starting 8 workers on queue "syncflow.tasks"
[worker] starting 8 workers on queue "syncflow.tasks" (replica 2)
```

## POST /tasks — Enqueue a task

```bash
curl -X POST http://localhost:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{"type":"echo","payload":{"message":"hello syncflow"}}'
```

```json
{
  "task_id": "a3f8c2e1-91b4-4d7e-b3c2-0f1a2b3c4d5e",
  "status": "queued"
}
```

## GET /tasks — List Results

```json
[
  {
    "id": "a3f8c2e1-91b4-4d7e-b3c2-0f1a2b3c4d5e",
    "type": "echo",
    "status": "success",
    "output": "{\"message\":\"hello syncflow\"}",
    "duration_ms": 3,
    "created_at": "2025-11-12T14:32:01Z",
    "updated_at": "2025-11-12T14:32:01Z"
  }
]
```

## Worker Processing Log

```
[worker-0] task a3f8c2e1 type=echo dispatched
[worker-0] task a3f8c2e1 success in 3ms
[worker-3] task b9d1a4f2 type=noop dispatched
[worker-3] task b9d1a4f2 success in 1ms
```

## RabbitMQ Management UI (localhost:15672)

```
Queue: syncflow.tasks
  Messages ready:     0
  Messages unacked:   0
  Message rate in:  124/s
  Message rate out: 124/s
  Consumers:         16  (8 workers × 2 replicas)

Exchange: syncflow.exchange  type=topic
  Bindings: task.* → syncflow.tasks
```

## GitHub Actions CI

```
✅ golangci-lint    — passed (0 issues)
✅ go test -race    — passed (24 tests, 0 failures)
   coverage: 81.4%
✅ docker build     — image built successfully
   Image: syncflow:abc1234 (18.2MB)
```

## Jaeger Trace (Distributed Tracing)

```
Trace: POST /tasks  duration=4ms
  ├── syncflow-router: POST /tasks          1ms
  ├── syncflow-gateway: PublishTask         2ms
  │   └── amqp: channel.Publish            1ms
  └── syncflow-worker: process-task         1ms
      └── handler: echo                    <1ms
```
