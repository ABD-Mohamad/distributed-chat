# NexusChat — A Distributed Systems Capstone Project
### Complete Project Guide: Overview, Architecture, and Phase-by-Phase Build Instructions

**Stack:** Python, FastAPI, Docker / Docker Compose, Postgres, Redis, Kafka, RabbitMQ, gRPC, Nginx, Jaeger (OpenTelemetry)

---

## Table of Contents

0. [How to Use This Guide](#0-how-to-use-this-guide)
1. [Project Overview](#1-project-overview)
2. [Concept Coverage Map](#2-concept-coverage-map)
3. [High-Level Architecture](#3-high-level-architecture)
4. [Tech Stack & Why](#4-tech-stack--why)
5. [Repository Structure](#5-repository-structure)
6. [Prerequisites & Environment Setup](#6-prerequisites--environment-setup)
7. [Phase 0 — Skeleton & Compose Baseline](#7-phase-0--skeleton--compose-baseline)
8. [Phase 1 — MVP: One Service, One DB, Real Chat](#8-phase-1--mvp-one-service-one-db-real-chat)
9. [Phase 2 — Service Split, gRPC, API Gateway](#9-phase-2--service-split-grpc-api-gateway)
10. [Phase 3 — Load Balancing, Sharding, Replication, Caching](#10-phase-3--load-balancing-sharding-replication-caching)
11. [Phase 4 — Consensus & Coordination](#11-phase-4--consensus--coordination)
12. [Phase 5 — Reliability & Observability](#12-phase-5--reliability--observability)
13. [Phase 6 — Messaging, Flash-Sale Shop & Exam Specials](#13-phase-6--messaging-flash-sale-shop--exam-specials)
14. [Suggested Timeline](#14-suggested-timeline)
15. [Stretch Goals / What to Cut If Short on Time](#15-stretch-goals--what-to-cut-if-short-on-time)
16. [Appendix: Concept → File Map](#16-appendix-concept--file-map)

---

## 0. How to Use This Guide

This guide is organized into **phases**, not "features." Each phase:

- Adds new services/infrastructure to the `docker-compose.yml`
- Implements 1–2 parts of the distributed systems study guide
- Ends with an **acceptance checklist** — concrete tests you run yourself (kill a container, flood an endpoint, disconnect a network) to *prove* the concept actually works, not just that the code compiles

Do not skip ahead. Phase 3 (sharding/replication) assumes the service split from Phase 2 exists. Phase 4 (Raft/Gossip) assumes you have multiple chat-service replicas from Phase 3. Each phase is a fully working system on its own — you can stop after any phase and still have something real.

---

## 1. Project Overview

**NexusChat** is a small but real distributed chat platform with a flash-sale shop bolted onto it. The combination of "chat" and "limited-stock purchase" is deliberate: chat gives you natural scenarios for eventual consistency, fan-out, and WebSocket state, while the shop gives you natural scenarios for strong consistency, locking, and overselling — so the project forces you to make and defend real CAP trade-offs instead of using one consistency model everywhere.

**What the finished system does:**
- Users register/login (JWT) and join group chats
- Messages are delivered in real time over WebSocket, stored durably, and survive a server restart without the user noticing (session resumption)
- Chat data is **sharded** across multiple database nodes and each shard is **replicated** for failover
- A cluster of chat-service nodes elects a leader (**Raft**) and detects failures peer-to-peer (**Gossip**) instead of relying on one central registry
- A small in-app **shop** sells limited-quantity items (e.g. event badges) without overselling, using idempotent, retried, traced requests
- Every cross-service call is observable: you can pick one slow request and see exactly which hop was slow

**Why build this instead of a tutorial CRUD app:** almost every concept in the study guide maps to a specific, runnable piece of this system. You are not memorizing definitions — you're going to watch split-brain happen, fix it, and break it again on purpose.

---

## 2. Concept Coverage Map

| Study Guide Part | Concepts | Where in NexusChat |
|---|---|---|
| 1. Foundations | Latency, partial failure, concurrency, bottlenecks, fault tolerance | Everywhere — explicitly called out per phase |
| 2. RMI & RPC | Serialization, sync vs async, value vs reference | gRPC payloads (Phase 2), REST vs queue calls (Phase 6) |
| 3. gRPC & APIs | gRPC, Protobuf, HTTP/2, API Gateway, backpressure | Internal service calls (Phase 2), gateway (Phase 2) |
| 4. Load Balancing | Algorithms, health checks, consistent hashing | LB tier in front of chat-service (Phase 3) |
| 5. Data, Storage & Caching | Sharding, replication, SQL/NoSQL, caching, eviction | Chat shards + Redis cache (Phase 3) |
| 6. Consistency & Consensus | CAP, Raft, gossip, locks, fencing, HLC, split-brain | Coordination layer (Phase 4) |
| 7. Reliability & Observability | Retry/backoff, circuit breaker, idempotency, DLX, tracing | Cross-cutting layer (Phase 5) |
| 8. Messaging & Architecture | Kafka vs RabbitMQ, Outbox/CDC, microservices | Shop + notifications (Phase 6) |
| 9. Exam Specials | Fan-out, overselling, WebSocket resumption, E2EE | Shop + chat delivery (Phase 6) |

---

## 3. High-Level Architecture

```
                                   ┌─────────────┐
                                   │   Browser   │
                                   └──────┬──────┘
                                          │ HTTPS / WSS
                                   ┌──────▼──────┐
                                   │    Nginx    │  (TLS termination, reverse proxy)
                                   └──────┬──────┘
                                          │
                                   ┌──────▼──────┐
                                   │ API Gateway │  (FastAPI: JWT auth, rate limit, routing)
                                   └──┬───┬───┬──┘
                     ┌────────────────┘   │   └────────────────┐
                     │                    │                    │
              ┌──────▼─────┐      ┌───────▼───────┐    ┌───────▼──────┐
              │Auth Service │      │ Load Balancer │    │ Shop Service │
              │ (FastAPI)   │      │ (your existing│    │  (FastAPI)   │
              │  Postgres   │      │  LB project)  │    │   Postgres   │
              └─────────────┘      └───────┬───────┘    └───┬──────┬───┘
                                           │                │      │
                              ┌────────────┼────────────┐   │      │
                              │            │            │   │      │
                       ┌──────▼───┐ ┌──────▼───┐ ┌──────▼─┐ │   ┌──▼─────────┐
                       │Chat Node1│ │Chat Node2│ │Chat Node3│  │Outbox Relay│
                       │ (Raft +  │ │ (Raft +  │ │ (Raft +  │  └─────┬──────┘
                       │  Gossip) │ │  Gossip) │ │  Gossip) │        │
                       └────┬─────┘ └────┬─────┘ └────┬─────┘        │
                            │            │            │              │
                  ┌─────────┼────────────┼────────────┼──────┐       │
                  │         │            │            │      │       │
            ┌─────▼───┐┌────▼────┐ ┌─────▼───┐  ┌─────▼───┐  │       │
            │ Shard 0 ││Shard 0  │ │ Shard 1 │  │ Shard 1 │  │       │
            │ Primary ││Replica  │ │ Primary │  │ Replica │  │       │
            └─────────┘└─────────┘ └─────────┘  └─────────┘  │       │
                            │                                 │       │
                       ┌────▼────┐                            │       │
                       │  Redis  │ (cache, presence, locks,   │       │
                       │         │  session resumption)       │       │
                       └─────────┘                            │       │
                                                                ▼       ▼
                                                          ┌──────────────────┐
                                                          │ RabbitMQ / Kafka │
                                                          └─────────┬────────┘
                                                                    │
                                                          ┌─────────▼────────┐
                                                          │Notification Worker│
                                                          └───────────────────┘

         Cross-cutting: Jaeger (tracing) receives spans from every service above.
```

---

## 4. Tech Stack & Why

| Component | Choice | Why |
|---|---|---|
| Service framework | **FastAPI** | Native async, easy WebSocket support, plays nicely with gRPC and Pydantic for contracts |
| Containers | **Docker + Docker Compose** | Lets you run "a cluster" of 10+ processes on one laptop |
| Relational data | **Postgres** | Built-in streaming replication (for Phase 3's passive replication), strong consistency where you need it (shop) |
| Cache / locks / presence | **Redis** | Lua scripting (atomic decrement), pub/sub (presence), TTL keys (locks, session resumption) |
| Internal RPC | **gRPC + Protobuf** | Directly matches Part 3 of the study guide |
| Public API | **REST via API Gateway** | Public/mobile-friendly; gateway translates REST → internal gRPC |
| Reverse proxy / edge LB | **Nginx** | TLS termination, static handling, in front of the gateway |
| Internal load balancer | **Your existing FastAPI load balancer project** | Reuse it directly in front of chat-service replicas — this is the perfect home for it |
| Streaming log | **Kafka** | Chat message stream, replay, fan-out source of truth |
| Task queue | **RabbitMQ** | Shop orders, notifications, DLX, delayed retries |
| Tracing | **OpenTelemetry + Jaeger** | Both have first-class FastAPI instrumentation, free, runs in one container |

You already have hands-on experience with FastAPI, Docker Compose, circuit breakers, and consistent hashing from your load balancer project — this project is designed so that project becomes a *component* of this one rather than something you rebuild from scratch.

---

## 5. Repository Structure

Build this incrementally — don't create empty folders for things you haven't reached yet.

```
nexuschat/
├── docker-compose.yml
├── docker-compose.override.yml        # per-phase additions, explained below
├── .env
├── gateway/
│   ├── app/
│   │   ├── main.py
│   │   ├── auth.py                    # JWT verification
│   │   ├── rate_limit.py              # token bucket
│   │   └── routes/
│   ├── Dockerfile
│   └── requirements.txt
├── auth-service/
│   ├── app/
│   ├── Dockerfile
│   └── requirements.txt
├── chat-service/
│   ├── app/
│   │   ├── main.py
│   │   ├── ws_manager.py              # WebSocket connections + session resumption
│   │   ├── sharding.py                # consistent hash ring → shard lookup
│   │   ├── raft/
│   │   ├── gossip/
│   │   ├── hlc.py
│   │   └── grpc_server.py
│   ├── Dockerfile
│   └── requirements.txt
├── shop-service/
│   ├── app/
│   │   ├── main.py
│   │   ├── locking.py                 # Redis Lua atomic decrement
│   │   ├── outbox.py
│   │   └── idempotency.py
│   ├── Dockerfile
│   └── requirements.txt
├── notification-worker/
│   ├── app/
│   └── Dockerfile
├── outbox-relay/
│   ├── app/
│   └── Dockerfile
├── load-balancer/                     # your existing project, added as a submodule or copied in
├── proto/
│   └── chat.proto
├── nginx/
│   └── nginx.conf
└── infra/
    ├── postgres/
    │   ├── shard0-primary/
    │   ├── shard0-replica/
    │   ├── shard1-primary/
    │   └── shard1-replica/
    └── jaeger/
```

---

## 6. Prerequisites & Environment Setup

1. **Tools:** Docker + Docker Compose v2, Python 3.11+, `protoc` (Protobuf compiler) or `grpcio-tools` (installs it for you), `make` (optional, for shortcuts), a REST client (Postman/Insomnia/`httpie`), `websocat` or a small browser test page for WebSocket testing.
2. **Accounts/services:** none — everything runs locally in containers. No cloud dependency needed.
3. **Resource check:** by Phase 6 you'll be running roughly 15–18 containers (gateway, auth, 3× chat nodes, shop, notification worker, outbox relay, load balancer, 4× Postgres, Redis, Kafka (+Zookeeper or KRaft), RabbitMQ, Jaeger, Nginx). 8 GB RAM minimum; 16 GB is comfortable. If you're resource-constrained, scale chat-service replicas down to 2 and use a single-broker Kafka — call this out explicitly in your README as a documented simplification (this is also a legitimate exam-style trade-off to discuss: "in production you'd want replication factor 3, here I used 1 due to local resource limits").
4. **Initial repo setup:**
   ```bash
   mkdir nexuschat && cd nexuschat
   git init
   mkdir gateway auth-service chat-service shop-service notification-worker outbox-relay nginx infra proto
   ```

---

## 7. Phase 0 — Skeleton & Compose Baseline

**Goal:** get a `docker-compose.yml` that boots an empty gateway + one empty chat-service, talking to each other, with nothing distributed yet. This phase is about plumbing, not concepts.

### Steps

1. Write a minimal FastAPI app in `gateway/app/main.py` with one `/health` endpoint.
2. Write a minimal FastAPI app in `chat-service/app/main.py` with one `/health` endpoint.
3. Dockerfile for each (standard `python:3.11-slim` base, `pip install -r requirements.txt`, `uvicorn app.main:app --host 0.0.0.0 --port 8000`).
4. Base `docker-compose.yml`:
   ```yaml
   services:
     gateway:
       build: ./gateway
       ports: ["8080:8000"]
       depends_on: [chat-service]
     chat-service:
       build: ./chat-service
       ports: ["8001:8000"]
   ```
5. `docker compose up --build` and confirm both `/health` endpoints respond.
6. Set up a shared `.env` for ports/secrets so you're not hardcoding them later.

### Acceptance Checklist
- [ ] `docker compose up` boots both services with no errors
- [ ] `curl localhost:8080/health` and `curl localhost:8001/health` both return 200
- [ ] You have a `.gitignore` excluding `.env`, `__pycache__`, `*.pyc`

---

## 8. Phase 1 — MVP: One Service, One DB, Real Chat

**Goal:** a working chat with no distribution yet — one chat-service, one Postgres, WebSocket delivery, basic auth. This is your **baseline** for "is the system simpler/faster without all the distributed-systems machinery, and what do we lose by not having it?" — write that comparison down, you'll want it later.

*Study guide concepts: Part 1 — latency, partial failure, concurrency.*

### Steps

1. **Auth (simplified for now):** add a `users` table to chat-service's own Postgres (you'll split this out in Phase 2). Issue JWTs on login (`python-jose` or `pyjwt`).
2. **Data model:**
   ```sql
   CREATE TABLE chats (id UUID PRIMARY KEY, name TEXT, created_at TIMESTAMPTZ);
   CREATE TABLE chat_members (chat_id UUID, user_id UUID);
   CREATE TABLE messages (id UUID PRIMARY KEY, chat_id UUID, sender_id UUID, body TEXT, sent_at TIMESTAMPTZ);
   ```
3. **WebSocket endpoint:** `/ws/{chat_id}` — on connect, register the socket in an in-memory `dict[chat_id, set[WebSocket]]`; on message, persist to Postgres, then broadcast to all connected sockets for that chat.
4. **Concurrency demo (Part 1):** write a small load test that has 50 simulated users send messages to the same chat simultaneously. Confirm no message is lost or corrupted (this validates your DB writes are safe under concurrent access before you add any distributed complexity).
5. **Partial failure demo (Part 1):** kill the Postgres container mid-conversation. Confirm the chat-service returns a clear error rather than hanging forever, and reconnecting Postgres lets it recover. This is your first hands-on "partial failure leaves the system in an unknown state" moment — write down what state was ambiguous (was the last message saved or not?).

### Acceptance Checklist
- [ ] Two browser tabs (or two WebSocket clients) in the same chat see each other's messages in real time
- [ ] Messages persist across a chat-service restart
- [ ] You have a written note (a few sentences) describing what broke when Postgres went down, and why — this becomes the motivation for replication in Phase 3

---

## 9. Phase 2 — Service Split, gRPC, API Gateway

**Goal:** break the monolith into `auth-service`, `chat-service`, and an `api-gateway` that's the only public entry point. Internal calls between gateway and services go over gRPC.

*Study guide concepts: Part 2 (RMI/RPC contrast), Part 3 (gRPC, Protobuf, HTTP/2, API Gateway, backpressure).*

### Steps

1. **Define the contract first** — `proto/chat.proto`:
   ```protobuf
   syntax = "proto3";
   package nexuschat;

   service ChatService {
     rpc SendMessage (SendMessageRequest) returns (SendMessageResponse);
     rpc GetHistory (HistoryRequest) returns (HistoryResponse);
     rpc SubscribeMessages (SubscribeRequest) returns (stream MessageEvent); // server streaming
   }

   message SendMessageRequest { string chat_id = 1; string sender_id = 2; string body = 3; string idempotency_key = 4; }
   message SendMessageResponse { string message_id = 1; string status = 2; }
   ```
   Generate code: `python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/chat.proto` and do this for each service that needs the stubs.
2. **Move auth out:** `auth-service` owns its own Postgres, issues JWTs, exposes `POST /login`, `POST /register` over REST (this is a public-facing service called through the gateway) and a gRPC `VerifyToken` method for *internal* calls (other services validate tokens without re-implementing JWT logic).
3. **Gateway becomes a real gateway:**
   - REST in (public), gRPC out (internal) — this is the REST-vs-RPC boundary from the study guide, made concrete in code
   - JWT validation on every protected route (call `auth-service.VerifyToken` over gRPC, or validate locally if you've distributed the public key — document which you chose and why)
   - Token-bucket rate limiting per user (simple Redis-backed counter is fine here; you'll build a fuller token bucket in Phase 5)
4. **Add the `SubscribeMessages` server-streaming RPC** so the gateway (or a future mobile client) can receive a live stream of messages instead of polling — this is your concrete example of gRPC streaming types beyond plain unary calls.
5. **HTTP/2 verification:** confirm gRPC traffic is actually using HTTP/2 multiplexing (you can verify with `grpcurl` or by inspecting connection counts — note in your README how many TCP connections a busy chat would need under plain HTTP/1.1 vs the one persistent HTTP/2 connection gRPC uses).
6. **Backpressure demo:** in `SubscribeMessages`, simulate a slow consumer (add an artificial delay before processing each streamed event) and a fast producer (blast 1000 messages). Use a bounded queue between producer and the gRPC stream writer, and observe what happens when it fills — log when you start shedding/blocking instead of unboundedly buffering. This is your hands-on backpressure example (the IoT-100k-vs-DB-20k scenario from the cheat sheet, just in your own system).

### Acceptance Checklist
- [ ] Browser/REST client never talks gRPC directly — only the gateway does
- [ ] A request with an invalid/expired JWT is rejected at the gateway, before it reaches chat-service
- [ ] You can demonstrate the streaming RPC delivering messages without polling
- [ ] You have a short written explanation of REST vs gRPC vs RMI, in your own words, referencing actual code you wrote (this is exam-tip material — "explain serialization to a CEO" — write your own version)

---

## 10. Phase 3 — Load Balancing, Sharding, Replication, Caching

**Goal:** run **3 chat-service replicas**, split message storage across **2 shards** (each with a primary + replica), and add Redis caching with a real eviction policy. This is the densest phase — take it slowly.

*Study guide concepts: Part 4 (load balancing, consistent hashing), Part 5 (sharding, replication, caching, eviction, CDN concept).*

### Steps — Load Balancing

1. **Bring in your existing load balancer project** as the `load-balancer` service, placed between the gateway and the chat-service replicas (gateway → load-balancer → chat-service-1/2/3).
2. Configure it to route by **consistent hashing on `chat_id`** so the same conversation always lands on the same chat-service replica (this gives you cache locality for free in the next step) — you already built this ring logic, so this is mostly wiring + config, not new code.
3. Add **health checks**: kill `chat-service-2` mid-traffic and confirm the load balancer routes around it within your configured check interval. If your LB project doesn't yet have active health checks, this is the moment to add them.
4. **Document the algorithm choice**: write down *why* consistent hashing beats plain Round Robin here (sticky conversations) — and where you'd switch to Least-Connections or Power-of-Two-Choices instead (a chat node with a stuck WebSocket holding many slow connections).

### Steps — Sharding & Replication

5. Stand up 4 Postgres containers: `shard0-primary`, `shard0-replica`, `shard1-primary`, `shard1-replica`.
6. Configure Postgres **streaming replication** (primary writes WAL, replica streams it via `pg_basebackup` + `recovery.conf`/`standby.signal`). This is real passive (leader-follower) replication, not a simulation — Postgres's documentation on streaming replication is the canonical reference; budget real time for this step, it's the trickiest infra piece in the whole project.
7. **Shard router** in `chat-service/app/sharding.py`: `shard_id = consistent_hash(chat_id) % num_shards` (reuse your ring code again). All writes for a chat go to that shard's primary; reads can optionally go to the replica (and now you have a real reason to discuss replica lag — what if a user posts a message and immediately re-reads from a lagging replica and doesn't see it?).
8. **Failover drill:** kill `shard0-primary`. Manually promote `shard0-replica` (`pg_ctl promote` or `SELECT pg_promote()`), point the shard router's config at the new primary, and confirm chat continues. Time how long the outage window was — this number is exactly the kind of detail exam answers reward ("failover took ~12s, here's why").

### Steps — Caching

9. Add Redis as a read cache in front of `GetHistory`: cache key `history:{chat_id}:{page}`, populate on miss, invalidate/append on new message.
10. Configure **`maxmemory-policy allkeys-lru`** on the Redis container and demonstrate eviction by filling it past capacity and showing older conversations get evicted while recently-opened ones stay hot — this is the literal "last 50 chat messages, LRU keeps recent conversations hot" example from your study guide, but it's your own Redis instance doing it.
11. (Optional but a nice touch) Add a tiny static-asset CDN-style cache for avatar images using versioned URLs (`avatar_v2.png`) so you can demonstrate cache invalidation without a real CDN vendor.

### Acceptance Checklist
- [ ] Killing one chat-service replica doesn't drop active conversations on the other two
- [ ] The same `chat_id` always routes to the same shard, verified by logging which shard handled 100 sample requests
- [ ] You can kill a shard's primary, promote its replica, and the system recovers (document the recovery time)
- [ ] Redis cache hit/miss is visible in logs, and you can demonstrate LRU eviction happening
- [ ] Written note: which parts of this system are SQL-appropriate (shop, strong consistency) vs which could be NoSQL (chat history, if you wanted to swap later) — you don't have to build both, just justify your choice

---

## 11. Phase 4 — Consensus & Coordination

**Goal:** the 3 chat-service replicas elect a leader via a simplified Raft, detect each other's failures via gossip (not the load balancer's health check — a separate, peer-to-peer mechanism), and protect a shared resource with a distributed lock + fencing tokens. This phase is the heart of the study guide's hardest section (Part 6) — go slowly and test each piece in isolation before combining them.

### Steps — Raft (simplified leader election + a tiny replicated value)

1. Implement only what you need: **states** (`Follower`, `Candidate`, `Leader`), a **term counter**, and a **heartbeat timeout**. Each chat-service replica runs this as a background task.
2. **Leader election:** if a follower hears no heartbeat within a randomized timeout, it becomes a candidate, increments its term, votes for itself, and requests votes from the other two over a simple internal RPC (gRPC or even plain HTTP is fine here — the algorithm matters more than the transport). First to get a majority (2 of 3) becomes leader for that term.
3. **What the leader is *for*:** pick one small piece of real state to replicate through this — e.g., the leader is the only node allowed to assign a brand-new `chat_id` to a shard (an "AppendEntries"-style log of shard assignments, replicated to followers, committed once a majority ack). You don't need full Raft log compaction/snapshots — committing a short append-only log of assignments is enough to demonstrate "replicate via AppendEntries, commit on majority ack."
4. **Kill-the-leader drill:** kill whichever node is currently leader. Confirm a new leader is elected within your heartbeat timeout window, and that no two nodes ever believe they're leader for the *same term* at the same time.

### Steps — Gossip (failure detection, separate from Raft)

5. Each chat-service node periodically (e.g. every 1s) picks 2 random peers and exchanges its view of "who's alive" (a simple membership list with timestamps). Over a few rounds, all nodes converge on the same alive/dead view — without any central registry.
6. **Contrast drill (this is explicitly an exam comparison):** log how many messages gossip sends per second across the cluster as you add more simulated nodes (even fake/mock nodes for this measurement), versus how many a centralized heartbeat-to-one-registry approach would send. Write down the trade-off in your own words: gossip scales but has propagation delay and redundant messages; centralized is simpler but is a single point of failure.

### Steps — Distributed Locks & Fencing Tokens

7. Use Redis for a lock: `SET lock:shop:item123 <token> NX PX 5000` (atomic acquire with TTL).
8. **Fencing token:** the token is a monotonically increasing integer (store the "last issued token" in Redis with `INCR`). When a lock holder writes to the shop's stock table, it includes its token; the write path checks the token against the highest token *already seen* for that resource and rejects if it's lower.
9. **Break it on purpose:** acquire the lock, simulate a pause (GC pause / network blip) long enough for the TTL to expire and a second client to acquire the lock and write. Let the first client "wake up" and attempt its write — confirm the fencing check rejects it. This is the exact scenario from your study guide's exam tip; you want to be able to say "I made this happen and fixed it," not just describe it.

### Steps — Hybrid Logical Clock (HLC)

10. Implement a small HLC: each event timestamp is `(physical_time, logical_counter)`; when receiving an event from another node, bump your logical counter if needed to stay causally after it.
11. Use it to order messages arriving at the gateway from different chat-service nodes/shards before displaying them, and demonstrate (with an artificial clock skew on one container) that causal order is preserved even when wall-clock order would have been wrong.

### Acceptance Checklist
- [ ] Exactly one leader exists at any time, verified by logging leader state across all 3 nodes simultaneously during a kill-the-leader drill
- [ ] Gossip-based membership converges to the correct alive/dead set within a few rounds after you kill a node, with no central registry involved
- [ ] You can demonstrate a stale lock holder's write being rejected by a fencing token check
- [ ] You can demonstrate HLC producing correct causal ordering despite artificial clock skew
- [ ] Written note explaining your CAP choice for this system: chat = AP (eventual, prioritize availability), shop = CP (strong, prioritize correctness) — and *why* those are the right defaults for each

---

## 12. Phase 5 — Reliability & Observability

**Goal:** make failures graceful instead of catastrophic, and make every request traceable end to end.

*Study guide concepts: Part 7 — retry/backoff, circuit breaker, idempotency, outbox, DLX, tracing.*

### Steps

1. **Idempotency keys:** every `SendMessage` and shop `PurchaseItem` request carries a client-generated `idempotency_key`. The receiving service checks Redis for that key first; if seen, return the cached response instead of reprocessing. Test by firing the exact same request 5 times in a row and confirming only one message/order is created.
2. **Circuit breaker:** wrap the gateway's calls to the notification path (or any service-to-service call) with a circuit breaker FSM (Closed → Open → Half-Open) — you've already built this once in your load balancer project, so port that logic here as a small reusable library used by multiple services. Demonstrate: make the downstream service slow/erroring, confirm the breaker opens and starts failing fast instead of piling up blocked requests, then confirm it probes back to Closed once the downstream recovers.
3. **Retry + backoff + jitter:** for transient failures (e.g., a momentarily unavailable RabbitMQ connection), retry with exponential backoff and jitter, capped at a max number of attempts. Demonstrate the difference jitter makes by removing it temporarily and showing synchronized retry storms from multiple clients hitting the downstream at the same instant.
4. **Outbox pattern:** in `shop-service`, writing an order and writing an `OrderPlaced` outbox row happen in the **same Postgres transaction**. A separate `outbox-relay` worker polls the outbox table and publishes events to RabbitMQ, marking rows as sent. Kill the relay (or RabbitMQ) right after an order is placed but before publishing — confirm the event is *not lost*, just delayed, and gets published once the relay/queue comes back.
5. **DLX (Dead Letter Exchange):** configure a RabbitMQ queue with a DLX so messages that fail processing (or exceed a TTL) land in a dead-letter queue you can inspect and optionally replay. Build the "run a job after 5 minutes" delayed-queue trick using TTL + DLX for a "remind me about this flash sale" feature.
6. **Distributed tracing:** instrument every service with OpenTelemetry, exporting to a Jaeger container. Trace one request all the way from gateway → auth verification → chat-service → shard DB → Redis cache, and find it in the Jaeger UI as a single trace with all spans. Then deliberately slow down one hop and confirm you can spot exactly which span is the bottleneck — this is your hands-on version of "10 services, one request takes 15s, no error logs."

### Acceptance Checklist
- [ ] Sending the same request twice with the same idempotency key never creates a duplicate
- [ ] You have screenshots/logs of the circuit breaker opening and recovering
- [ ] You can kill the outbox relay mid-flow and prove no order event is lost
- [ ] A deliberately-failing message ends up in the dead-letter queue and you can inspect it
- [ ] You can find a single request's full trace across 3+ services in Jaeger and identify which span was the slow one

---

## 13. Phase 6 — Messaging, Flash-Sale Shop & Exam Specials

**Goal:** finish the shop with real concurrency protection, add Kafka as the durable chat event log, and implement the exam-special scenarios (fan-out, WebSocket resumption, simplified E2EE).

*Study guide concepts: Part 8 (Kafka vs RabbitMQ, microservices best practices), Part 9 (fan-out, overselling, WebSocket sessions, E2EE/forward secrecy).*

### Steps — Kafka for Chat

1. Every persisted message also gets published to a Kafka topic (`chat-messages`, partitioned by `chat_id` so a conversation's events stay ordered within a partition).
2. Configure for durability: `acks=all`, a reasonable replication factor for your cluster size, `min.insync.replicas`, and consumers that commit offsets **only after** successfully processing — demonstrate you understand *why* each setting matters by deliberately misconfiguring one (e.g. `acks=1`) and showing how a broker restart can lose an unacknowledged message, then fixing it.
3. Add one more consumer of this topic — an analytics counter (messages-per-chat-per-hour) — to demonstrate Kafka's core value: multiple independent consumers replaying the same durable stream.

### Steps — Flash-Sale Shop (Overselling Protection)

4. Stock check-and-decrement happens via a single Redis **Lua script** (atomic: check `stock > 0`, decrement, return success/fail in one round trip — no race window between check and decrement).
5. Load test: fire 200 concurrent purchase requests at 100 units of stock. Confirm exactly 100 succeed and 100 fail cleanly (no negative stock, no double-sells) — pair this with the idempotency key from Phase 5 so a retried/duplicated request never double-decrements.
6. Write up the **optimistic vs pessimistic locking** trade-off in your README, referencing this actual test: you chose the Redis-atomic (optimistic-style, no waiting) approach because conflicts are resolved in microseconds and pessimistic row locks would have meant 200 requests queuing behind a `SELECT...FOR UPDATE`.

### Steps — Fan-Out

7. Implement both:
   - **Write-time (push)** for small groups (≤ ~50 members): on send, write a copy of the message reference to each member's "inbox" (or just push directly over their WebSocket if connected).
   - **Read-time (pull)** for large broadcast channels: store the message once; each reader's client computes "what's new for me" at read time instead of fanning out 10,000 writes.
8. Build a simple rule that picks push vs pull based on member count, and document the trade-off with real numbers from your own test (e.g., "a 10,000-member broadcast channel did 1 write either way under the pull model vs. 10,000 writes under push — pull wins here").

### Steps — WebSocket Session Resumption

9. On WebSocket connect, issue a `session_token`; store `session_token → {user_id, chat_node}` in Redis with a TTL.
10. Kill the chat-service node a client is connected to. On reconnect, the client presents its `session_token`; the gateway/load balancer routes it to whichever node is now responsible (per your consistent-hash ring), that node looks up the session in Redis, and the client resumes without re-login or losing its place in the conversation.

### Steps — Simplified E2EE & Forward Secrecy

11. This does **not** need to be a production-grade Signal Protocol implementation — the goal is to demonstrate the *mechanism*, not ship a security product. Use a well-vetted crypto library (e.g. `PyNaCl`) rather than writing your own primitives:
    - Each user publishes a public key on registration
    - For a 1:1 chat, derive a shared session key via key exchange (X3DH-style: combine identity + a one-time "prekey")
    - **Forward secrecy demo:** rotate the session key forward after each message (a simple HKDF-based ratchet is enough). When a member leaves a group, rotate the group key so they can't derive future messages, and show that someone who captured an *old* key still cannot decrypt messages sent *after* rotation.
12. Document clearly in your README: "this is a simplified educational implementation of the Signal Protocol's key ideas (X3DH-style setup + ratcheting for forward secrecy), not a production security implementation." That distinction itself is good exam-answer material.

### Acceptance Checklist
- [ ] A killed-then-recovered Kafka broker doesn't lose an already-acknowledged message (with `acks=all` configured)
- [ ] The 200-vs-100-stock load test never oversells and never errors ungracefully
- [ ] You can show push fan-out for a small group and pull fan-out for a large channel, with your own numbers justifying the cutoff
- [ ] Killing a chat-service node mid-conversation and reconnecting resumes the session without re-login
- [ ] A message encrypted before a key rotation cannot be decrypted using the rotated (newer) key, and vice versa for messages after rotation

---

## 14. Suggested Timeline

This is sized for a working engineer doing this on evenings/weekends, not full-time:

| Phase | Focus | Rough Effort |
|---|---|---|
| 0 | Skeleton | 1 evening |
| 1 | MVP chat | 1 weekend |
| 2 | gRPC + gateway | 1 weekend |
| 3 | LB + sharding + replication + cache | 1.5–2 weekends (replication setup is the long pole) |
| 4 | Raft + Gossip + locks + HLC | 2 weekends (hardest phase — don't rush it) |
| 5 | Reliability + tracing | 1 weekend |
| 6 | Messaging + shop + exam specials | 1.5–2 weekends |

Roughly 8–10 weekends end to end. If you're using this primarily as exam prep rather than a polished portfolio piece, you can compress by doing the **acceptance checklist drills** even when the surrounding feature is minimal — the drills are where the understanding actually happens.

---

## 15. Stretch Goals / What to Cut If Short on Time

**If you need to cut scope**, cut in this order (each cut loses you the least exam coverage):
1. Simplify E2EE to "encrypt with a shared key, rotate it" — skip real X3DH key exchange
2. Use a single Kafka broker instead of a 3-broker cluster (document this as a known simplification)
3. Skip the analytics consumer — one consumer of the Kafka topic is enough to show durability
4. Reduce chat-service replicas from 3 to 2 (Raft still works with 2-of-2 majority, though 3 is more illustrative of "tolerate 1 failure")

**If you have extra time, stretch goals that add real value:**
- Add a second region (a second Docker Compose stack) and a simple GeoDNS-style router to demonstrate multi-region failover
- Implement CRDTs for a "typing indicator" or reaction-count feature to show a genuinely conflict-free data type in action
- Add Byzantine fault tolerance as a thought-experiment writeup (full BFT implementation is out of scope for a solo project, but you can simulate "one node lies about a vote" in your Raft implementation and show the system is *not* tolerant of it — which is itself the correct exam point: Raft tolerates crashes, not lies)

---

## 16. Appendix: Concept → File Map

Once the project is built, this table is your fastest way to "find the concept in your own code" for exam review — fill in the right-hand column as you go:

| Concept | File(s) once built |
|---|---|
| Consistent Hashing | `chat-service/app/sharding.py`, load-balancer config |
| Raft | `chat-service/app/raft/` |
| Gossip | `chat-service/app/gossip/` |
| Fencing Tokens | `shop-service/app/locking.py` |
| HLC | `chat-service/app/hlc.py` |
| Outbox Pattern | `shop-service/app/outbox.py`, `outbox-relay/app/main.py` |
| Circuit Breaker | shared library, used in `gateway/` and `chat-service/` |
| Idempotency | `shop-service/app/idempotency.py`, `chat-service` send-message handler |
| DLX | RabbitMQ config in `docker-compose.yml` / `infra/` |
| gRPC contract | `proto/chat.proto` |
| CAP choice | README section: "Consistency Model Decisions" |
| Fan-out (push/pull) | `chat-service/app/fanout.py` |
| WebSocket resumption | `chat-service/app/ws_manager.py` |
| E2EE / Forward Secrecy | `chat-service/app/crypto/` |

---

*End of guide. Build it phase by phase, run every acceptance checklist before moving on, and you'll come out the other side with both a working system and the kind of hands-on understanding that turns "name the concept, explain with an example, give a trade-off" into something you can do without studying it.*
