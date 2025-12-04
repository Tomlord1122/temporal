# Comprehensive Guide to Temporal Server

A deep-dive guide for learning Temporal and contributing to the open-source project.

## Table of Contents

- [What is Temporal?](#what-is-temporal)
- [Core Concepts](#core-concepts)
- [Architecture Overview](#architecture-overview)
- [The Four Core Services](#the-four-core-services)
- [Codebase Structure](#codebase-structure)
- [Key Design Patterns](#key-design-patterns)
- [Development Setup](#development-setup)
- [Running and Testing](#running-and-testing)
- [Contributing to Temporal](#contributing-to-temporal)
- [Good First Issues](#good-first-issues)
- [Learning Resources](#learning-resources)

---

## What is Temporal?

**Temporal** is a **durable execution platform** that enables developers to build scalable, fault-tolerant applications. It automatically handles intermittent failures and retries failed operations.

### Key Characteristics

| Attribute | Value |
|-----------|-------|
| Language | Go (1.25.0) |
| Codebase Size | ~757,000 lines of code |
| License | MIT |
| Origin | Fork of Uber's Cadence |
| Status | Production-ready |

### The Problem Temporal Solves

Traditional distributed systems require developers to handle:

- Network failures and retries
- State persistence across failures
- Long-running process coordination
- Timeouts and error handling

Temporal abstracts all of this away, letting you write business logic as straightforward code while the platform guarantees **durable execution** - your workflows will complete correctly even if servers crash, networks fail, or processes restart.

---

## Core Concepts

### Workflows

**Workflows** are the brain of your orchestration. They define the sequence of steps your application runs.

Key properties:

- Must be **deterministic** (same input produces same output)
- Must have **no side effects** (with specific exceptions)
- Can run for seconds, days, or even years
- Automatically resume after failures

```go
// Example workflow pseudocode
func MyWorkflow(ctx workflow.Context, input string) (string, error) {
    var output string
    err := workflow.ExecuteActivity(ctx, MyActivity, input).Get(ctx, &output)
    if err != nil {
        return "", err
    }

    // Can sleep for days - state is preserved!
    workflow.Sleep(ctx, time.Hour * 24 * 30)

    return output, nil
}
```

### Activities

**Activities** are the building blocks that do actual work (I/O, external calls, etc.).

Key properties:

- Can have side effects (API calls, database writes)
- Must be **idempotent** OR **non-retryable**
- Executed by Workers, not the Temporal server
- Automatically retried on failure (configurable)

```go
func MyActivity(ctx context.Context, input string) (string, error) {
    // Can make external API calls, write to database, etc.
    result, err := externalService.Call(input)
    return result, err
}
```

### Workers

**Workers** are user-hosted processes that execute your Workflow and Activity code:

- Poll the Temporal server for tasks
- Execute Workflow/Activity code locally
- Report results back to the server
- You control the deployment and scaling

### Task Queues

**Task Queues** are named queues that Workers poll for work:

- **Workflow Tasks**: Execute workflow decision code
- **Activity Tasks**: Execute activity code
- Enable routing work to specific workers
- Support partitioning for high throughput

### Workflow Execution

A running instance of a Workflow, identified by:

| Identifier | Description |
|------------|-------------|
| Namespace | Logical isolation unit |
| Workflow ID | User-defined identifier |
| Run ID | System-generated unique ID |

### Event History

An **append-only log** of all events in a Workflow Execution:

- Complete audit trail of what happened
- Enables replay for debugging
- Used to reconstruct workflow state
- Supports branching for resets and conflict resolution

---

## Architecture Overview

```
+-------------------------------------------------------------------+
|                     USER-HOSTED PROCESSES                          |
+-------------------------------------------------------------------+
|  +-------------------+       +-------------------------------+     |
|  | User Application  |       |       Temporal Workers        |     |
|  |   (SDK Client)    |       | (Execute Workflows/Activities)|     |
|  +---------+---------+       +---------------+---------------+     |
|            |                                 |                     |
+------------|---------------------------------|---------------------+
             | gRPC                            | gRPC (poll/respond)
             v                                 v
+-------------------------------------------------------------------+
|                        TEMPORAL CLUSTER                            |
+-------------------------------------------------------------------+
|  +---------------------------------------------------------------+ |
|  |                      FRONTEND SERVICE                         | |
|  |         (API Gateway - handles all client requests)           | |
|  +-----------------------------+---------------------------------+ |
|                                |                                   |
|  +-----------------------------v---------------------------------+ |
|  |                                                               | |
|  |  +-----------+    +------------+    +-----------------+       | |
|  |  |  HISTORY  |    |  MATCHING  |    |     WORKER      |       | |
|  |  |  SERVICE  |    |   SERVICE  |    |    SERVICE      |       | |
|  |  |           |    |            |    |                 |       | |
|  |  | (Workflow |    | (Task Queue|    | (Internal       |       | |
|  |  | Execution)|    | Management)|    |  Background)    |       | |
|  |  +-----+-----+    +-----+------+    +-----------------+       | |
|  +--------|---------------|--------------------------------------+ |
|           |               |                                        |
|           v               v                                        |
|  +---------------------------------------------------------------+ |
|  |                     PERSISTENCE LAYER                         | |
|  |  Cassandra | PostgreSQL | MySQL | SQLite | Elasticsearch      | |
|  +---------------------------------------------------------------+ |
+-------------------------------------------------------------------+
```

### Design Principles

1. **Event Sourcing**: All state changes are recorded as immutable history events
2. **Durable Execution**: Workflows continue correctly despite failures
3. **Deterministic Workflows**: User code must be deterministic
4. **Task-Based Processing**: Workers poll for workflow/activity tasks
5. **Horizontal Scaling**: Sharded history service and partitioned task queues

---

## The Four Core Services

### 1. Frontend Service

**Location**: `service/frontend/`

The **entry point** for all client requests.

**Responsibilities:**

- Handles user application RPCs (Start/Cancel/Query/Update/Signal workflows)
- Routes long-poll requests to Matching Service
- Provides HTTP API support via gRPC-Gateway
- Implements namespace management
- Rate limiting and authorization

**Key Files:**

| File | Purpose |
|------|---------|
| `workflow_handler.go` | Main workflow API implementation |
| `admin_handler.go` | Admin operations |
| `namespace_handler.go` | Namespace management |
| `service.go` | Service initialization |

### 2. History Service

**Location**: `service/history/`

The **brain** that manages individual workflow executions durably.

**Responsibilities:**

- Processes RPCs from user applications and workers
- Maintains workflow execution history (append-only event log)
- Manages mutable state (cached summaries of execution state)
- Shards workflow executions across cluster
- Manages internal task queues

**Internal Task Queues:**

| Queue | Purpose |
|-------|---------|
| Transfer Queue | Immediate execution tasks |
| Timer Queue | Scheduled execution tasks |
| Visibility Queue | Metadata updates for search |
| Replication Queue | Multi-cluster sync |
| Archival Queue | History archival |

**Key Concepts:**

- **History Shards**: Logical partitions (typically 256-512) for scaling
- **Mutable State**: In-memory cache of workflow execution state
- **State Machines (HSM)**: Manage complex state transitions

**Key Files:**

| File | Purpose |
|------|---------|
| `handler.go` | Main gRPC handler (~87KB) |
| `history_engine.go` | Core execution logic |
| `shard/controller.go` | Shard lifecycle management |
| `workflow/mutable_state_impl.go` | Workflow state management |

### 3. Matching Service

**Location**: `service/matching/`

Manages **task distribution** to workers.

**Responsibilities:**

- Manages Task Queues exposed to users
- Handles long-poll requests from workers
- Distributes Workflow and Activity tasks
- Partition-based task queue management
- Task forwarding between partitions

**Key Features:**

- Default 4 partitions per task queue
- Sticky task queues (worker affinity)
- Build ID versioning for worker compatibility
- Hierarchical forwarding between partitions

**Key Files:**

| File | Purpose |
|------|---------|
| `handler.go` | gRPC handler |
| `engine.go` | Core matching logic |
| `matcher.go` | Task matching algorithm |
| `forwarder.go` | Inter-node forwarding |

### 4. Worker Service

**Location**: `service/worker/`

**Internal background processing** for the cluster.

**Responsibilities:**

| Component | Purpose |
|-----------|---------|
| Replicator | Consumes replication tasks from remote clusters |
| Scanner | Scans closed workflows for cleanup |
| Archival | Archives completed workflow histories |
| Scheduler | Executes scheduled workflows |
| Batcher | Batch workflow operations |
| Delete Namespace Worker | Namespace deletion orchestration |

---

## Codebase Structure

```
temporal/
├── api/                    # Generated gRPC and protobuf code
├── cmd/                    # Entry points
│   ├── server/            # temporal-server binary
│   └── tools/             # CLI tools (cassandra, sql, tdbg, etc.)
├── service/               # Core services
│   ├── frontend/          # Frontend Service (~36 subdirs)
│   ├── history/           # History Service (~69 subdirs)
│   ├── matching/          # Matching Service (~77 subdirs)
│   └── worker/            # Worker Service
├── common/                # Shared utilities (78 packages)
│   ├── persistence/       # Database abstraction (66 subdirs)
│   ├── metrics/           # Observability (36 subdirs)
│   ├── log/               # Structured logging
│   ├── cache/             # Caching utilities
│   ├── authorization/     # Auth/JWT support
│   ├── dynamicconfig/     # Runtime configuration
│   ├── tasks/             # Task execution framework
│   ├── cluster/           # Cluster metadata
│   ├── namespace/         # Namespace management
│   └── ...
├── temporal/              # Server initialization (Fx DI framework)
├── tests/                 # Functional and integration tests
│   ├── ndc/              # Namespace-DC tests
│   └── xdc/              # Cross-DC tests
├── schema/                # Database schemas
│   ├── cassandra/        # Cassandra schemas
│   ├── postgresql/       # PostgreSQL schemas
│   ├── mysql/            # MySQL schemas
│   └── sqlite/           # SQLite schemas
├── proto/                 # Protocol buffer definitions
├── docs/                  # Architecture documentation
│   └── architecture/     # Detailed architecture docs
├── develop/               # Development configuration
│   └── docker-compose/   # Docker compose files
└── tools/                 # Development tools
```

### Key Packages

| Package | Purpose |
|---------|---------|
| `common/persistence/` | Database abstraction layer (SQL, Cassandra, etc.) |
| `common/metrics/` | Prometheus metrics instrumentation |
| `common/log/` | Zap-based structured logging |
| `common/dynamicconfig/` | Runtime configuration without restart |
| `common/authorization/` | JWT/OIDC authorization |
| `common/cache/` | Generic in-memory caching |
| `common/tasks/` | Task execution framework |
| `common/membership/` | Service discovery (Ringpop-based) |
| `service/history/hsm/` | State machine framework |
| `service/history/workflow/` | Workflow execution state |
| `service/history/queues/` | Queue processors |

---

## Key Design Patterns

### 1. Sharding Strategy

- **Pattern**: Consistent hashing with ranges
- **Implementation**: `WorkflowIDToHistoryShard(namespaceID, workflowID, shardCount) → shardID`
- **Location**: `common/primitives/`
- **Benefits**: Load distribution, horizontal scaling
- **Typical Count**: 256-512 shards per cluster

### 2. State Machines (HSM)

- **Pattern**: Explicit state machine framework for complex workflows
- **Location**: `service/history/hsm/`
- **Features**:
  - Type-safe state transitions
  - Automatic task generation on state changes
  - Serializable state
- **Types**: Workflow, Activity, ChildWorkflow, RequestForUpdate, etc.

### 3. Dependency Injection (Uber FX)

- **Pattern**: Complete DI container for service initialization
- **Location**: `temporal/fx.go`
- **Benefits**: Testability, modularity, clean startup/shutdown
- **Usage**: All services bootstrapped via `fx.Options`, `fx.Provide`, `fx.Invoke`

### 4. Transactional Outbox

- **Pattern**: Ensures consistency between History and Matching services
- **Implementation**: Transfer Tasks written atomically with state changes
- **Benefit**: Queue processors guarantee eventual delivery to Matching

### 5. Optimistic Concurrency Control

- **Pattern**: Version/RangeID based conditional updates
- **Benefit**: No locks, high concurrency
- **Failure handling**: `WorkflowConditionFailedError` → retry with refresh

### 6. Event Sourcing

- **Pattern**: Immutable event log for complete history
- **Benefits**:
  - Complete audit trail
  - Replay capability for debugging
  - All state reconstructable from events

### 7. Long-Polling

- **Pattern**: Worker polls matching service, waits for tasks
- **Timeout**: Configurable (typically 60s)
- **Feature**: Zombie poller detection via `CancelOutstandingPoll`

---

## Development Setup

### Prerequisites

**Required:**

```bash
# Go (minimum version in go.mod - currently 1.25.0)
# macOS
brew install go

# Ubuntu
sudo apt install golang

# Verify
go version
```

**Optional:**

```bash
# Protocol Buffers (only for proto changes)
brew install protobuf    # macOS

# Temporal CLI
brew install temporal

# Docker (for integration/functional tests)
# Install from https://docs.docker.com/engine/install/
```

### Clone and Build

```bash
# Clone the repository
git clone https://github.com/temporalio/temporal.git
cd temporal

# First-time build (installs all dependencies and tools)
make

# Build binaries only (faster, no tests)
make bins

# Build specific tools
make temporal-server
make tdbg
```

### Start Dependencies (Optional)

For integration and functional tests, or for using databases other than SQLite:

```bash
# Start Docker containers
make start-dependencies

# This starts:
# - MySQL 8.0.29 (port 3306)
# - Cassandra 3.11 (port 9042)
# - PostgreSQL 13.5 (port 5432)
# - Elasticsearch 7.10.1 (port 9200)
# - Prometheus (port 9090)
# - Grafana (port 3000, admin/admin)
# - Temporal UI

# Stop dependencies
make stop-dependencies
```

### Windows Development

For developing on Windows:

1. Install [Windows Subsystem for Linux 2 (WSL2)](https://aka.ms/wsl)
2. Install Ubuntu from the Microsoft Store
3. Follow Ubuntu setup instructions within WSL2

---

## Running and Testing

### Run the Server

**SQLite in-memory (simplest, default):**

```bash
make start
```

**SQLite with persistent file:**

```bash
make start-sqlite-file
```

**PostgreSQL:**

```bash
make install-schema-postgresql
make start-postgresql
```

**MySQL:**

```bash
make install-schema-mysql
make start-mysql
```

**Cassandra + Elasticsearch:**

```bash
make start-dependencies  # Start Docker containers first
make install-schema-cass-es
make start-cass-es
```

### Create a Namespace

After starting the server:

```bash
temporal operator namespace create default
```

### Access the UI

Open http://localhost:8080 in your browser.

### Run Samples

Clone and run the official samples:

```bash
# Go samples
git clone https://github.com/temporalio/samples-go.git
cd samples-go/helloworld
go run ./...

# Java samples
git clone https://github.com/temporalio/samples-java.git
```

### Run Tests

**Unit tests (no external dependencies):**

```bash
make unit-test
```

**Integration tests (requires database):**

```bash
make start-dependencies  # In another terminal
make integration-test
```

**Functional tests (full E2E):**

```bash
make functional-test
```

**All tests:**

```bash
make test
```

**Single test:**

```bash
go test -v <path> -run <TestSuite> -testify.m <TestName>

# Example:
go test -v github.com/temporalio/temporal/common/persistence \
    -run TestCassandraPersistenceSuite \
    -testify.m TestPersistenceStartWorkflow
```

**Test with race detector:**

```bash
TEST_RACE_FLAG=on make test
```

### Code Quality

```bash
# Lint code
make lint-code

# Format imports
make fmt-imports

# Lint protobuf files
make lint-protos

# Verify copyright headers
make copyright

# All linting checks
make lint

# All checks
make check
```

### IDE Debugging (GoLand)

1. Start dependencies: `make start-dependencies`
2. Create a Run Configuration:
   - **Run Type**: Package
   - **Package path**: `go.temporal.io/server/cmd/server`
   - **Program arguments**: `--env development-postgres12 --allow-no-auth start`
   - **Build flags**: `-tags disable_grpc_modules,test_dep`

---

## Contributing to Temporal

### Before You Start

1. **Sign the CLA**: All contributors must sign the [Temporal Contributor License Agreement](https://github.com/temporalio/temporal/blob/main/docs/development/temporal-cla.md)

2. **Read the Contribution Guidelines**: See [CONTRIBUTING.md](https://github.com/temporalio/temporal/blob/main/CONTRIBUTING.md)

### Commit Message Guidelines

Follow [Chris Beams' git commit guide](http://chris.beams.io/posts/git-commit/):

- Start with uppercase
- No trailing period
- Be specific (avoid generic titles like "bug fixes")
- Explain "why" rather than "what"

PR titles are used as commit messages, so format them properly.

### PR Template

Every PR should include:

```markdown
## What changed?
[Describe the changes]

## Why?
[Explain the rationale]

## How did you test it?
- [ ] built
- [ ] run locally and tested manually
- [ ] covered by existing tests
- [ ] added new unit test(s)
- [ ] added new functional test(s)

## Potential risks
[Identify any risks or remove section if none]
```

### CI/CD Pipeline

Your PR will be tested against:

| Check | Description |
|-------|-------------|
| Unit Tests | Fast tests with no dependencies |
| Integration Tests | Tests against real databases |
| Functional Tests | Full E2E tests across 7 DB configurations |
| Linting | golangci-lint, buf for protos |
| Code Coverage | Tracked via Codecov |
| Breaking Changes | Proto breaking change detection |

**Database Configurations Tested:**

- Cassandra + Elasticsearch (v7)
- Cassandra + Elasticsearch (v8)
- Cassandra + OpenSearch 2
- SQLite
- MySQL 8
- PostgreSQL 12
- PostgreSQL 12 PGX

### Working with API Changes

For gRPC/protobuf changes:

```bash
# Update to latest merged API changes
make update-go-api
```

For local API development:

1. Clone `api`, `api-go`, and `sdk-go` repos
2. Make changes to `api` repo
3. Update `go.mod` with replace directives:

```go
replace (
    go.temporal.io/api => ../api-go
    go.temporal.io/sdk => ../sdk-go
)
```

4. Rebuild: `make proto && make bins`

### License Headers

All source files must have the MIT license header:

```bash
# Verify headers
make copyright
```

---

## Good First Issues

Current open issues suitable for new contributors:

| Issue | Title | Type | Labels |
|-------|-------|------|--------|
| [#4094](https://github.com/temporalio/temporal/issues/4094) | Version info upgrade notification not cleared after upgrade | Bug | `up-for-grabs` |
| [#1338](https://github.com/temporalio/temporal/issues/1338) | Move workflow reset point logic from CLI to service | Enhancement | `refactoring`, `up-for-grabs` |
| [#1311](https://github.com/temporalio/temporal/issues/1311) | Make workflow state available via API | Enhancement | `API`, `difficulty: easy` |
| [#983](https://github.com/temporalio/temporal/issues/983) | Logging not capturing underlying errors | Refactoring | `up-for-grabs` |
| [#778](https://github.com/temporalio/temporal/issues/778) | Add ScheduleToStart timeout to WorkflowTaskScheduledEvent | Enhancement | `API`, `difficulty: easy`, `up-for-grabs` |
| [#607](https://github.com/temporalio/temporal/issues/607) | Add tests for parentclosepolicy workflow | Testing | `testing` |
| [#503](https://github.com/temporalio/temporal/issues/503) | Record activity events for retrying activities on workflow completion | Enhancement | `difficulty: easy`, `up-for-grabs` |

**How to find more:**

Search GitHub issues with labels:
- `good first issue`
- `up-for-grabs`
- `difficulty: easy`

---

## Learning Resources

### Official Documentation

| Resource | URL |
|----------|-----|
| Temporal Docs | https://docs.temporal.io/ |
| Concepts Guide | https://docs.temporal.io/concepts/ |
| Go SDK | https://docs.temporal.io/develop/go |
| Free Courses | https://learn.temporal.io/courses/ |

### Sample Repositories

| Language | Repository |
|----------|------------|
| Go | https://github.com/temporalio/samples-go |
| Java | https://github.com/temporalio/samples-java |
| TypeScript | https://github.com/temporalio/samples-typescript |
| Python | https://github.com/temporalio/samples-python |

### Architecture Documentation (in this repo)

| File | Content |
|------|---------|
| `docs/architecture/README.md` | High-level overview |
| `docs/architecture/history-service.md` | History service deep dive |
| `docs/architecture/matching-service.md` | Matching service details |
| `docs/architecture/workflow-lifecycle.md` | Workflow execution sequences |
| `docs/development/testing.md` | Testing practices |

### Community

| Resource | URL |
|----------|-----|
| Slack | https://temporal.io/slack |
| GitHub Discussions | https://github.com/temporalio/temporal/discussions |
| Community Forum | https://community.temporal.io/ |

---

## Quick Reference

### Common Make Commands

```bash
# Build
make                    # Full build with tests
make bins               # Build binaries only
make proto              # Regenerate proto files

# Run
make start              # Run with SQLite in-memory
make start-sqlite-file  # Run with persistent SQLite
make start-postgresql   # Run with PostgreSQL
make start-mysql        # Run with MySQL
make start-cass-es      # Run with Cassandra + Elasticsearch

# Test
make unit-test          # Unit tests
make integration-test   # Integration tests
make functional-test    # Functional tests
make test               # All tests

# Quality
make lint               # All linting
make lint-code          # Go code linting
make fmt-imports        # Format imports
make copyright          # Verify license headers
make check              # All checks

# Dependencies
make start-dependencies # Start Docker containers
make stop-dependencies  # Stop Docker containers
make gomodtidy          # Tidy go modules
make update-go-api      # Update API dependency
```

### Project Statistics

| Metric | Value |
|--------|-------|
| Total Go Files | ~2,381 |
| Lines of Code | ~757,626 |
| Main Services | 4 |
| Common Packages | 78 |
| Database Backends | 5 |
| Build Tools | 15+ |
| Test Types | 4 |

---

## Summary

Temporal is a sophisticated distributed workflow engine built on:

| Concept | Implementation |
|---------|----------------|
| Microservices | 4 independent services (Frontend, History, Matching, Worker) |
| Sharding | Consistent hashing distributes workflows across 256-512 shards |
| Event Sourcing | Immutable history enables replay and auditing |
| State Machines | HSM framework manages complex state transitions |
| Persistence | Abstracted layer supports Cassandra, PostgreSQL, MySQL, SQLite |
| Scaling | Horizontal scaling via sharding and partitioning |

### Quick Start Checklist

- [ ] Clone and build: `git clone https://github.com/temporalio/temporal.git && cd temporal && make`
- [ ] Start server: `make start`
- [ ] Create namespace: `temporal operator namespace create default`
- [ ] Run samples from https://github.com/temporalio/samples-go
- [ ] Read architecture docs in `docs/architecture/`
- [ ] Sign the CLA
- [ ] Pick a `good first issue` to contribute!

---

*This guide was generated to help new contributors understand and contribute to the Temporal project.*
