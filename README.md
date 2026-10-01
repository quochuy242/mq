# mq — RabbitMQ CLI over AMQP 0-9-1

A RabbitMQ command-line tool that talks **only AMQP 0-9-1**. It never needs the
management port (15672/15671) or the `rabbitmq_management` plugin, so it works
against brokers where those are firewalled off — which is exactly the situation
where you most want a working tool.

---

## Contents

- [Install](#install)
- [Configure](#configure)
- [Daily use](#daily-use)
- [Commands](#commands)
- [What AMQP can and cannot do](#what-amqp-can-and-cannot-do)
- [The queue inventory](#the-queue-inventory)
- [Safety rules this tool follows](#safety-rules-this-tool-follows)
- [Diagnosis without a management UI](#diagnosis-without-a-management-ui)
- [Scripting and exit codes](#scripting-and-exit-codes)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

---

## Install

```bash
pip install .
```

Requires Python 3.10+, `pika`, `typer`, `orjson`, `pyyaml`. A standalone binary
can be built with `pyinstaller mq.spec`.

---

## Configure

Settings are merged from, highest priority first:

1. CLI flags (`--host`, `--port`, …)
2. Environment variables
3. The selected profile in the config file
4. The flat top level of the config file
5. Built-in defaults

### One URL, no config file

```bash
export MQ_URL='amqps://user:pass@rabbit.internal:5671/prod'
mq ping
```

`amqp://` implies plaintext on 5672, `amqps://` implies TLS on 5671. The path is
the vhost (`/%2F` for the default `/`). Query parameters are accepted:
`?heartbeat=30&connection_timeout=5`.

### The config file

`~/.config/mq/config.yaml`:

```yaml
# Defaults for every profile
heartbeat: 60
connection_timeout: 10
blocked_connection_timeout: 30

host: rabbit.internal
port: 5671
username: ops
password: secret          # or use password_command, see below
vhost: prod
ssl: true
ssl_verify: true
ssl_cafile: /etc/ssl/certs/internal-ca.pem

profiles:
  staging:
    host: rabbit.staging.internal
    vhost: staging
  eu-west:
    host: rabbit.eu-west.internal
    vhost: prod

active_profile: eu-west
```

```bash
mq --profile staging ping     # per-run override
mq config --profile devbox --host rabbit.dev --set-active --username dev
```

`mq config --show` prints the fully resolved settings; `mq config --list-profiles`
lists the profiles.

### Environment variables

`MQ_URL`, `MQ_PROFILE`, `MQ_HOST`, `MQ_PORT`, `MQ_USERNAME`, `MQ_PASSWORD`,
`MQ_VHOST`, `MQ_SSL`, `MQ_SSL_VERIFY`, `MQ_SSL_CAFILE`, `MQ_HEARTBEAT`,
`MQ_CONNECTION_TIMEOUT`, `MQ_CONNECTION_ATTEMPTS`, `MQ_BLOCKED_CONNECTION_TIMEOUT`,
`MQ_AUTH`, `MQ_TOKEN`, `MQ_PASSWORD_COMMAND`, `MQ_CONNECTION_NAME`,
`MQ_CLIENT_CERT`, `MQ_CLIENT_KEY`, `MQ_MANAGEMENT_PORT`.

### Keeping the password out of the file

```bash
mq config --password-command 'pass show rabbitmq/prod'
```

The command's stdout becomes the password. On POSIX the command is split without
a shell, so metacharacters cannot be reinterpreted.

### TLS

Verification is on by default. If the broker uses an internal CA:

```bash
mq config --ssl-cafile /etc/ssl/certs/internal-ca.pem
```

`--no-ssl-verify` disables verification entirely and removes protection against
man-in-the-middle attacks; only use it on a trusted network. `mq doctor` runs the
TLS handshake using the same trust store as `mq ping`, so it cannot disagree with
it.

### Authentication mechanisms

| `--auth` | Credentials | Needs |
|----------|-------------|-------|
| `plain` (default) | username + password | — |
| `external` | TLS client certificate | `ssl: true`, `--client-cert`, optional `--client-key` |
| `oauth` | bearer token as the password | `--token` |

---

## Daily use

```bash
mq ping                     # connect, and report what the broker says about itself
mq doctor                   # full diagnostics
mq doctor --status          # one line + exit code, for monitoring
mq whoami                   # what can this user do in this vhost?
mq list                     # tracked queues, with live depths
mq stats orders             # one queue in detail
mq peek orders --count 5    # read messages without consuming them
mq consume orders --limit 10
mq export orders -o dump.json   # leaves the queue untouched
```

---

## Commands

### Connection and diagnosis

| Command | What it does |
|---------|--------------|
| `mq ping` | Connect and print negotiated limits, broker version and cluster name |
| `mq info` | Local settings + broker self-report + permission probe |
| `mq info --no-probe` | Local settings only; does not touch the broker |
| `mq info --queue NAME` | Alias for `mq info-queue` |
| `mq whoami [-q QUEUE]` | Effective permissions. `--queue` needs only read permission |
| `mq doctor` | DNS, TCP, TLS, auth, channel, broker info, confirms, alarm state |
| `mq doctor --readonly` | Skips the write probes; safe with read-only credentials |
| `mq doctor --status` | One-line health summary; exit 1 when unhealthy |
| `mq version` | Tool version |

### Queues

| Command | What it does |
|---------|--------------|
| `mq declare NAME` | Declare a queue |
| `mq declare NAME --quorum` | Shorthand for `--queue-type quorum` |
| `mq declare NAME --ttl 60000 --dlx events --max-length 10000` | Common x-arguments |
| `mq declare NAME --arg x-custom=value` | Any x-argument, repeatable |
| `mq delete NAME [--if-empty] [--if-unused] [-y]` | Delete |
| `mq purge NAME [--dry-run] [-y]` | Remove all messages, keep the queue |
| `mq stats NAME` | Depth and consumer count; warns when nobody is consuming |
| `mq info-queue NAME` | Depth, consumers, and optional expectations |
| `mq verify-queue NAME --arg k=v` | Exit non-zero when the queue does not match (CI) |
| `mq list [--queues a,b] [--pattern glob]` | Live depths for tracked or named queues |
| `mq top [--sort-by ready\|consumers\|name]` | The deepest or busiest tracked queues |
| `mq watch NAME [-i 1] [-n N] [--json]` | Poll a queue and show the depth trend |

`mq info-queue` can assert what a queue should be. AMQP cannot read arguments
back, but it refuses a mismatched declaration with 406, so each expectation
becomes a probe:

```bash
mq info-queue orders --expect-durable --expect-quorum --expect-ttl 3600000
mq verify-queue orders --expect-durable --arg x-queue-type=quorum   # exit 1 on drift
```

### Exchanges and bindings

| Command | What it does |
|---------|--------------|
| `mq exchange declare NAME [-t topic] [--durable]` | Declare |
| `mq exchange list [--exchanges a,b] [--pattern glob]` | Passive declare |
| `mq exchange info NAME --expect-type topic` | Assert type/durability |
| `mq exchange delete NAME [--if-unused]` | Delete |
| `mq bind QUEUE EXCHANGE [KEY]` | Bind |
| `mq unbind QUEUE EXCHANGE [KEY]` | Unbind |

### Messages

| Command | What it does |
|---------|--------------|
| `mq publish QUEUE --body "hi"` | Publish and wait for the broker's confirm |
| `mq publish --exchange events --routing-key order.created --body …` | Publish to any exchange |
| `mq publish … --mandatory` | Fail if no queue is bound to the routing key |
| `mq get QUEUE [-n 3] [--ack] [--jq .id]` | Fetch by position, put it back by default |
| `mq peek QUEUE [-c 5]` | Read several messages without consuming them |
| `mq consume QUEUE [--limit N] [--prefetch N] [--header k=v]` | Stream messages |
| `mq move SRC DST [-c 100]` | Move, publish-confirm then ack |
| `mq retry DLQ [-n 50]` | Re-publish DLQ messages to their original destination |
| `mq replay QUEUE [-n 100]` | Re-publish a queue's messages back onto itself |
| `mq export QUEUE -o file.json` | Non-destructive by default |
| `mq import QUEUE file.json` | Publish a previously exported file |

### Inventory

| Command | What it does |
|---------|--------------|
| `mq inventory list` | What `mq list` and `mq exchange list` will check |
| `mq inventory add orders payments` | Track queue names |
| `mq inventory add amq.topic --exchange` | Track exchange names |
| `mq inventory rm payments` | Forget a name |

### Benchmarks

```bash
mq benchmark publish orders --count 10000 --size 512   # confirms on
mq benchmark consume orders --count 5000 --prefetch 50
```

---

## What AMQP can and cannot do

This is a property of the protocol, not of this tool.

**Possible over AMQP — everything except enumeration:**

| Capability | How |
|---|---|
| Queue/exchange declare, delete, purge, bind | native methods |
| Publish, get, consume, ack/nack | native methods |
| Queue existence, depth, consumer count | `queue.declare` with `passive=true` |
| Exchange existence | `exchange.declare` with `passive=true` |
| Queue declaration *drift* detection | re-declare and read the 406 |
| Broker version, cluster name, capabilities, negotiated limits | `Connection.Start-Ok` |
| Memory/disk alarm state | `connection.blocked` / `connection.unblocked` |
| Unroutable messages | `basic.return` with `mandatory=true` |
| Delivery certainty | publisher confirms |
| Approximate publish rate | depth delta over time |

**Impossible over AMQP — no amount of client work helps:**

- Listing every queue, exchange, binding or consumer owned by the broker.
- User, vhost, permission and policy administration.
- Connection, channel and node inventory.
- Global memory/disk watermarks and per-node status.
- Quota configuration, and statistics beyond depth and consumer count.

For these, `mq` asks you for the names it should check. That is the honest
trade-off: **you tell the tool what to look at, and it reports the truth about
those objects.** `mq list` never pretends to have seen the whole broker.

---

## The queue inventory

Because AMQP cannot enumerate, `mq list` and `mq exchange list` verify the names
in `~/.config/mq/inventory.yaml`:

```bash
mq inventory add orders payments notifications
mq list
```

The inventory is also written automatically whenever the tool touches a queue
or exchange — `declare`, `bind`, `publish`, `consume`, `move`, `retry`,
`replay`. In a real setup you would seed it from the same source of truth that
declares your infrastructure (Helm values, Terraform state, CI config).

For a one-off check without touching the file:

```bash
mq list --queues orders,payments
```

If the management plugin happens to be available, `--use-management` lets the
broker enumerate for you. It is never required, and it is never used
automatically.

---

## Safety rules this tool follows

These are the rules the test suite exists to enforce:

1. **Nothing is acked before it is confirmed.** `move`, `retry`, `replay` and
   `import` publish with publisher confirms enabled and ack the source only
   after the broker has accepted the message. A crash mid-run redelivers instead
   of losing data.
2. **`export` never removes anything** unless you pass `--destructive`.
3. **Batches are held, not cycled.** `peek`, `export` and `get --index` keep
   messages unacked for the whole batch and requeue them at the end. Requeueing
   one at a time makes the broker hand back the same message again, which is how
   `peek --count 5` used to print message #1 five times.
4. **`retry` strips `x-death`.** Forwarding it makes the broker dead-letter the
   message again on the next hop, so the retry never lands.
5. **Probes leave nothing behind.** `doctor` and `whoami` use server-named
   exclusive queues and never create a named object.
6. **Destructive commands ask first** when attached to a terminal, and support
   `--dry-run` (`purge`) so you can see the blast radius.

---

## Diagnosis without a management UI

**How full is this queue?** `mq stats orders` — depth, consumers, and a warning
when messages are piling up with nobody consuming.

**Is the declaration what I think it is?**

```bash
mq info-queue orders --expect-durable --expect-quorum
```

**Is the broker healthy?** `mq doctor --status`. It reports broker version,
cluster name, negotiated limits, capability flags and whether the connection is
blocked by a resource alarm.

**Why is publishing hanging?** A memory or disk alarm makes RabbitMQ send
`connection.blocked`. `mq` registers for those notifications and sets
`blocked_connection_timeout`, so a blocked publish fails with a clear error
instead of hanging until your shell times out.

**What can I do here?** `mq whoami`. Reply codes are translated rather than
dumped: 403 is reported as a permission problem, 404 as a missing object, 406 as
a declaration mismatch, 413 as a quota alarm.

---

## Scripting and exit codes

Global `--json` applies to every command; failures are JSON objects too, not
bare sentences on stderr.

```bash
mq --json stats orders | jq '.ready'
mq --json verify-queue orders --expect-durable --arg x-queue-type=quorum || echo drift
mq --json doctor --status >/dev/null || alert
```

| Exit code | Meaning |
|-----------|---------|
| 0 | Success |
| 1 | Failure — connection refused, permissions, drift, unroutable message, unhealthy broker |

Other useful flags: `--no-color` (or `NO_COLOR=1`), `--debug` for tracebacks,
`--profile` to switch environments.

---

## Troubleshooting

```bash
mq ping                       # can we connect at all?
mq --debug ping               # what exactly failed?
mq config --show              # which settings are actually in effect?
mq doctor                     # where in the handshake does it stop?
mq whoami --queue orders      # what can this user do to a real queue?
```

Common situations:

| Symptom | Cause and fix |
|---|---|
| `Connection refused ... while TLS is enabled` on port 5672 | TLS expects 5671. Use `--port 5671` or an `amqps://` URL |
| `TLS certificate verification failed` | Broker uses an internal CA: `mq config --ssl-cafile <ca.pem>` |
| `not allowed to perform this operation` | Check `mq whoami`; RabbitMQ also answers 403 when the object does not exist |
| `the broker is blocking publishers` | A memory or disk alarm is active. Nothing to fix client-side |
| `mq list` shows nothing | Nothing is tracked yet: `mq inventory add <names>` |
| publish succeeds but nothing arrives | Use `--mandatory`; the routing key has no queue bound |

---

## Development

```bash
python -m unittest discover -s tests -t .
```

The suite runs against an in-memory fake broker (`tests/fake_amqp.py`), so no
RabbitMQ node is required.

```
mq/
    cli.py                     # Typer command definitions
    config.py                  # settings: URL, env, profiles, auth
    connection.py              # AMQP connection, TLS, blocked-connection state
    errors.py                  # broker reply codes -> actionable messages
    inventory.py               # local queue/exchange name store

    commands/                  # thin CLI wrappers
    services/
        queue_service.py       # declare, delete, purge, passive probes, drift checks
        exchange_service.py    # declare, delete, bind, passive probes
        message_service.py     # get, peek, move, publish with confirms, consume
        discovery.py           # AMQP-only list/top/watch
        diagnostic_service.py  # doctor, broker info, permission probe
        management_service.py  # optional HTTP fallback, never required

    utils/                     # output and formatting helpers
    tests/                     # unit tests with a fake broker
```
