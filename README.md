# mq — RabbitMQ CLI Tool

Manage RabbitMQ queues, exchanges, bindings, and messages via the AMQP protocol.

## Features

- **Connection** — configure, ping, info, whoami, doctor
- **Queues** — declare, delete, purge, stats, list (via Management API)
- **Messages** — publish, get, peek, browse, consume, move, retry, replay
- **Exchanges** — declare, delete
- **Bindings** — bind, unbind
- **Export/Import** — export messages to JSON, import from JSON
- **Monitor** — watch, top (via Management API)
- **Benchmark** — publish/consume throughput testing
- **Output** — table, JSON (`--json`), pretty-print (`--pretty`), jq filtering (`--jq`)

## Installation

### Pre-built binary (Linux x86_64)

```bash
chmod +x mq
mv mq ~/.local/bin/
```

### From source

```bash
pip install .
```

Requires: Python 3.10+, `typer`, `pika`, `orjson`, `pyyaml`.

## Configuration

Config is stored at `~/.config/mq/config.yaml`:

```yaml
host: localhost
port: 5672
username: guest
password: guest
vhost: /
ssl: false
heartbeat: 60
connection_timeout: 10
management_port: 15672
```

### Configuration methods (priority order)

1. CLI flags (`--host`, `--port`, etc.) — highest
2. Environment variables (`MQ_HOST`, `MQ_PORT`, `MQ_USERNAME`, `MQ_PASSWORD`, `MQ_VHOST`, `MQ_SSL`, `MQ_HEARTBEAT`, `MQ_CONNECTION_TIMEOUT`, `MQ_MANAGEMENT_PORT`)
3. Config file (`~/.config/mq/config.yaml`)
4. Defaults

## Usage

### Global Options

```bash
mq [OPTIONS] COMMAND [ARGS]...
```

| Option | Description |
|--------|-------------|
| `--debug` | Show Python tracebacks on error |
| `--json` | Machine-readable JSON output |
| `--trace` | Display protocol operations |
| `--host TEXT` | Override RabbitMQ host |
| `--port INT` | Override RabbitMQ port |
| `--username TEXT` | Override RabbitMQ username |
| `--password TEXT` | Override RabbitMQ password |
| `--vhost TEXT` | Override RabbitMQ vhost |
| `--ssl/--no-ssl` | Override TLS setting |
| `--heartbeat INT` | Override heartbeat interval |
| `--connection-timeout INT` | Override connection timeout |

### Commands

#### `mq config`

Configure connection parameters.

```bash
mq config --host rabbit.example.com --port 5671 --ssl
mq config --show
```

#### `mq ping`

Test connection to RabbitMQ.

```bash
mq ping
```

#### `mq info`

Display connection information, user permissions (AMQP probe), or queue details.

```bash
mq info
mq info --queue myqueue
```

#### `mq info-queue <queue>`

Show details of a specific queue via passive declare.

```bash
mq info-queue myqueue
```

#### `mq whoami`

Display current user, vhost, and permissions (AMQP probe).

```bash
mq whoami
```

#### `mq doctor`

Run comprehensive diagnostic checks (TCP, TLS, auth, declare, publish, consume, latency).

```bash
mq doctor
```

#### `mq declare <queue>`

Declare a queue.

```bash
mq declare myqueue --durable
```

#### `mq delete <queue>`

Delete a queue.

```bash
mq delete myqueue
```

#### `mq purge <queue>`

Purge all messages from a queue.

```bash
mq purge myqueue
```

#### `mq stats <queue>`

Get queue statistics via passive declare (ready, consumers).

```bash
mq stats myqueue
```

#### `mq list`

List queues (requires Management HTTP API).

```bash
mq list
mq list --pattern "camera.*"
```

#### `mq top`

Show top queues by message count (requires Management HTTP API).

```bash
mq top --limit 20
mq top --sort-by unacked
```

#### `mq publish <queue>`

Publish a message to a queue.

```bash
mq publish myqueue --body "hello world"
mq publish myqueue --file ./message.json --content-type application/json
mq publish myqueue --body "alert" --header severity=high
```

#### `mq get <queue>`

Fetch one message by position from a queue.

```bash
mq get myqueue              # peek first message
mq get myqueue --index 3    # third message
mq get myqueue --ack        # fetch and remove
mq get myqueue --pretty     # pretty-print JSON body
mq get myqueue --jq .id     # extract .id from JSON body
```

#### `mq peek <queue>`

Peek at messages without removing them.

```bash
mq peek myqueue
mq peek myqueue --count 5
mq peek myqueue --pretty
```

#### `mq consume <queue>`

Consume messages continuously.

```bash
mq consume myqueue
mq consume myqueue --limit 5 --auto-ack
mq consume myqueue --json
mq consume myqueue --header type=image
mq consume myqueue --jq .event.id
```

#### `mq exchange declare|delete|list`

Manage exchanges.

```bash
mq exchange declare my.exchange --type topic --durable
mq exchange delete my.exchange
mq exchange list              # requires Management HTTP API
```

#### `mq bind / unbind`

Bind/unbind a queue to an exchange.

```bash
mq bind myqueue my.exchange my.routing.key
mq unbind myqueue my.exchange my.routing.key
```

#### `mq move <source> <dest>`

Move messages from one queue to another.

```bash
mq move source-queue dest-queue --count 10
```

#### `mq retry <dlq>`

Retry messages from a DLQ to their original queue (uses x-death headers).

```bash
mq retry myqueue.dlq
mq retry myqueue.dlq --limit 50
```

#### `mq replay <queue>`

Replay all messages in a queue (consume and republish).

```bash
mq replay myqueue
mq replay myqueue --limit 100
```

#### `mq export <queue>`

Export messages to JSON file.

```bash
mq export myqueue
mq export myqueue --output messages.json --limit 100
```

#### `mq import <queue> <file>`

Import messages from JSON file.

```bash
mq import myqueue messages.json
```

#### `mq watch <queue>`

Watch a queue in real-time (requires Management HTTP API).

```bash
mq watch myqueue
mq watch myqueue --interval 2
```

#### `mq benchmark publish|consume`

Benchmark throughput.

```bash
mq benchmark publish myqueue --count 10000 --size 512
mq benchmark consume myqueue --count 5000
```

## Architecture

```
mq/
    cli.py                     # Typer CLI definitions
    config.py                  # Configuration (file + env + flags)
    connection.py              # AMQP connection management

    commands/                  # CLI command handlers (thin wrappers)
        config_cmd.py
        ping.py, get.py, publish.py, consume.py
        queue.py, exchange_cmd.py, binding.py
        peek.py, move.py, retry.py, replay.py
        export_import.py, benchmark.py
        list_cmd.py, top_cmd.py, watch_cmd.py
        info.py, info_queue.py, whoami.py, doctor.py

    services/                  # Business logic
        queue_service.py       # Queue declare/delete/purge/stats
        exchange_service.py    # Exchange declare/delete, bind/unbind
        message_service.py     # Get, peek, move messages
        diagnostic_service.py  # Doctor, permissions probe
        management_service.py  # Management HTTP API client

    utils/                     # Utilities
        output.py              # Table/JSON/color printing
        helpers.py             # Message formatting, jq filter, error handling
```

## AMQP Limitations

Some features require the RabbitMQ Management HTTP API (plugin `rabbitmq_management`):

| Feature | Pure AMQP | Management API required |
|---------|-----------|------------------------|
| Queue declare/delete/purge | ✅ | — |
| Publish / Get / Peek | ✅ | — |
| Consume / Move / Retry / Replay | ✅ | — |
| Export / Import | ✅ | — |
| Bind / Unbind | ✅ | — |
| Exchange declare/delete | ✅ | — |
| Info / Whoami / Doctor | ✅ (AMQP probe) | — |
| Queue stats (passive declare) | ✅ (ready + consumers) | — |
| **List queues** | ❌ | ✅ |
| **Exchange list** | ❌ | ✅ |
| **Top queues** | ❌ | ✅ |
| **Watch (unacked + rates)** | ❌ | ✅ |

The management port defaults to 15672 (HTTP) or 15671 (HTTPS), and can be configured
via `management_port` in the config file or `MQ_MANAGEMENT_PORT` environment variable.

## Troubleshooting

```bash
# Test basic connectivity
mq ping

# Check your current user and permissions
mq whoami

# Run full diagnostics
mq doctor

# Show connection details
mq info

# Enable debug mode for tracebacks
mq --debug consume myqueue
```
