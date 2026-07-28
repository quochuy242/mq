from __future__ import annotations

from typing import Optional

import typer

from mq.commands import (
    benchmark as benchmark_cmd,
    binding as binding_cmd,
    config as config_cmd,
    consume as consume_cmd,
    declare as declare_cmd,
    delete as delete_cmd,
    doctor as doctor_cmd,
    exchange_cmd as exchange_cmd,
    export_import as export_import_cmd,
    get as get_cmd,
    info as info_cmd,
    info_queue as info_queue_cmd,
    list_cmd as list_cmd,
    move as move_cmd,
    peek as peek_cmd,
    ping as ping_cmd,
    publish as publish_cmd,
    purge as purge_cmd,
    replay as replay_cmd,
    retry as retry_cmd,
    stats as stats_cmd,
    top_cmd as top_cmd,
    watch_cmd as watch_cmd,
    whoami as whoami_cmd,
)
from mq.config import load_config, set_overrides
from mq.utils import set_debug, set_json, set_trace


app = typer.Typer(
    help="RabbitMQ CLI tool - manage RabbitMQ via AMQP protocol.",
    rich_markup_mode="rich",
)


@app.callback()
def main(
    ctx: typer.Context,
    debug: bool = typer.Option(False, "--debug", help="Show Python tracebacks on error"),
    json: bool = typer.Option(False, "--json", help="Machine-readable JSON output"),
    trace: bool = typer.Option(False, "--trace", help="Display protocol operations"),
    host: Optional[str] = typer.Option(None, "--host", help="Override RabbitMQ host", show_default=False),
    port: Optional[int] = typer.Option(None, "--port", help="Override RabbitMQ port", show_default=False),
    username: Optional[str] = typer.Option(None, "--username", help="Override RabbitMQ username", show_default=False),
    password: Optional[str] = typer.Option(None, "--password", help="Override RabbitMQ password", show_default=False),
    vhost: Optional[str] = typer.Option(None, "--vhost", help="Override RabbitMQ vhost", show_default=False),
    ssl: Optional[bool] = typer.Option(None, "--ssl/--no-ssl", help="Override TLS setting", show_default=False),
    heartbeat: Optional[int] = typer.Option(None, "--heartbeat", help="Override heartbeat interval (seconds)", show_default=False),
    connection_timeout: Optional[int] = typer.Option(None, "--connection-timeout", help="Override connection timeout (seconds)", show_default=False),
) -> None:
    set_debug(debug)
    set_json(json)
    set_trace(trace)
    set_overrides(
        host=host,
        port=port,
        username=username,
        password=password,
        vhost=vhost,
        ssl=ssl,
        heartbeat=heartbeat,
        connection_timeout=connection_timeout,
    )


@app.command()
def ping() -> None:
    """Test connection to RabbitMQ."""
    config = load_config()
    ping_cmd.execute(config)


@app.command()
def config(
    host: Optional[str] = typer.Option(None, "--host", help="RabbitMQ host", show_default=False),
    port: Optional[int] = typer.Option(None, "--port", help="RabbitMQ port", show_default=False),
    username: Optional[str] = typer.Option(None, "--username", help="RabbitMQ username", show_default=False),
    password: Optional[str] = typer.Option(None, "--password", help="RabbitMQ password", show_default=False),
    vhost: Optional[str] = typer.Option(None, "--vhost", help="RabbitMQ vhost", show_default=False),
    ssl: Optional[bool] = typer.Option(None, "--ssl/--no-ssl", help="Enable TLS", show_default=False),
    heartbeat: Optional[int] = typer.Option(None, "--heartbeat", help="Heartbeat interval in seconds", show_default=False),
    connection_timeout: Optional[int] = typer.Option(None, "--connection-timeout", help="Connection timeout in seconds", show_default=False),
    show: bool = typer.Option(False, "--show", help="Display current configuration"),
) -> None:
    """Configure connection parameters (saved to ~/.config/mq/config.yaml)."""
    config_cmd.execute(
        host=host,
        port=port,
        username=username,
        password=password,
        vhost=vhost,
        ssl=ssl,
        heartbeat=heartbeat,
        connection_timeout=connection_timeout,
        show=show,
    )


@app.command()
def declare(
    queue: str = typer.Argument(..., help="Queue name"),
    durable: bool = typer.Option(False, "--durable", help="Durable queue (survives broker restart)"),
    exclusive: bool = typer.Option(False, "--exclusive", help="Exclusive queue (deleted when connection closes)"),
    auto_delete: bool = typer.Option(False, "--auto-delete", help="Auto-delete queue (deleted when last consumer cancels)"),
) -> None:
    """Declare a queue."""
    config = load_config()
    declare_cmd.execute(config, queue, durable=durable, exclusive=exclusive, auto_delete=auto_delete)


@app.command()
def delete(
    queue: str = typer.Argument(..., help="Queue name"),
) -> None:
    """Delete a queue."""
    config = load_config()
    delete_cmd.execute(config, queue)


@app.command()
def purge(
    queue: str = typer.Argument(..., help="Queue name"),
) -> None:
    """Purge all messages from a queue."""
    config = load_config()
    purge_cmd.execute(config, queue)


@app.command()
def stats(
    queue: str = typer.Argument(..., help="Queue name"),
) -> None:
    """Get queue statistics via passive declare."""
    config = load_config()
    stats_cmd.execute(config, queue)


@app.command()
def publish(
    queue: str = typer.Argument(..., help="Queue name"),
    file: Optional[str] = typer.Option(None, "--file", help="Read message body from file", show_default=False),
    body: Optional[str] = typer.Option(None, "--body", help="Message body string", show_default=False),
    persistent: bool = typer.Option(False, "--persistent", help="Persistent message"),
    content_type: Optional[str] = typer.Option(None, "--content-type", help="Content type of the message", show_default=False),
    header: list[str] = typer.Option([], "--header", help="Header key=value (repeatable)", show_default=False),
) -> None:
    """Publish a message to a queue."""
    config = load_config()
    publish_cmd.execute(config, queue, file=file, body=body, persistent=persistent, content_type=content_type, headers=header)


@app.command()
def get(
    queue: str = typer.Argument(..., help="Queue name"),
    index: int = typer.Option(1, "--index", "-n", help="Message position (1-based) in the queue", show_default=True),
    ack: bool = typer.Option(False, "--ack", help="Acknowledge and remove message from queue"),
    requeue: bool = typer.Option(False, "--requeue", help="Requeue the message after fetching"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON body"),
    jq: Optional[str] = typer.Option(None, "--jq", help="jq-style filter expression (e.g. .event.id)", show_default=False),
) -> None:
    """Fetch a message from a queue by position using basic_get."""
    config = load_config()
    get_cmd.execute(config, queue, index=index, ack=ack, requeue=requeue, pretty=pretty, jq_expr=jq)


@app.command()
def consume(
    queue: str = typer.Argument(..., help="Queue name"),
    auto_ack: bool = typer.Option(False, "--auto-ack", help="Auto acknowledge messages"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Maximum number of messages to consume", show_default=False),
    json: bool = typer.Option(False, "--json", help="Output messages as JSON"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON bodies"),
    jq: Optional[str] = typer.Option(None, "--jq", help="jq-style filter expression (e.g. .event.id)", show_default=False),
    header: Optional[list[str]] = typer.Option(None, "--header", help="Filter by header key=value (repeatable)", show_default=False),
) -> None:
    """Consume messages continuously from a queue."""
    config = load_config()
    consume_cmd.execute(config, queue, auto_ack=auto_ack, limit=limit, as_json=json, pretty=pretty, jq_expr=jq, header_filter=header)


@app.command()
def info(
    queue: Optional[str] = typer.Option(None, "--queue", "-q", help="Show queue details", show_default=False),
) -> None:
    """Display connection information (or queue details with --queue)."""
    config = load_config()
    info_cmd.execute(config, queue=queue)


@app.command(name="info-queue")
def info_queue(
    queue: str = typer.Argument(..., help="Queue name"),
) -> None:
    """Show details of a specific queue."""
    config = load_config()
    info_queue_cmd.execute(config, queue)


@app.command()
def peek(
    queue: str = typer.Argument(..., help="Queue name"),
    count: int = typer.Option(1, "--count", "-c", help="Number of messages to peek"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON bodies"),
) -> None:
    """Peek at messages without removing them from the queue."""
    config = load_config()
    peek_cmd.execute(config, queue, count=count, pretty=pretty)


exchange_app = typer.Typer(help="Manage exchanges")

app.add_typer(exchange_app, name="exchange")


@exchange_app.command("declare")
def exchange_declare(
    exchange: str = typer.Argument(..., help="Exchange name"),
    type: str = typer.Option("direct", "--type", "-t", help="Exchange type (direct, fanout, topic, headers)"),
    durable: bool = typer.Option(False, "--durable", help="Durable exchange"),
    auto_delete: bool = typer.Option(False, "--auto-delete", help="Auto-delete exchange"),
) -> None:
    """Declare an exchange."""
    config = load_config()
    exchange_cmd.execute_declare(config, exchange, exchange_type=type, durable=durable, auto_delete=auto_delete)


@exchange_app.command("delete")
def exchange_delete(
    exchange: str = typer.Argument(..., help="Exchange name"),
) -> None:
    """Delete an exchange."""
    config = load_config()
    exchange_cmd.execute_delete(config, exchange)


@exchange_app.command("list")
def exchange_list(
    pattern: Optional[str] = typer.Option(None, "--pattern", help="Exchange name pattern"),
) -> None:
    """List exchanges (requires Management HTTP API)."""
    config = load_config()
    exchange_cmd.execute_list(config, pattern=pattern)


@app.command()
def bind(
    queue: str = typer.Argument(..., help="Queue name"),
    exchange: str = typer.Argument(..., help="Exchange name"),
    routing_key: str = typer.Argument("", help="Routing key"),
) -> None:
    """Bind a queue to an exchange."""
    config = load_config()
    binding_cmd.execute_bind(config, queue, exchange, routing_key)


@app.command()
def unbind(
    queue: str = typer.Argument(..., help="Queue name"),
    exchange: str = typer.Argument(..., help="Exchange name"),
    routing_key: str = typer.Argument("", help="Routing key"),
) -> None:
    """Unbind a queue from an exchange."""
    config = load_config()
    binding_cmd.execute_unbind(config, queue, exchange, routing_key)


@app.command()
def whoami() -> None:
    """Display current user, vhost, and permissions."""
    config = load_config()
    whoami_cmd.execute(config)


@app.command()
def doctor() -> None:
    """Run comprehensive diagnostic checks."""
    config = load_config()
    doctor_cmd.execute(config)


@app.command()
def move(
    source: str = typer.Argument(..., help="Source queue"),
    destination: str = typer.Argument(..., help="Destination queue"),
    count: int = typer.Option(1, "--count", "-c", help="Number of messages to move"),
) -> None:
    """Move messages from one queue to another."""
    config = load_config()
    move_cmd.execute(config, source, destination, count=count)


@app.command()
def retry(
    queue: str = typer.Argument(..., help="DLQ queue name"),
    limit: int = typer.Option(-1, "--limit", "-n", help="Max messages to retry (-1 = all)"),
) -> None:
    """Retry messages from a DLQ to their original queue."""
    config = load_config()
    retry_cmd.execute(config, queue, limit=limit)


@app.command()
def replay(
    queue: str = typer.Argument(..., help="Queue name"),
    limit: int = typer.Option(-1, "--limit", "-n", help="Max messages to replay (-1 = all)"),
) -> None:
    """Replay all messages in a queue."""
    config = load_config()
    replay_cmd.execute(config, queue, limit=limit)


@app.command()
def export(
    queue: str = typer.Argument(..., help="Queue name"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path"),
    limit: int = typer.Option(-1, "--limit", "-n", help="Max messages to export (-1 = all)"),
) -> None:
    """Export messages from a queue to a JSON file."""
    config = load_config()
    export_import_cmd.execute_export(config, queue, output=output, limit=limit)


@app.command(name="import")
def import_(
    queue: str = typer.Argument(..., help="Target queue name"),
    file: str = typer.Argument(..., help="JSON file to import"),
) -> None:
    """Import messages from a JSON file into a queue."""
    config = load_config()
    export_import_cmd.execute_import(config, queue, file)


@app.command()
def list(
    pattern: Optional[str] = typer.Option(None, "--pattern", help="Queue name pattern (glob)"),
) -> None:
    """List queues (requires Management HTTP API)."""
    config = load_config()
    list_cmd.execute(config, pattern=pattern)


@app.command()
def top(
    limit: int = typer.Option(10, "--limit", "-n", help="Number of queues to show"),
    sort_by: str = typer.Option("ready", "--sort-by", help="Sort by: ready or unacked"),
) -> None:
    """Show top queues by message count (requires Management HTTP API)."""
    config = load_config()
    top_cmd.execute(config, limit=limit, sort_by=sort_by)


@app.command()
def watch(
    queue: str = typer.Argument(..., help="Queue name"),
    interval: float = typer.Option(1.0, "--interval", "-i", help="Refresh interval (seconds)"),
) -> None:
    """Watch a queue in real-time (requires Management HTTP API)."""
    config = load_config()
    watch_cmd.execute(config, queue, interval=interval)


benchmark_app = typer.Typer(help="Run benchmarks")

app.add_typer(benchmark_app, name="benchmark")


@benchmark_app.command("publish")
def benchmark_publish(
    queue: str = typer.Argument(..., help="Queue name"),
    count: int = typer.Option(1000, "--count", "-c", help="Number of messages"),
    size: int = typer.Option(1024, "--size", "-s", help="Message size in bytes"),
) -> None:
    """Benchmark publish throughput."""
    config = load_config()
    benchmark_cmd.execute_publish(config, queue, count=count, size=size)


@benchmark_app.command("consume")
def benchmark_consume(
    queue: str = typer.Argument(..., help="Queue name"),
    count: int = typer.Option(1000, "--count", "-c", help="Number of messages"),
) -> None:
    """Benchmark consume throughput."""
    config = load_config()
    benchmark_cmd.execute_consume(config, queue, count=count)


if __name__ == "__main__":
    app()
