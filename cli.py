from typing import List, Optional

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
    inventory_cmd as inventory_cmd,
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
from mq.config import (
    ConfigError,
    load_config_or_exit,
    load_local_config,
    set_overrides,
    set_profile,
)
from mq.utils import set_color, set_debug, set_json, set_trace

app = typer.Typer(
    help="RabbitMQ CLI tool - manage RabbitMQ over AMQP 0-9-1, no management port required.",
    rich_markup_mode="rich",
    no_args_is_help=True,
)


@app.callback()
def main(
    ctx: typer.Context,
    debug: bool = typer.Option(False, "--debug", help="Show Python tracebacks on error"),
    json: bool = typer.Option(False, "--json", help="Machine-readable JSON output"),
    trace: bool = typer.Option(False, "--trace", help="Display protocol operations"),
    color: Optional[bool] = typer.Option(
        None, "--color/--no-color", help="Force colour on or off (also honours NO_COLOR)"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", "-p", help="Use a named profile from the config file",
        show_default=False,
    ),
    host: Optional[str] = typer.Option(None, "--host", help="Override RabbitMQ host", show_default=False),
    port: Optional[int] = typer.Option(None, "--port", help="Override RabbitMQ port", show_default=False),
    username: Optional[str] = typer.Option(None, "--username", help="Override RabbitMQ username", show_default=False),
    password: Optional[str] = typer.Option(None, "--password", help="Override RabbitMQ password", show_default=False),
    vhost: Optional[str] = typer.Option(None, "--vhost", help="Override RabbitMQ vhost", show_default=False),
    ssl: Optional[bool] = typer.Option(None, "--ssl/--no-ssl", help="Override TLS setting", show_default=False),
    ssl_verify: Optional[bool] = typer.Option(None, "--ssl-verify/--no-ssl-verify", help="Verify broker certificate (default: verify)", show_default=False),
    ssl_cafile: Optional[str] = typer.Option(None, "--ssl-cafile", help="CA certificate file (.pem/.crt) to trust", show_default=False),
    heartbeat: Optional[int] = typer.Option(None, "--heartbeat", help="Override heartbeat interval (seconds)", show_default=False),
    connection_timeout: Optional[int] = typer.Option(None, "--connection-timeout", help="Override connection timeout (seconds)", show_default=False),
    connection_attempts: Optional[int] = typer.Option(
        None, "--connection-attempts", help="How many times to retry the TCP/TLS/auth handshake"
    ),
    auth: Optional[str] = typer.Option(
        None, "--auth", help="SASL mechanism: plain, external (client cert) or oauth"
    ),
    token: Optional[str] = typer.Option(None, "--token", help="OAuth token when --auth oauth", show_default=False),
    password_command: Optional[str] = typer.Option(
        None, "--password-command",
        help="Shell command that prints the password (e.g. 'pass show mq/prod')",
        show_default=False,
    ),
) -> None:
    set_debug(debug)
    set_json(json)
    set_trace(trace)
    set_color(color)
    set_profile(profile)
    set_overrides(
        host=host,
        port=port,
        username=username,
        password=password,
        vhost=vhost,
        ssl=ssl,
        ssl_verify=ssl_verify,
        ssl_cafile=ssl_cafile,
        heartbeat=heartbeat,
        connection_timeout=connection_timeout,
        connection_attempts=connection_attempts,
        auth=auth,
        token=token,
        password_command=password_command,
    )


def _config():
    return load_config_or_exit()


def _local_config():
    """For commands that only touch local files, like 'mq inventory'."""
    return load_local_config()


@app.command()
def ping() -> None:
    """Connect and report what the broker says about itself."""
    ping_cmd.execute(_config())


@app.command()
def config(
    host: Optional[str] = typer.Option(None, "--host", help="RabbitMQ host", show_default=False),
    port: Optional[int] = typer.Option(None, "--port", help="RabbitMQ port", show_default=False),
    username: Optional[str] = typer.Option(None, "--username", help="RabbitMQ username", show_default=False),
    password: Optional[str] = typer.Option(None, "--password", help="RabbitMQ password", show_default=False),
    vhost: Optional[str] = typer.Option(None, "--vhost", help="RabbitMQ vhost", show_default=False),
    ssl: Optional[bool] = typer.Option(None, "--ssl/--no-ssl", help="Enable TLS", show_default=False),
    ssl_verify: Optional[bool] = typer.Option(None, "--ssl-verify/--no-ssl-verify", help="Verify broker certificate (default: verify)", show_default=False),
    ssl_cafile: Optional[str] = typer.Option(None, "--ssl-cafile", help="CA certificate file (.pem/.crt) to trust", show_default=False),
    heartbeat: Optional[int] = typer.Option(None, "--heartbeat", help="Heartbeat interval in seconds", show_default=False),
    connection_timeout: Optional[int] = typer.Option(None, "--connection-timeout", help="Connection timeout in seconds", show_default=False),
    connection_attempts: Optional[int] = typer.Option(None, "--connection-attempts", help="Handshake retry count", show_default=False),
    auth: Optional[str] = typer.Option(None, "--auth", help="SASL mechanism: plain, external, oauth", show_default=False),
    token: Optional[str] = typer.Option(None, "--token", help="OAuth token", show_default=False),
    password_command: Optional[str] = typer.Option(None, "--password-command", help="Command that prints the password", show_default=False),
    connection_name: Optional[str] = typer.Option(None, "--connection-name", help="Label this connection in the broker's connection list", show_default=False),
    client_cert: Optional[str] = typer.Option(None, "--client-cert", help="Client certificate for --auth external", show_default=False),
    client_key: Optional[str] = typer.Option(None, "--client-key", help="Client private key for --auth external", show_default=False),
    management_port: Optional[int] = typer.Option(None, "--management-port", help="Management API port (only used by --use-management)", show_default=False),
    blocked_connection_timeout: Optional[int] = typer.Option(None, "--blocked-connection-timeout", help="How long to keep retrying while the broker blocks publishers", show_default=False),
    profile: Optional[str] = typer.Option(None, "--profile", help="Write into this named profile", show_default=False),
    set_active: bool = typer.Option(False, "--set-active", help="Also make this profile the default"),
    show: bool = typer.Option(False, "--show", help="Display the effective configuration"),
    list_profiles: bool = typer.Option(False, "--list-profiles", help="List configured profiles"),
    unset: List[str] = typer.Option([], "--unset", help="Remove a setting (repeatable)", show_default=False),
) -> None:
    """Write connection settings to ~/.config/mq/config.yaml."""
    config_cmd.execute(
        host=host,
        port=port,
        username=username,
        password=password,
        vhost=vhost,
        ssl=ssl,
        ssl_verify=ssl_verify,
        ssl_cafile=ssl_cafile,
        heartbeat=heartbeat,
        connection_timeout=connection_timeout,
        management_port=management_port,
        blocked_connection_timeout=blocked_connection_timeout,
        connection_attempts=connection_attempts,
        auth=auth,
        token=token,
        password_command=password_command,
        connection_name=connection_name,
        client_cert=client_cert,
        client_key=client_key,
        profile=profile,
        set_active=set_active,
        show=show,
        list_profiles=list_profiles,
        unset=unset,
    )


@app.command()
def declare(
    queue: str = typer.Argument(..., help="Queue name ('' asks the broker to generate one)"),
    durable: bool = typer.Option(False, "--durable", help="Durable queue (survives broker restart)"),
    exclusive: bool = typer.Option(False, "--exclusive", help="Exclusive queue (deleted when connection closes)"),
    auto_delete: bool = typer.Option(False, "--auto-delete", help="Auto-delete queue (deleted when last consumer cancels)"),
    queue_type: Optional[str] = typer.Option(None, "--queue-type", help="classic, quorum or stream"),
    ttl: Optional[int] = typer.Option(None, "--ttl", help="x-message-ttl in milliseconds"),
    expires: Optional[int] = typer.Option(None, "--expires", help="x-expires in milliseconds"),
    max_length: Optional[int] = typer.Option(None, "--max-length", help="x-max-length"),
    max_length_bytes: Optional[int] = typer.Option(None, "--max-length-bytes", help="x-max-length-bytes"),
    dlx: Optional[str] = typer.Option(None, "--dlx", help="x-dead-letter-exchange"),
    dl_routing_key: Optional[str] = typer.Option(None, "--dl-routing-key", help="x-dead-letter-routing-key"),
    overflow: Optional[str] = typer.Option(None, "--overflow", help="x-overflow: reject-publish or drop-head"),
    max_priority: Optional[int] = typer.Option(None, "--max-priority", help="x-max-priority"),
    single_active_consumer: bool = typer.Option(False, "--single-active-consumer", help="x-single-active-consumer"),
    lazy: bool = typer.Option(False, "--lazy", help="x-queue-mode: lazy"),
    arg: List[str] = typer.Option([], "--arg", help="Raw x-argument as key=value (repeatable)", show_default=False),
) -> None:
    """Declare a queue, with the arguments real brokers need."""
    try:
        arguments = declare_cmd.build_arguments(
            arg,
            queue_type=queue_type,
            ttl=ttl,
            expires=expires,
            max_length=max_length,
            max_length_bytes=max_length_bytes,
            dlx=dlx,
            dl_routing_key=dl_routing_key,
            overflow=overflow,
            max_priority=max_priority,
            single_active_consumer=single_active_consumer,
            lazy=lazy,
        )
    except ValueError as e:
        from mq.utils import handle_error

        handle_error(str(e))
        return

    declare_cmd.execute(
        _config(),
        queue,
        durable=durable,
        exclusive=exclusive,
        auto_delete=auto_delete,
        arguments=arguments or None,
    )


@app.command()
def delete(
    queue: str = typer.Argument(..., help="Queue name"),
    if_unused: bool = typer.Option(False, "--if-unused", help="Only delete when nothing is bound to it"),
    if_empty: bool = typer.Option(False, "--if-empty", help="Only delete when it holds no messages"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not prompt"),
) -> None:
    """Delete a queue and everything in it."""
    delete_cmd.execute(_config(), queue, if_unused=if_unused, if_empty=if_empty, assume_yes=yes)


@app.command()
def purge(
    queue: str = typer.Argument(..., help="Queue name"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not prompt"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report what would be removed"),
) -> None:
    """Remove all messages from a queue, keeping the queue itself."""
    purge_cmd.execute(_config(), queue, assume_yes=yes, dry_run=dry_run)


@app.command()
def stats(
    queue: str = typer.Argument(..., help="Queue name"),
) -> None:
    """Depth and consumer count for one queue, via a passive declare."""
    stats_cmd.execute(_config(), queue)


@app.command(name="info-queue")
def info_queue(
    queue: str = typer.Argument(..., help="Queue name"),
    expect_durable: Optional[bool] = typer.Option(
        None, "--expect-durable/--expect-transient", help="Assert the queue's durability"
    ),
    expect_quorum: bool = typer.Option(False, "--expect-quorum", help="Assert x-queue-type=quorum"),
    expect_classic: bool = typer.Option(False, "--expect-classic", help="Assert x-queue-type=classic"),
    expect_ttl: Optional[int] = typer.Option(None, "--expect-ttl", help="Assert x-message-ttl"),
    expect_dlx: Optional[str] = typer.Option(None, "--expect-dlx", help="Assert x-dead-letter-exchange"),
    arg: List[str] = typer.Option([], "--expect-arg", help="Assert any x-argument as key=value (repeatable)", show_default=False),
) -> None:
    """Inspect a queue and assert what it should be, over AMQP only."""
    info_queue_cmd.execute(
        _config(),
        queue,
        expect_durable=expect_durable,
        expect_quorum=expect_quorum,
        expect_classic=expect_classic,
        expect_ttl=expect_ttl,
        expect_dlx=expect_dlx,
        args=arg,
    )


@app.command(name="verify-queue")
def verify_queue(
    queue: str = typer.Argument(..., help="Queue name"),
    arg: List[str] = typer.Option(
        [], "--arg", help="Expected x-arguments as key=value (repeatable)", show_default=False
    ),
    expect_durable: Optional[bool] = typer.Option(
        None, "--expect-durable/--expect-transient", help="Assert the queue's durability"
    ),
) -> None:
    """Exit non-zero when a queue does not match the declaration (for CI)."""
    info_queue_cmd.execute_drift(_config(), queue, arg, args=arg, expect_durable=expect_durable)


@app.command()
def publish(
    queue: str = typer.Argument(..., help="Routing key, or queue name when using the default exchange"),
    file: Optional[str] = typer.Option(None, "--file", help="Read message body from file", show_default=False),
    body: Optional[str] = typer.Option(None, "--body", help="Message body string", show_default=False),
    exchange: str = typer.Option("", "--exchange", "-e", help="Target exchange (default: the default exchange)"),
    routing_key: Optional[str] = typer.Option(None, "--routing-key", "-r", help="Override the routing key", show_default=False),
    mandatory: bool = typer.Option(False, "--mandatory", help="Fail if no queue is bound to the routing key"),
    persistent: bool = typer.Option(False, "--persistent", help="Persistent message (delivery_mode=2)"),
    content_type: Optional[str] = typer.Option(None, "--content-type", help="Content type of the message", show_default=False),
    header: List[str] = typer.Option([], "--header", help="Header key=value (repeatable)", show_default=False),
    correlation_id: Optional[str] = typer.Option(None, "--correlation-id", show_default=False),
    message_id: Optional[str] = typer.Option(None, "--message-id", show_default=False),
    app_id: Optional[str] = typer.Option(None, "--app-id", show_default=False),
    expiration: Optional[str] = typer.Option(None, "--expiration", help="Per-message TTL in milliseconds", show_default=False),
    priority: Optional[int] = typer.Option(None, "--priority", show_default=False),
    reply_to: Optional[str] = typer.Option(None, "--reply-to", show_default=False),
    type: Optional[str] = typer.Option(None, "--type", help="AMQP message type", show_default=False),
) -> None:
    """Publish a message and wait for the broker to confirm it."""
    publish_cmd.execute(
        _config(),
        queue,
        file=file,
        body=body,
        exchange=exchange,
        routing_key=routing_key,
        mandatory=mandatory,
        persistent=persistent,
        content_type=content_type,
        headers=header,
        correlation_id=correlation_id,
        message_id=message_id,
        app_id=app_id,
        expiration=expiration,
        priority=priority,
        reply_to=reply_to,
        msg_type=type,
    )


@app.command()
def get(
    queue: str = typer.Argument(..., help="Queue name"),
    index: int = typer.Option(1, "--index", "-n", help="Message position (1-based) in the queue", show_default=True),
    ack: bool = typer.Option(False, "--ack", help="Acknowledge and remove the message"),
    requeue: bool = typer.Option(True, "--requeue/--no-requeue", help="Put the message back on the queue"),
    pretty: bool = typer.Option(True, "--pretty/--no-pretty", help="Pretty-print a JSON body"),
    jq: Optional[str] = typer.Option(None, "--jq", help="Dotted path into the body, e.g. .event.id", show_default=False),
) -> None:
    """Fetch one message by position, leaving it in place by default."""
    get_cmd.execute(
        _config(), queue, index=index, ack=ack, requeue=requeue, pretty=pretty, jq_expr=jq
    )


@app.command()
def consume(
    queue: str = typer.Argument(..., help="Queue name"),
    auto_ack: bool = typer.Option(False, "--auto-ack", help="Auto acknowledge messages"),
    limit: Optional[int] = typer.Option(None, "--limit", help="Stop after N matching messages", show_default=False),
    prefetch: int = typer.Option(1, "--prefetch", help="Unacked messages allowed per consumer"),
    json: bool = typer.Option(False, "--json", help="Output messages as JSON"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON bodies"),
    jq: Optional[str] = typer.Option(None, "--jq", help="Dotted path into the body", show_default=False),
    header: Optional[List[str]] = typer.Option(None, "--header", help="Only messages with these headers (repeatable)", show_default=False),
) -> None:
    """Consume messages from a queue."""
    consume_cmd.execute(
        _config(),
        queue,
        auto_ack=auto_ack,
        limit=limit,
        prefetch=prefetch,
        as_json=json,
        pretty=pretty,
        jq_expr=jq,
        header_filter=header,
    )


@app.command()
def info(
    queue: Optional[str] = typer.Option(None, "--queue", "-q", help="Show details for one queue", show_default=False),
    no_probe: bool = typer.Option(False, "--no-probe", help="Print settings only, do not touch the broker"),
) -> None:
    """Show the broker's self-report, the local settings, and permissions."""
    info_cmd.execute(_config(), queue=queue, probe=not no_probe)


@app.command()
def peek(
    queue: str = typer.Argument(..., help="Queue name"),
    count: int = typer.Option(1, "--count", "-c", help="Number of messages to peek"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON bodies"),
    json: bool = typer.Option(False, "--json", help="Output messages as JSON"),
) -> None:
    """Read messages without removing them."""
    peek_cmd.execute(_config(), queue, count=count, pretty=pretty, as_json=json)


exchange_app = typer.Typer(help="Manage exchanges", no_args_is_help=True)
app.add_typer(exchange_app, name="exchange")


@exchange_app.command("declare")
def exchange_declare(
    exchange: str = typer.Argument(..., help="Exchange name"),
    type: str = typer.Option("direct", "--type", "-t", help="direct, fanout, topic, headers"),
    durable: bool = typer.Option(False, "--durable", help="Durable exchange"),
    auto_delete: bool = typer.Option(False, "--auto-delete", help="Delete when the last queue unbinds"),
    internal: bool = typer.Option(False, "--internal", help="Only publishable from other exchanges"),
    arg: List[str] = typer.Option([], "--arg", help="x-argument as key=value (repeatable)", show_default=False),
) -> None:
    """Declare an exchange."""
    from mq.services.queue_service import parse_arguments
    from mq.utils import handle_error

    try:
        arguments = parse_arguments(arg)
    except ValueError as e:
        handle_error(str(e))
        return
    exchange_cmd.execute_declare(
        _config(), exchange, exchange_type=type, durable=durable,
        auto_delete=auto_delete, internal=internal, arguments=arguments or None,
    )


@exchange_app.command("delete")
def exchange_delete(
    exchange: str = typer.Argument(..., help="Exchange name"),
    if_unused: bool = typer.Option(False, "--if-unused", help="Only delete when nothing is bound"),
) -> None:
    """Delete an exchange."""
    exchange_cmd.execute_delete(_config(), exchange, if_unused=if_unused)


@exchange_app.command("list")
def exchange_list(
    pattern: Optional[str] = typer.Option(None, "--pattern", help="Glob filter on exchange names"),
    exchanges: Optional[List[str]] = typer.Option(None, "--exchanges", help="Comma-separated names to check", show_default=False),
) -> None:
    """List exchanges using passive declares (no management port)."""
    exchange_cmd.execute_list(_config(), pattern=pattern, exchanges=exchanges)


@exchange_app.command("info")
def exchange_info(
    exchange: str = typer.Argument(..., help="Exchange name"),
    expect_type: Optional[str] = typer.Option(None, "--expect-type", help="Assert the exchange type"),
    expect_durable: Optional[bool] = typer.Option(None, "--expect-durable/--expect-transient", help="Assert durability"),
) -> None:
    """Assert what an exchange is, using 406 PRECONDITION_FAILED probes."""
    exchange_cmd.execute_info(
        _config(), exchange, exchange_type=expect_type, durable=expect_durable
    )


@app.command()
def bind(
    queue: str = typer.Argument(..., help="Queue name"),
    exchange: str = typer.Argument(..., help="Exchange name"),
    routing_key: str = typer.Argument("", help="Routing key"),
    arg: List[str] = typer.Option([], "--arg", help="Binding argument as key=value (repeatable)", show_default=False),
) -> None:
    """Bind a queue to an exchange."""
    binding_cmd.execute_bind(_config(), queue, exchange, routing_key, args=arg)


@app.command()
def unbind(
    queue: str = typer.Argument(..., help="Queue name"),
    exchange: str = typer.Argument(..., help="Exchange name"),
    routing_key: str = typer.Argument("", help="Routing key"),
    arg: List[str] = typer.Option([], "--arg", help="Binding argument as key=value (repeatable)", show_default=False),
) -> None:
    """Unbind a queue from an exchange."""
    binding_cmd.execute_unbind(_config(), queue, exchange, routing_key, args=arg)


@app.command()
def whoami(
    queue: Optional[str] = typer.Option(
        None, "--queue", "-q",
        help="Probe against an existing queue; needs only read permission",
        show_default=False,
    ),
) -> None:
    """Show the current user, vhost and effective permissions."""
    whoami_cmd.execute(_config(), queue=queue)


@app.command()
def doctor(
    readonly: bool = typer.Option(
        False, "--readonly", help="Skip the write probes (safe with read-only permissions)"
    ),
    status: bool = typer.Option(
        False, "--status", help="One-line readiness summary, exit 1 when unhealthy"
    ),
) -> None:
    """Run connectivity, TLS, auth and broker capability checks."""
    config = _config()
    if status:
        doctor_cmd.execute_status(config)
    else:
        doctor_cmd.execute(config, readonly=readonly)


@app.command()
def move(
    source: str = typer.Argument(..., help="Source queue"),
    destination: str = typer.Argument(..., help="Destination queue"),
    count: int = typer.Option(1, "--count", "-c", help="Messages to move, -1 for all"),
    exchange: str = typer.Option("", "--exchange", "-e", help="Destination exchange"),
    routing_key: Optional[str] = typer.Option(None, "--routing-key", "-r", help="Override the routing key", show_default=False),
    persistent: Optional[bool] = typer.Option(
        None, "--persistent/--transient", help="Force the delivery mode on moved messages"
    ),
    mandatory: bool = typer.Option(False, "--mandatory", help="Fail if nothing is bound to the destination"),
) -> None:
    """Move messages between queues, confirming each publish before acking."""
    move_cmd.execute(
        _config(), source, destination, count=count, exchange=exchange,
        routing_key=routing_key, persistent=persistent, mandatory=mandatory,
    )


@app.command()
def retry(
    queue: str = typer.Argument(..., help="DLQ queue name"),
    limit: int = typer.Option(-1, "--limit", "-n", help="Max messages to retry (-1 = all)"),
    target_queue: Optional[str] = typer.Option(None, "--target-queue", help="Force the destination queue", show_default=False),
    target_exchange: Optional[str] = typer.Option(None, "--target-exchange", show_default=False),
    target_routing_key: Optional[str] = typer.Option(None, "--target-routing-key", show_default=False),
    no_mandatory: bool = typer.Option(False, "--no-mandatory", help="Do not fail on unroutable messages"),
) -> None:
    """Re-publish DLQ messages to where they came from, via x-death headers."""
    retry_cmd.execute(
        _config(), queue, limit=limit, target_queue=target_queue,
        target_exchange=target_exchange, target_routing_key=target_routing_key,
        mandatory=not no_mandatory,
    )


@app.command()
def replay(
    queue: str = typer.Argument(..., help="Queue name"),
    limit: int = typer.Option(-1, "--limit", "-n", help="Max messages to replay (-1 = all)"),
    exchange: str = typer.Option("", "--exchange", "-e", help="Exchange to publish to"),
    routing_key: Optional[str] = typer.Option(None, "--routing-key", "-r", show_default=False),
    no_mandatory: bool = typer.Option(False, "--no-mandatory", help="Do not fail on unroutable messages"),
) -> None:
    """Re-publish every message in a queue back onto itself."""
    replay_cmd.execute(
        _config(), queue, limit=limit, exchange=exchange,
        routing_key=routing_key, mandatory=not no_mandatory,
    )


@app.command()
def export(
    queue: str = typer.Argument(..., help="Queue name"),
    output: Optional[str] = typer.Option(None, "--output", "-o", help="Output file path (default: stdout)"),
    limit: int = typer.Option(-1, "--limit", "-n", help="Max messages to export (-1 = all)"),
    destructive: bool = typer.Option(
        False, "--destructive",
        help="Consume the exported messages. Without this the queue is left untouched.",
    ),
) -> None:
    """Export a queue's messages to JSON. Non-destructive unless --destructive."""
    export_import_cmd.execute_export(
        _config(), queue, output=output, limit=limit, destructive=destructive
    )


@app.command(name="import")
def import_(
    queue: str = typer.Argument(..., help="Target queue name"),
    file: str = typer.Argument(..., help="JSON file to import"),
    exchange: str = typer.Option("", "--exchange", "-e", help="Exchange to publish to"),
    mandatory: bool = typer.Option(False, "--mandatory", help="Fail if nothing is bound"),
) -> None:
    """Import messages from a JSON file, publishing with confirms."""
    export_import_cmd.execute_import(_config(), queue, file, exchange=exchange, mandatory=mandatory)


@app.command()
def list(
    pattern: Optional[str] = typer.Option(None, "--pattern", help="Glob filter on queue names"),
    queues: Optional[List[str]] = typer.Option(
        None, "--queues", help="Comma-separated queue names to check", show_default=False
    ),
    use_management: bool = typer.Option(
        False, "--use-management",
        help="If the inventory is empty, fall back to the management HTTP API",
    ),
) -> None:
    """List queues with passive declares. No management port needed."""
    list_cmd.execute(_config(), pattern=pattern, queues=queues, use_management=use_management)


@app.command()
def top(
    limit: int = typer.Option(10, "--limit", "-n", help="Number of queues to show"),
    sort_by: str = typer.Option("ready", "--sort-by", help="Sort by: ready, consumers or name"),
    pattern: Optional[str] = typer.Option(None, "--pattern", help="Glob filter on queue names"),
    queues: Optional[List[str]] = typer.Option(None, "--queues", help="Comma-separated queue names", show_default=False),
) -> None:
    """Show the deepest or busiest tracked queues."""
    top_cmd.execute(_config(), limit=limit, sort_by=sort_by, pattern=pattern, queues=queues)


@app.command()
def watch(
    queue: str = typer.Argument(..., help="Queue name"),
    interval: float = typer.Option(1.0, "--interval", "-i", help="Poll interval in seconds"),
    count: Optional[int] = typer.Option(None, "--count", "-n", help="Stop after N samples", show_default=False),
    json: bool = typer.Option(False, "--json", help="Emit one JSON object per sample"),
) -> None:
    """Poll a queue over AMQP and show the depth trend."""
    watch_cmd.execute(_config(), queue, interval=interval, count=count, as_json=json)


inventory_app = typer.Typer(
    help="Track the queue and exchange names this tool knows about",
    no_args_is_help=True,
)
app.add_typer(inventory_app, name="inventory")


@inventory_app.command("list")
def inventory_list(
    kind: str = typer.Option("all", "--kind", help="all, queue or exchange"),
) -> None:
    """Show the local inventory that 'mq list' and 'mq exchange list' use."""
    inventory_cmd.execute_list(_local_config(), kind=kind)


@inventory_app.command("add")
def inventory_add(
    names: List[str] = typer.Argument(..., help="Queue name(s), comma separated"),
    exchange: bool = typer.Option(False, "--exchange", help="Record exchanges instead of queues"),
    tag: List[str] = typer.Option([], "--tag", help="Attach a tag (repeatable)", show_default=False),
    note: Optional[str] = typer.Option(None, "--note", show_default=False),
) -> None:
    """Add names so they can be listed without a management port."""
    inventory_cmd.execute_add(
        _local_config(), names, kind="exchange" if exchange else "queue", tag=tag, note=note
    )


@inventory_app.command("rm")
def inventory_rm(
    names: List[str] = typer.Argument(..., help="Name(s) to forget"),
    exchange: bool = typer.Option(False, "--exchange", help="Remove exchanges instead of queues"),
) -> None:
    """Remove names from the inventory."""
    inventory_cmd.execute_remove(_local_config(), names, kind="exchange" if exchange else "queue")


benchmark_app = typer.Typer(help="Run benchmarks", no_args_is_help=True)
app.add_typer(benchmark_app, name="benchmark")


@benchmark_app.command("publish")
def benchmark_publish(
    queue: str = typer.Argument(..., help="Queue name"),
    count: int = typer.Option(1000, "--count", "-c", help="Number of messages"),
    size: int = typer.Option(1024, "--size", "-s", help="Message size in bytes"),
    exchange: str = typer.Option("", "--exchange", "-e", help="Target exchange"),
    persistent: bool = typer.Option(False, "--persistent", help="Persistent messages"),
) -> None:
    """Measure confirmed publish throughput."""
    benchmark_cmd.execute_publish(
        _config(), queue, count=count, size=size, exchange=exchange, persistent=persistent
    )


@benchmark_app.command("consume")
def benchmark_consume(
    queue: str = typer.Argument(..., help="Queue name"),
    count: int = typer.Option(1000, "--count", "-c", help="Number of messages to wait for"),
    prefetch: int = typer.Option(50, "--prefetch", help="Unacked messages allowed"),
) -> None:
    """Measure consume throughput with acks."""
    benchmark_cmd.execute_consume(_config(), queue, count=count, prefetch=prefetch)


@app.command()
def version() -> None:
    """Print the tool version."""
    from mq.connection import CLIENT_NAME, CLIENT_VERSION

    from mq.utils import is_json_mode, print_json

    if is_json_mode():
        print_json({"name": CLIENT_NAME, "version": CLIENT_VERSION})
    else:
        print(f"{CLIENT_NAME} {CLIENT_VERSION}")


if __name__ == "__main__":
    try:
        app()
    except ConfigError as e:  # pragma: no cover - safety net
        import sys

        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)
