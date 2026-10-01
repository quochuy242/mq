"""Test helpers: route every service through an in-memory fake broker."""

from __future__ import annotations

import builtins
import contextlib
import importlib
import io
import os
import pathlib
import tempfile
import unittest
from typing import Iterator

from mq.config import MQConfig
from tests.fake_amqp import FakeBroker, FakeConnection

def _mq_modules() -> list:
    """Every module in the package that might hold a connection factory.

    Walking the package beats a hard-coded list: a new command module that
    opens a connection is then covered automatically.
    """
    import pkgutil

    import mq

    found = []
    for info in pkgutil.walk_packages(mq.__path__, prefix="mq."):
        try:
            found.append(importlib.import_module(info.name))
        except Exception:
            continue
    found.append(importlib.import_module("mq.connection"))
    return found


def make_config(**overrides) -> MQConfig:
    defaults = dict(
        host="localhost",
        port=5672,
        username="mq",
        password="secret",
        vhost="/",
        ssl=False,
        ssl_verify=True,
        ssl_cafile=None,
        heartbeat=60,
        # 1s keeps the doctor's real DNS/TCP probes fast; there is no broker in
        # the test environment, so those checks are expected to fail anyway.
        connection_timeout=1,
        management_port=None,
        blocked_connection_timeout=1,
        connection_attempts=1,
        auth="plain",
        token=None,
        password_command=None,
        connection_name=None,
        client_cert=None,
        client_key=None,
        source="tests",
    )
    defaults.update(overrides)
    return MQConfig(**defaults)  # type: ignore[arg-type]


@contextlib.contextmanager
def fake_connection(broker: FakeBroker) -> Iterator[FakeBroker]:
    """Make every ``create_channel``/``create_connection`` in mq use ``broker``.

    Also sandboxes the config directory: any broker call may record inventory,
    and a test must never write to the developer's real files.
    """
    def create_channel(config, blocked_state=None, confirm=False):
        from mq.connection import BlockedState

        state = blocked_state if blocked_state is not None else BlockedState()
        conn = FakeConnection(broker)
        channel = conn.channel()
        conn.add_on_connection_blocked_callback(state.blocked_callback)
        conn.add_on_connection_unblocked_callback(state.unblocked_callback)
        if confirm:
            channel.confirm_delivery()
        return conn, channel, state

    def create_connection(config, blocked_state=None):
        conn = FakeConnection(broker)
        if blocked_state is not None:
            conn.add_on_connection_blocked_callback(blocked_state.blocked_callback)
            conn.add_on_connection_unblocked_callback(blocked_state.unblocked_callback)
        return conn

    patched: list[tuple[object, str, object]] = []
    for module in _mq_modules():
        for attr, replacement in (
            ("create_channel", create_channel),
            ("create_connection", create_connection),
        ):
            if hasattr(module, attr):
                patched.append((module, attr, getattr(module, attr)))
                setattr(module, attr, replacement)

    with temp_config_dir():
        try:
            yield broker
        finally:
            for module, attr, original_value in patched:
                setattr(module, attr, original_value)


_sandbox_stack: list[pathlib.Path] = []


@contextlib.contextmanager
def temp_config_dir() -> Iterator[str]:
    """Point the config and inventory files at a throwaway directory.

    Re-entrant: nesting two sandboxes would make the inner guard reject the
    outer one's files, so a nested call simply reuses the active sandbox.

    Every module that caches the path is repointed, and a guard fails loudly if
    anything tries to write the tool's settings outside the sandbox -- a test
    must never edit the developer's real config.
    """
    import mq.config as config_module
    import mq.inventory as inventory_module

    if _sandbox_stack:
        yield str(_sandbox_stack[-1])
        return

    with tempfile.TemporaryDirectory() as tmp:
        sandbox = pathlib.Path(tmp)
        original = (config_module.CONFIG_DIR, config_module.CONFIG_FILE)
        config_module.CONFIG_DIR = sandbox
        config_module.CONFIG_FILE = sandbox / "config.yaml"
        inventory_module.INVENTORY_FILE = sandbox / "inventory.yaml"
        _sandbox_stack.append(sandbox)

        real_open = builtins.open
        guarded_names = {"config.yaml", "inventory.yaml"}

        def guarded_open(file, mode="r", *args, **kwargs):
            # Only the tool's own settings files are protected here: a test is
            # still allowed to write the export fixture it passes to --output.
            if (
                any(flag in mode for flag in ("w", "a", "x", "+"))
                and isinstance(file, (str, pathlib.Path))
                and pathlib.Path(file).name in guarded_names
                and sandbox.resolve() not in pathlib.Path(file).resolve().parents
            ):
                raise AssertionError(
                    f"a test tried to write {file} outside its sandbox"
                )
            return real_open(file, mode, *args, **kwargs)

        builtins.open = guarded_open
        try:
            yield tmp
        finally:
            builtins.open = real_open
            _sandbox_stack.pop()
            config_module.CONFIG_DIR, config_module.CONFIG_FILE = original


@contextlib.contextmanager
def clean_env(*names: str) -> Iterator[None]:
    """Remove MQ_* variables so a developer's real config cannot leak into a test."""
    saved = {name: os.environ.pop(name, None) for name in names}
    saved_all = {
        key: value
        for key, value in list(os.environ.items())
        if key.startswith("MQ_") and key not in saved
    }
    for key in saved_all:
        del os.environ[key]
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is not None:
                os.environ[key] = value
        for key, value in saved_all.items():
            os.environ[key] = value


class QuietTestCase(unittest.TestCase):
    """Base case that swallows command output so the test log stays readable."""

    def setUp(self) -> None:
        self._buffer = io.StringIO()
        self._redirect = contextlib.redirect_stdout(self._buffer)
        self._redirect.__enter__()

    def tearDown(self) -> None:
        self._redirect.__exit__(None, None, None)

    @property
    def output(self) -> str:
        return self._buffer.getvalue()
