"""Configuration assembly: URLs, profiles, env-only operation, auth modes."""

from __future__ import annotations

import contextlib
import os
import unittest

from mq import config as config_module
from mq.config import ConfigError, load_config, load_local_config, set_overrides, set_profile
from tests.support import QuietTestCase, clean_env, temp_config_dir


@contextlib.contextmanager
def _noop():
    """Clears any CLI overrides a previous test left behind."""
    set_overrides()
    set_profile(None)
    try:
        yield
    finally:
        set_overrides()
        set_profile(None)


class ConfigFileMissingIsNotFatal(QuietTestCase):
    """``load_config`` used to sys.exit when the file was absent, which blocked
    ``MQ_URL=... mq ping`` and every CI use."""

    def test_environment_only(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqp://alice:pw@rabbit.internal:5672/prod"
            config = load_config()
            self.assertEqual("rabbit.internal", config.host)
            self.assertEqual(5672, config.port)
            self.assertEqual("alice", config.username)
            self.assertEqual("pw", config.password)
            self.assertEqual("prod", config.vhost)
            self.assertFalse(config.ssl)

    def test_nothing_configured_raises_a_helpful_error(self):
        with temp_config_dir(), clean_env(), _noop():
            with self.assertRaises(ConfigError) as raised:
                load_config()
            message = str(raised.exception)
            self.assertIn("host", message)
            self.assertIn("MQ_URL", message)

    def test_local_config_never_demands_credentials(self):
        with temp_config_dir(), clean_env(), _noop():
            config = load_local_config()
            self.assertEqual("/", config.vhost)
            self.assertEqual("", config.host)


class UrlParsing(QuietTestCase):
    def test_amqps_implies_tls_and_port_5671(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqps://u:p@secure.host:5671/v1"
            config = load_config()
            self.assertTrue(config.ssl)
            self.assertEqual(5671, config.port)
            self.assertEqual("v1", config.vhost)

    def test_amqps_defaults_to_5671(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqps://u:p@secure.host/"
            self.assertEqual(5671, load_config().port)

    def test_percent_encoded_vhost(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqp://u:p@h/%2F"
            self.assertEqual("/", load_config().vhost)

    def test_query_parameters(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = (
                "amqp://u:p@h:5672/v?heartbeat=15&connection_timeout=3&ssl=false"
            )
            config = load_config()
            self.assertEqual(15, config.heartbeat)
            self.assertEqual(3, config.connection_timeout)
            self.assertFalse(config.ssl)

    def test_discrete_env_beats_the_url(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqp://u:p@h:5672/v"
            os.environ["MQ_HOST"] = "override.host"
            self.assertEqual("override.host", load_config().host)

    def test_bad_scheme_is_rejected(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "http://u:p@h:15672/api"
            with self.assertRaises(ConfigError):
                load_config()


class Precedence(QuietTestCase):
    def test_flags_beat_env_beat_file(self):
        with temp_config_dir(), clean_env(), _noop():
            path = config_module.CONFIG_FILE
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                "host: file.host\nusername: file_user\npassword: file_pass\nvhost: file\n"
            )
            os.environ["MQ_HOST"] = "env.host"
            os.environ["MQ_USERNAME"] = "env_user"

            self.assertEqual("env.host", load_config().host)
            self.assertEqual("env_user", load_config().username)

            set_overrides(host="flag.host")
            self.assertEqual("flag.host", load_config().host)
            set_overrides()

    def test_config_file_only(self):
        with temp_config_dir(), clean_env(), _noop():
            path = config_module.CONFIG_FILE
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("host: h\nusername: u\npassword: p\nvhost: v\n")
            config = load_config()
            self.assertEqual(("h", "u", "p", "v"), (config.host, config.username, config.password, config.vhost))
            self.assertIn("config file", config.source)


class Profiles(QuietTestCase):
    def _write(self, text: str) -> None:
        path = config_module.CONFIG_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def test_selected_profile_overrides_the_flat_settings(self):
        with temp_config_dir(), clean_env(), _noop():
            self._write(
                "host: default.host\nusername: u\npassword: p\n"
                "profiles:\n"
                "  staging:\n"
                "    host: staging.host\n"
                "    vhost: stage\n"
            )
            set_profile("staging")
            config = load_config()
            self.assertEqual("staging.host", config.host)
            self.assertEqual("stage", config.vhost)
            self.assertEqual("staging", config.profile)
            set_profile(None)

    def test_active_profile_is_used_when_none_is_requested(self):
        with temp_config_dir(), clean_env(), _noop():
            self._write(
                "host: default.host\nusername: u\npassword: p\n"
                "active_profile: staging\n"
                "profiles:\n  staging:\n    host: staging.host\n"
            )
            self.assertEqual("staging.host", load_config().host)

    def test_env_profile_beats_the_file_default(self):
        with temp_config_dir(), clean_env(), _noop():
            self._write(
                "host: default.host\nusername: u\npassword: p\n"
                "active_profile: staging\n"
                "profiles:\n  prod:\n    host: prod.host\n"
            )
            os.environ["MQ_PROFILE"] = "prod"
            self.assertEqual("prod.host", load_config().host)

    def test_unknown_profile_lists_the_known_ones(self):
        with temp_config_dir(), clean_env(), _noop():
            self._write(
                "host: h\nusername: u\npassword: p\n"
                "profiles:\n  staging:\n    host: s\n  prod:\n    host: x\n"
            )
            set_profile("nope")
            with self.assertRaises(ConfigError) as raised:
                load_config()
            message = str(raised.exception)
            self.assertIn("prod", message)
            self.assertIn("staging", message)
            set_profile(None)


class Auth(QuietTestCase):
    def test_external_requires_tls(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqp://host/"
            os.environ["MQ_AUTH"] = "external"
            with self.assertRaises(ConfigError) as raised:
                load_config()
            self.assertIn("external requires TLS", str(raised.exception))

    def test_external_does_not_need_a_password(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqps://host/"
            os.environ["MQ_AUTH"] = "external"
            config = load_config()
            self.assertEqual("external", config.auth)

    def test_oauth_requires_a_token(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqp://u@h/"
            os.environ["MQ_AUTH"] = "oauth"
            with self.assertRaises(ConfigError):
                load_config()
            os.environ["MQ_TOKEN"] = "abc"
            self.assertEqual("abc", load_config().token)

    def test_unknown_mechanism_is_rejected(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqp://u:p@h/"
            os.environ["MQ_AUTH"] = "kerberos"
            with self.assertRaises(ConfigError):
                load_config()

    def test_password_command_supplies_the_secret(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqp://u@h/"
            os.environ["MQ_PASSWORD_COMMAND"] = "echo hunter2"
            self.assertEqual("hunter2", load_config().password)

    def test_failing_password_command_is_reported(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqp://u@h/"
            os.environ["MQ_PASSWORD_COMMAND"] = "exit 3"
            with self.assertRaises(ConfigError) as raised:
                load_config()
            self.assertIn("exited with 3", str(raised.exception))

    def test_redacted_hides_secrets(self):
        with temp_config_dir(), clean_env(), _noop():
            config = load_local_config()
            config.host = "h"
            config.username = "u"
            config.password = "secret"
            config.token = "tok"
            redacted = config.redacted
            self.assertEqual("****", redacted.password)
            self.assertEqual("****", redacted.token)
            self.assertEqual("secret", config.password)


class BrokerAlarmDefaults(QuietTestCase):
    def test_blocked_connection_timeout_is_positive_by_default(self):
        with temp_config_dir(), clean_env(), _noop():
            os.environ["MQ_URL"] = "amqp://u:p@h/"
            self.assertGreater(load_config().blocked_connection_timeout, 0)


if __name__ == "__main__":
    unittest.main()
