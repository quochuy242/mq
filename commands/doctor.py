from __future__ import annotations

import pika

from mq.config import MQConfig
from mq.services.diagnostic_service import run_doctor
from mq.utils import handle_error, is_json_mode, print_json
from mq.utils.output import colorize, fail, ok


def execute(config: MQConfig) -> None:
    try:
        report = run_doctor(config)

        if is_json_mode():
            print_json(
                {
                    "checks": [
                        {
                            "name": c.name,
                            "status": c.status,
                            "detail": c.detail,
                            "duration_ms": round(c.duration_ms, 2),
                        }
                        for c in report.checks
                    ],
                    "latency_ms": round(report.latency_ms, 2),
                    "overall": "pass" if report.all_ok else "fail",
                }
            )
            return

        print("Diagnostic Report:")
        print()
        for c in report.checks:
            icon = ok("\u2714") if c.status else fail("\u2716")
            dur = f"({c.duration_ms:.1f}ms)" if c.duration_ms > 0 else ""
            print(f"  {icon} {c.name} {dur}")
            if not c.status and c.detail:
                print(f"     {fail(c.detail)}")

        print()
        lat = f"{report.latency_ms:.1f}ms"
        print(f"  Latency: {lat}")

        if report.all_ok:
            print(f"\n  {ok('All checks passed')}")
        else:
            print(f"\n  {fail('Some checks failed')}")

    except pika.exceptions.AMQPConnectionError as e:
        handle_error(f"Connection failed: {e}")
    except Exception as e:
        handle_error("Unexpected error", e)
