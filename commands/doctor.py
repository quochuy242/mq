from __future__ import annotations

from mq.config import MQConfig
from mq.services.diagnostic_service import run_doctor
from mq.utils import handle_error, is_json_mode, print_json
from mq.utils.output import dim, fail, header, ok, print_pairs, status_marker, warn


def execute(config: MQConfig, readonly: bool = False) -> None:
    try:
        report = run_doctor(config, readonly=readonly)
    except Exception as e:
        handle_error("Diagnostics could not run", e)
        return

    if is_json_mode():
        print_json(
            {
                "checks": [c.to_dict() for c in report.checks],
                "latency_ms": round(report.latency_ms, 2),
                "broker": report.broker,
                "readonly": report.readonly,
                "overall": "pass" if report.all_ok else "fail",
            }
        )
        return

    print(header("Diagnostic report") + (dim("  (read-only mode)") if readonly else ""))
    print()
    for check in report.checks:
        if check.status:
            icon = ok("✔")
        elif check.severity == "warning":
            icon = warn("!")
        else:
            icon = fail("✖")
        duration = f"{dim(f'({check.duration_ms:.1f}ms)')}" if check.duration_ms > 0 else ""
        suffix = f" {dim('[warning]')}" if check.severity == "warning" and not check.status else ""
        print(f"  {icon} {check.name} {duration}{suffix}")
        if check.detail and not check.status:
            print(f"      {fail(check.detail)}")
        elif check.detail:
            print(f"      {dim(check.detail)}")
        if check.hint and not check.status:
            print(f"      {warn('hint: ' + check.hint)}")

    print()
    print_pairs(
        [("Connect latency", f"{report.latency_ms:.1f}ms"),
         ("Checks", f"{sum(1 for c in report.checks if c.status)}/{len(report.checks)} passed")],
    )
    print()
    if report.all_ok:
        print(f"  {ok('All checks passed')}")
    else:
        blocking = [c for c in report.failures if c.severity == "error"]
        soft = [c for c in report.failures if c.severity != "error"]
        if blocking:
            print(f"  {fail(str(len(blocking)) + ' blocking failure(s): ' + ', '.join(c.name for c in blocking))}")
        if soft:
            print(f"  {warn(str(len(soft)) + ' warning(s): ' + ', '.join(c.name for c in soft))}")


def execute_status(config: MQConfig) -> None:
    """A single-line readiness summary, for monitoring scripts."""
    try:
        report = run_doctor(config, readonly=True)
    except Exception as e:
        handle_error("Status check failed", e)
        return

    blocking = [c for c in report.failures if c.severity == "error"]
    payload = {
        "healthy": not blocking,
        "latency_ms": round(report.latency_ms, 2),
        "broker": report.broker.get("product"),
        "version": report.broker.get("version"),
        "cluster": report.broker.get("cluster_name"),
        "failures": [c.name for c in blocking],
        "warnings": [c.name for c in report.failures if c.severity != "error"],
    }
    if is_json_mode():
        print_json(payload)
    else:
        marker = status_marker("ok" if payload["healthy"] else "missing")
        print(f"{marker}  {payload['broker']} {payload['version']}  cluster={payload['cluster']}  "
              f"{payload['latency_ms']:.0f}ms")
        for name in payload["failures"]:
            print(f"      {fail(name)}")
    raise SystemExit(0 if payload["healthy"] else 1)
