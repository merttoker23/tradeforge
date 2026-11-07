"""Deployment state transitions; migration errors never restart application processes."""

import re


def diagnostic(error, fallback):
    code = str(error)
    return (
        code if isinstance(error, ValueError) and re.fullmatch(r"[A-Z_]{1,100}", code) else fallback
    )


def perform(ops):
    result = {
        "outcome": "failed",
        "migration": "not_run",
        "health": "not_run",
        "rollback": "not_run",
        "cleanup": "not_run",
    }
    try:
        ops.prepare()
    except Exception as error:
        result["error"] = diagnostic(error, "PREFLIGHT_FAILED_MANUAL_REVIEW")
        return result
    try:
        ops.migrate()
        result["migration"] = "success"
    except Exception:
        result["migration"] = "failed"
        result["error"] = "MIGRATION_FAILED_DEPLOYMENT_STOPPED"
        return result
    try:
        if not ops.schema_compatible():
            result.update(outcome="manual_review", error="SCHEMA_INCOMPATIBLE")
            return result
        ops.start()
        ops.healthy()
        result["health"] = "healthy"
    except Exception:
        result["health"] = "failed"
        try:
            ops.stop()
            if not ops.schema_compatible():
                result.update(outcome="manual_review", error="ROLLBACK_SCHEMA_INCOMPATIBLE")
                return result
            ops.rollback()
            ops.rollback_healthy()
            result.update(outcome="rolled_back", rollback="healthy")
        except Exception:
            result.update(outcome="manual_review", rollback="failed", error="ROLLBACK_FAILED")
        return result
    try:
        ops.finish()
    except Exception:
        result.update(outcome="manual_review", error="STATE_COMMIT_FAILED")
        return result
    result["outcome"] = "success"
    try:
        ops.cleanup()
        result["cleanup"] = "success"
    except Exception:
        result["cleanup"] = "failed"
        result["error"] = "CLEANUP_FAILED_APPLICATION_HEALTHY"
    return result
