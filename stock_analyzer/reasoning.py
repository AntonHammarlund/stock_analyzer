from typing import Any, Dict


def _format_change(label: str, before: float | None, after: float | None) -> str | None:
    if before is None or after is None:
        return None
    if abs(before - after) < 1e-6:
        return None
    return f"{label} {before:.2f} -> {after:.2f}"


def _feature_label(key: str) -> str:
    labels = {
        "momentum_score": "Momentum",
        "risk_score": "Risk",
        "ml_score": "ML score",
        "ml_confidence": "ML confidence",
    }
    return labels.get(key, key.replace("_", " ").title())


def _heuristic_summary(
    baseline: Dict,
    best: Dict,
    params_before: Dict,
    params_after: Dict,
    applied: bool,
) -> str:
    if baseline.get("status") != "ok" or best.get("status") != "ok":
        return "Insufficient data to tune parameters this quarter."

    lines: list[str] = []
    if applied:
        improvement = best.get("mse", 0) - baseline.get("mse", 0)
        lines.append("Quarterly tuning applied using forward-return errors as the target.")
        if improvement < 0:
            lines.append(f"Mean squared error improved by {abs(improvement):.6f}.")
    else:
        lines.append("Optimization did not beat the baseline by the minimum threshold, so weights were retained.")

    changes = []
    changes.append(
        _format_change(
            "Momentum base", params_before.get("momentum_weight_base"), params_after.get("momentum_weight_base")
        )
    )
    changes.append(
        _format_change(
            "Momentum risk scale",
            params_before.get("momentum_weight_risk_scale"),
            params_after.get("momentum_weight_risk_scale"),
        )
    )
    changes.append(
        _format_change("ML base", params_before.get("ml_weight_base"), params_after.get("ml_weight_base"))
    )
    changes.append(
        _format_change("ML scale", params_before.get("ml_weight_scale"), params_after.get("ml_weight_scale"))
    )
    changes.append(
        _format_change(
            "Expected return scale",
            params_before.get("expected_return_scale"),
            params_after.get("expected_return_scale"),
        )
    )
    changes = [item for item in changes if item]
    if changes and applied:
        lines.append("Updated parameters: " + ", ".join(changes) + ".")

    feature_corr = best.get("feature_corr") or {}
    scored = [(key, value) for key, value in feature_corr.items() if value is not None]
    if scored:
        key, value = max(scored, key=lambda item: abs(item[1]))
        direction = "positive" if value >= 0 else "negative"
        lines.append(
            f"{_feature_label(key)} had the strongest {direction} relationship with realized returns "
            f"({value:.3f})."
        )

    return " ".join(lines).strip()


def build_reasoning_summary(
    baseline: Dict,
    best: Dict,
    params_before: Dict,
    params_after: Dict,
    applied: bool,
    *,
    config: Dict[str, Any] | None = None,
) -> str:
    _ = config
    return _heuristic_summary(baseline, best, params_before, params_after, applied)
