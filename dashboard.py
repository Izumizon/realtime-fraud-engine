import json
from datetime import datetime
from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import text

from database import AsyncSessionLocal

app = FastAPI(title="Fraud Operations Dashboard", version="1.0.0")

WINDOW_SECONDS: dict[str, int | None] = {
"5m": 5 * 60,
"1h": 60 * 60,
"24h": 24 * 60 * 60,
"all": None,
}

WINDOW_LABELS: dict[str, str] = {
"5m": "Last 5 minutes",
"1h": "Last 1 hour",
"24h": "Last 24 hours",
"all": "All time",
}


def build_time_filter(window: str) -> tuple[str, dict[str, Any], str]:
    """
    Build a safe SQL time filter for dashboard queries.

    The window value is mapped from a fixed allow-list, so the generated SQL
    fragment is not user-controlled.
    """

    selected_window = window if window in WINDOW_SECONDS else "1h"
    window_seconds = WINDOW_SECONDS[selected_window]

    if window_seconds is None:
        return "", {}, selected_window

    return (
        "WHERE received_at >= "
        "((NOW() AT TIME ZONE 'UTC') - (:window_seconds * INTERVAL '1 second'))",
        {"window_seconds": window_seconds},
        selected_window,
    )

def percentage(part: int, total: int) -> float:
    if total == 0:
        return 0.0

    return round((part / total) * 100, 1)


DASHBOARD_HTML = """

<!DOCTYPE html>

<html lang="en">
<head>
    <meta charset="UTF-8" />
    <title>Fraud Operations Dashboard</title>
    <style>
        :root {
            --bg: #0f0f10;
            --panel: #17181b;
            --panel-soft: #1f2025;
            --text: #f5f5f5;
            --muted: #a7a7a7;
            --border: #303136;
            --green: #22c55e;
            --yellow: #eab308;
            --red: #ef4444;
            --purple: #a855f7;
            --orange: #f97316;
            --crimson: #dc2626;
            --blue: #2563eb;
        }


    * {
        box-sizing: border-box;
    }

    body {
        margin: 0;
        background: var(--bg);
        color: var(--text);
        font-family: Inter, Segoe UI, Arial, sans-serif;
    }

    .page {
        padding: 32px 40px;
    }

    .header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 28px;
        gap: 24px;
    }

    h1 {
        margin: 0;
        font-size: 28px;
        font-weight: 650;
        letter-spacing: -0.04em;
    }

    .window-context {
        color: var(--muted);
        font-size: 14px;
        margin-top: 8px;
    }

    .window-context strong {
        color: var(--text);
    }

    .controls {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
        justify-content: flex-end;
    }

    button {
        border: 0;
        color: white;
        padding: 12px 20px;
        border-radius: 999px;
        cursor: pointer;
        font-weight: 700;
        background: var(--panel-soft);
    }

    button.primary {
        background: #1e3a8a;
    }

    button.window-button {
        background: var(--panel-soft);
        color: var(--muted);
    }

    button.window-button.active {
        background: #2563eb;
        color: white;
    }

    .kpis {
        display: grid;
        grid-template-columns: repeat(6, 1fr);
        gap: 16px;
        margin-bottom: 20px;
    }

    .card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: 18px;
    }

    .kpi-label {
        color: var(--muted);
        font-size: 13px;
        margin-bottom: 10px;
    }

    .kpi-value {
        font-size: 30px;
        font-weight: 750;
        letter-spacing: -0.04em;
    }

    .analytics {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 20px;
        margin-bottom: 20px;
    }

    .analytics-title {
        margin: 0 0 14px;
        font-size: 16px;
    }

    .stacked-bar {
        display: flex;
        overflow: hidden;
        height: 18px;
        border-radius: 999px;
        background: var(--panel-soft);
        margin-bottom: 14px;
    }

    .bar-approved {
        background: var(--green);
    }

    .bar-review {
        background: var(--yellow);
    }

    .bar-declined {
        background: var(--red);
    }

    .bar-low {
        background: var(--green);
    }

    .bar-medium {
        background: var(--yellow);
    }

    .bar-high {
        background: var(--red);
    }

    .legend {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 10px;
    }

    .legend-item {
        background: var(--panel-soft);
        border-radius: 12px;
        padding: 12px;
    }

    .legend-label {
        color: var(--muted);
        font-size: 12px;
        margin-bottom: 6px;
    }

    .legend-value {
        font-size: 18px;
        font-weight: 750;
    }

    .layout {
        display: grid;
        grid-template-columns: 1fr 320px;
        gap: 20px;
    }

    table {
        width: 100%;
        border-collapse: collapse;
        overflow: hidden;
    }

    th {
        color: var(--muted);
        font-size: 13px;
        text-align: left;
        padding: 14px 12px;
        border-bottom: 1px solid var(--border);
    }

    td {
        padding: 14px 12px;
        border-bottom: 1px solid #24252a;
        font-size: 14px;
        vertical-align: middle;
    }

    tr:hover {
        background: #15161a;
    }

    .mono {
        font-family: Consolas, monospace;
        color: #d7d7d7;
    }

    .amount {
        font-weight: 700;
    }

    .status {
        display: inline-flex;
        align-items: center;
        padding: 6px 10px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 800;
    }

    .status-approved {
        background: rgba(34, 197, 94, 0.15);
        color: var(--green);
    }

    .status-review {
        background: rgba(234, 179, 8, 0.18);
        color: var(--yellow);
    }

    .status-declined {
        background: rgba(239, 68, 68, 0.16);
        color: var(--red);
    }

    .pill {
        display: inline-flex;
        padding: 5px 9px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 750;
        margin: 2px 4px 2px 0;
        white-space: nowrap;
    }

    .pill-purple {
        background: rgba(168, 85, 247, 0.16);
        color: var(--purple);
    }

    .pill-orange {
        background: rgba(249, 115, 22, 0.16);
        color: var(--orange);
    }

    .pill-crimson {
        background: rgba(220, 38, 38, 0.18);
        color: var(--crimson);
    }

    .pill-blue {
        background: rgba(37, 99, 235, 0.16);
        color: #60a5fa;
    }

    .pill-muted {
        background: #26272c;
        color: var(--muted);
    }

    .side-title {
        margin: 0 0 14px;
        font-size: 16px;
    }

    .vector-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 12px 0;
        border-bottom: 1px solid #292a30;
    }

    .vector-count {
        font-weight: 800;
    }

    .empty {
        color: var(--muted);
        padding: 24px;
        text-align: center;
    }

    .footer {
        margin-top: 18px;
        color: var(--muted);
        font-size: 12px;
    }

    @media (max-width: 1300px) {
        .kpis {
            grid-template-columns: repeat(3, 1fr);
        }

        .analytics {
            grid-template-columns: 1fr;
        }
    }

    @media (max-width: 1100px) {
        .header {
            align-items: flex-start;
            flex-direction: column;
        }

        .controls {
            justify-content: flex-start;
        }

        .kpis {
            grid-template-columns: repeat(2, 1fr);
        }

        .layout {
            grid-template-columns: 1fr;
        }

        .legend {
            grid-template-columns: 1fr;
        }
    }
</style>


</head>
<body>
    <div class="page">
        <div class="header">
            <div>
                <h1>Fraud Operations Dashboard</h1>
                <div class="window-context">
                    Showing: <strong id="active-window-label">Last 1 hour</strong>
                </div>
            </div>


        <div class="controls">
            <button class="window-button" id="window-5m" onclick="setWindow('5m')">5m</button>
            <button class="window-button active" id="window-1h" onclick="setWindow('1h')">1h</button>
            <button class="window-button" id="window-24h" onclick="setWindow('24h')">24h</button>
            <button class="window-button" id="window-all" onclick="setWindow('all')">All</button>
            <button class="primary" onclick="togglePause()" id="pause-btn">Pause</button>
            <button onclick="loadStats()">Refresh</button>
        </div>
    </div>

    <section class="kpis">
        <div class="card">
            <div class="kpi-label">Window Volume</div>
            <div class="kpi-value" id="total-volume">0</div>
        </div>

        <div class="card">
            <div class="kpi-label">All-Time Volume</div>
            <div class="kpi-value" id="all-time-volume">0</div>
        </div>

        <div class="card">
            <div class="kpi-label">Approved</div>
            <div class="kpi-value" id="approved">0</div>
        </div>

        <div class="card">
            <div class="kpi-label">Under Review</div>
            <div class="kpi-value" id="review">0</div>
        </div>

        <div class="card">
            <div class="kpi-label">Declined</div>
            <div class="kpi-value" id="declined">0</div>
        </div>

        <div class="card">
            <div class="kpi-label">Avg Risk</div>
            <div class="kpi-value" id="avg-risk">0</div>
        </div>
    </section>

    <section class="analytics">
        <div class="card">
            <h2 class="analytics-title">Decision Split</h2>

            <div class="stacked-bar">
                <div class="bar-approved" id="decision-approved-bar"></div>
                <div class="bar-review" id="decision-review-bar"></div>
                <div class="bar-declined" id="decision-declined-bar"></div>
            </div>

            <div class="legend">
                <div class="legend-item">
                    <div class="legend-label">Approved</div>
                    <div class="legend-value" id="approved-percent">0%</div>
                </div>
                <div class="legend-item">
                    <div class="legend-label">Review</div>
                    <div class="legend-value" id="review-percent">0%</div>
                </div>
                <div class="legend-item">
                    <div class="legend-label">Declined</div>
                    <div class="legend-value" id="declined-percent">0%</div>
                </div>
            </div>
        </div>

        <div class="card">
            <h2 class="analytics-title">Risk Breakdown</h2>

            <div class="stacked-bar">
                <div class="bar-low" id="risk-low-bar"></div>
                <div class="bar-medium" id="risk-medium-bar"></div>
                <div class="bar-high" id="risk-high-bar"></div>
            </div>

            <div class="legend">
                <div class="legend-item">
                    <div class="legend-label">Low Risk</div>
                    <div class="legend-value" id="low-risk-count">0</div>
                </div>
                <div class="legend-item">
                    <div class="legend-label">Medium Risk</div>
                    <div class="legend-value" id="medium-risk-count">0</div>
                </div>
                <div class="legend-item">
                    <div class="legend-label">High Risk</div>
                    <div class="legend-value" id="high-risk-count">0</div>
                </div>
            </div>
        </div>
    </section>

    <section class="layout">
        <div class="card">
            <table>
                <thead>
                    <tr>
                        <th>Time</th>
                        <th>Trace ID</th>
                        <th>User ID</th>
                        <th>Merchant</th>
                        <th>Amount</th>
                        <th>Risk</th>
                        <th>Status</th>
                        <th>Trigger Rules</th>
                    </tr>
                </thead>
                <tbody id="transaction-feed">
                    <tr>
                        <td colspan="8" class="empty">Waiting for transactions...</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <aside class="card">
            <h2 class="side-title">Top Fraud Vectors</h2>
            <div id="fraud-vectors">
                <div class="empty">No triggered rules yet</div>
            </div>

            <div class="footer">
                Auto-refreshes every 2 seconds. Data source: PostgreSQL ledger.
            </div>
        </aside>
    </section>
</div>

<script>
    let paused = false;
    let selectedWindow = "1h";

    function updateWindowButtons() {
        ["5m", "1h", "24h", "all"].forEach((windowName) => {
            const button = document.getElementById(`window-${windowName}`);
            if (!button) return;

            if (windowName === selectedWindow) {
                button.classList.add("active");
            } else {
                button.classList.remove("active");
            }
        });
    }

    function setWindow(windowName) {
        selectedWindow = windowName;
        updateWindowButtons();
        loadStats();
    }

    function setBarWidth(id, value) {
        document.getElementById(id).style.width = `${value}%`;
    }

    function truncate(value) {
        if (!value) return "";
        if (value.length <= 12) return value;
        return value.slice(0, 6) + "..." + value.slice(-4);
    }

    function statusClass(status) {
        if (status === "APPROVED") return "status status-approved";
        if (status === "STEP-UP_REVIEW") return "status status-review";
        if (status === "DECLINED") return "status status-declined";
        return "status";
    }

    function reasonInfo(reason) {
        const map = {
            "receiver_swarm_detected": ["pill pill-purple", "🟣 Receiver Swarm"],
            "sender_velocity_exceeded": ["pill pill-orange", "🟠 Sender Velocity"],
            "panic_execution_speed": ["pill pill-crimson", "🔴 APP Scam / Panic"],
            "new_payee": ["pill pill-blue", "New Payee"],
            "recent_password_reset": ["pill pill-crimson", "Recent Password Reset"],
            "redis_unavailable_static_only": ["pill pill-muted", "Redis Unavailable"]
        };

        return map[reason] || ["pill pill-muted", reason];
    }

    function renderReasonPills(reasons) {
        if (!reasons || reasons.length === 0) {
            return '<span class="pill pill-muted">No flags</span>';
        }

        return reasons.map((reason) => {
            const [className, label] = reasonInfo(reason);
            return `<span class="${className}">${label}</span>`;
        }).join("");
    }

    function formatAmount(amount, currency) {
        return `${currency} ${(amount / 100).toFixed(2)}`;
    }

    function formatTime(value) {
        if (!value) return "";
        const date = new Date(value);
        return date.toLocaleTimeString();
    }

    function updateDecisionSplit(data) {
        const approvedPercent = data.decision_percentages.approved;
        const reviewPercent = data.decision_percentages.step_up_review;
        const declinedPercent = data.decision_percentages.declined;

        setBarWidth("decision-approved-bar", approvedPercent);
        setBarWidth("decision-review-bar", reviewPercent);
        setBarWidth("decision-declined-bar", declinedPercent);

        document.getElementById("approved-percent").textContent = `${approvedPercent}%`;
        document.getElementById("review-percent").textContent = `${reviewPercent}%`;
        document.getElementById("declined-percent").textContent = `${declinedPercent}%`;
    }

    function updateRiskBreakdown(data) {
        const low = data.risk_breakdown.low;
        const medium = data.risk_breakdown.medium;
        const high = data.risk_breakdown.high;

        setBarWidth("risk-low-bar", low.percent);
        setBarWidth("risk-medium-bar", medium.percent);
        setBarWidth("risk-high-bar", high.percent);

        document.getElementById("low-risk-count").textContent = `${low.count} (${low.percent}%)`;
        document.getElementById("medium-risk-count").textContent = `${medium.count} (${medium.percent}%)`;
        document.getElementById("high-risk-count").textContent = `${high.count} (${high.percent}%)`;
    }

    async function loadStats() {
        if (paused) return;

        const response = await fetch(`/api/stats?window=${selectedWindow}`);
        const data = await response.json();

        document.getElementById("active-window-label").textContent = data.window_label;
        document.getElementById("total-volume").textContent = data.total_volume;
        document.getElementById("all-time-volume").textContent = data.all_time_volume;
        document.getElementById("approved").textContent = data.approved;
        document.getElementById("review").textContent = data.step_up_review;
        document.getElementById("declined").textContent = data.declined;
        document.getElementById("avg-risk").textContent = data.average_risk_score;

        updateDecisionSplit(data);
        updateRiskBreakdown(data);

        const feed = document.getElementById("transaction-feed");

        if (data.latest_transactions.length === 0) {
            feed.innerHTML = '<tr><td colspan="8" class="empty">No transactions in this time window</td></tr>';
        } else {
            feed.innerHTML = data.latest_transactions.map((tx) => `
                <tr>
                    <td>${formatTime(tx.received_at)}</td>
                    <td class="mono" title="${tx.trace_id}">${truncate(tx.trace_id)}</td>
                    <td>${tx.user_id}</td>
                    <td>${tx.merchant_id}</td>
                    <td class="amount">${formatAmount(tx.amount, tx.currency)}</td>
                    <td>${tx.risk_score}</td>
                    <td><span class="${statusClass(tx.status)}">${tx.status}</span></td>
                    <td>${renderReasonPills(tx.risk_reasons)}</td>
                </tr>
            `).join("");
        }

        const vectors = document.getElementById("fraud-vectors");

        if (data.top_fraud_vectors.length === 0) {
            vectors.innerHTML = '<div class="empty">No triggered rules in this time window</div>';
        } else {
            vectors.innerHTML = data.top_fraud_vectors.map((vector) => {
                const [className, label] = reasonInfo(vector.reason);
                return `
                    <div class="vector-row">
                        <span class="${className}">${label}</span>
                        <span class="vector-count">${vector.count}</span>
                    </div>
                `;
            }).join("");
        }
    }

    function togglePause() {
        paused = !paused;
        document.getElementById("pause-btn").textContent = paused ? "Resume" : "Pause";
    }

    loadStats();
    setInterval(loadStats, 2000);
</script>


</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def dashboard_home() -> HTMLResponse:
    return HTMLResponse(DASHBOARD_HTML)

@app.get("/api/stats")
async def dashboard_stats(window: str = "1h") -> JSONResponse:
    time_filter, query_params, selected_window = build_time_filter(window)


    async with AsyncSessionLocal() as session:
        status_result = await session.execute(
            text(
                f"""
                SELECT status, COUNT(*) AS row_count
                FROM transactions
                {time_filter}
                GROUP BY status
                """
            ),
            query_params,
        )

    status_counts = {
        str(row["status"]): int(row["row_count"])
        for row in status_result.mappings()
    }

    summary_result = await session.execute(
        text(
            f"""
            SELECT
                COUNT(*) AS total_volume,
                COALESCE(ROUND(AVG(risk_score)::numeric, 2), 0) AS average_risk_score
            FROM transactions
            {time_filter}
            """
        ),
        query_params,
    )
    summary = summary_result.one()
    total_volume = int(summary.total_volume)

    all_time_result = await session.execute(
        text(
            """
            SELECT COUNT(*) AS total_volume
            FROM transactions
            """
        )
    )
    all_time_summary = all_time_result.one()

    risk_result = await session.execute(
        text(
            f"""
            SELECT
                CASE
                    WHEN risk_score < 40 THEN 'low'
                    WHEN risk_score < 70 THEN 'medium'
                    ELSE 'high'
                END AS risk_band,
                COUNT(*) AS row_count
            FROM transactions
            {time_filter}
            GROUP BY risk_band
            """
        ),
        query_params,
    )

    risk_counts = {
        str(row["risk_band"]): int(row["row_count"])
        for row in risk_result.mappings()
    }

    vectors_result = await session.execute(
        text(
            f"""
            SELECT reason, COUNT(*) AS row_count
            FROM transactions,
                jsonb_array_elements_text(risk_reasons) AS reason
            {time_filter}
            GROUP BY reason
            ORDER BY row_count DESC
            LIMIT 8
            """
        ),
        query_params,
    )

    latest_result = await session.execute(
        text(
            f"""
            SELECT
                transaction_id,
                trace_id,
                user_id,
                merchant_id,
                amount,
                currency,
                status,
                risk_score,
                risk_reasons,
                redis_eval_ms,
                sender_velocity_count,
                receiver_unique_sender_count,
                received_at
            FROM transactions
            {time_filter}
            ORDER BY received_at DESC
            LIMIT 20
            """
        ),
        query_params,
    )

    latest_transactions = []
    for row in latest_result:
        risk_reasons = row.risk_reasons

        if isinstance(risk_reasons, str):
            risk_reasons = json.loads(risk_reasons)

        latest_transactions.append(
            {
                "transaction_id": row.transaction_id,
                "trace_id": row.trace_id,
                "user_id": row.user_id,
                "merchant_id": row.merchant_id,
                "amount": row.amount,
                "currency": row.currency,
                "status": row.status,
                "risk_score": row.risk_score,
                "risk_reasons": risk_reasons or [],
                "redis_eval_ms": row.redis_eval_ms,
                "sender_velocity_count": row.sender_velocity_count,
                "receiver_unique_sender_count": row.receiver_unique_sender_count,
                "received_at": (
                    row.received_at.isoformat()
                    if isinstance(row.received_at, datetime)
                    else str(row.received_at)
                ),
            }
        )

    approved = status_counts.get("APPROVED", 0)
    step_up_review = status_counts.get("STEP-UP_REVIEW", 0)
    declined = status_counts.get("DECLINED", 0)

    low_risk = risk_counts.get("low", 0)
    medium_risk = risk_counts.get("medium", 0)
    high_risk = risk_counts.get("high", 0)

    payload: dict[str, Any] = {
        "window": selected_window,
        "window_label": WINDOW_LABELS[selected_window],
        "total_volume": total_volume,
        "all_time_volume": int(all_time_summary.total_volume),
        "approved": approved,
        "step_up_review": step_up_review,
        "declined": declined,
        "average_risk_score": float(summary.average_risk_score),
        "decision_percentages": {
            "approved": percentage(approved, total_volume),
            "step_up_review": percentage(step_up_review, total_volume),
            "declined": percentage(declined, total_volume),
        },
        "risk_breakdown": {
            "low": {
                "count": low_risk,
                "percent": percentage(low_risk, total_volume),
            },
            "medium": {
                "count": medium_risk,
                "percent": percentage(medium_risk, total_volume),
            },
            "high": {
                "count": high_risk,
                "percent": percentage(high_risk, total_volume),
            },
        },
        "top_fraud_vectors": [
            {
                "reason": str(row["reason"]),
                "count": int(row["row_count"]),
            }
            for row in vectors_result.mappings()
        ],
        "latest_transactions": latest_transactions,
    }

    return JSONResponse(payload)

