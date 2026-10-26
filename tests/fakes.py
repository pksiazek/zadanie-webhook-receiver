class FakeClock:
    """Manually advanced clock for deterministic tests."""

    def __init__(self, start_ms: int = 1_700_000_000_000) -> None:
        self._now_ms = start_ms

    def now_ms(self) -> int:
        return self._now_ms

    def advance(self, ms: int) -> None:
        self._now_ms += ms


# The five example webhooks from the task statement, verbatim:
# https://gist.github.com/kaluzaaa/fcd035829013d9952f71a9a6a59a4d2f
GIST_WEBHOOKS: list[dict] = [
    {
        "idempotency_key": "evt-001",
        "event_type": "order.created",
        "payload": {
            "order_id": "ORD-2024-1234",
            "customer": "Jan Kowalski",
            "amount": 299.99,
            "currency": "PLN",
        },
    },
    {
        "idempotency_key": "evt-002",
        "event_type": "order.paid",
        "payload": {
            "order_id": "ORD-2024-1234",
            "payment_method": "card",
            "paid_at": "2024-12-01T14:30:00Z",
        },
    },
    {
        "idempotency_key": "evt-003",
        "event_type": "order.shipped",
        "payload": {
            "order_id": "ORD-2024-1234",
            "tracking_number": "PL1234567890",
            "carrier": "DPD",
        },
    },
    {
        "idempotency_key": "evt-004",
        "event_type": "user.registered",
        "payload": {
            "user_id": "USR-9876",
            "email": "jan@example.com",
        },
    },
    {
        "idempotency_key": "evt-005",
        "event_type": "payment.failed",
        "payload": {
            "order_id": "ORD-2024-5678",
            "reason": "insufficient_funds",
        },
    },
]
