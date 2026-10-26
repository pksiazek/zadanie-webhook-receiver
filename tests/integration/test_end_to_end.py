"""End-to-end scenarios: API + worker + a real SQLite file (spec §14).

Nothing is faked here: the system clock, the real simulation (with a
short duration) and, in the process scenarios, a separate
``python -m webhook_receiver worker`` sharing the database file.
"""

import os

from tests.fakes import GIST_WEBHOOKS
from tests.integration.helpers import (
    assert_settled,
    make_settings,
    post,
    serve,
    wait_for_status,
    worker_id,
    worker_process,
)
from webhook_receiver.__main__ import embedded_worker_lifespan
from webhook_receiver.api.app import create_app

EMBEDDED_ID = worker_id(os.getpid())


def _webhook(key: str) -> dict:
    return {
        "idempotency_key": key,
        "event_type": "test.event",
        "payload": {"n": key},
    }


def _assert_completed_once(body: dict, processed_by: str) -> None:
    assert body["attempts"] == 1
    assert body["last_error"] is None
    assert body["finished_at"] is not None
    assert body["result"]["processed_by"] == processed_by
    assert body["result"]["attempt"] == 1


async def test_gist_examples_complete_end_to_end(tmp_path):
    settings = make_settings(tmp_path / "e2e.db")
    app = create_app(
        settings, extra_lifespan=embedded_worker_lifespan(settings)
    )
    async with serve(app) as client:
        for webhook in GIST_WEBHOOKS:
            accepted = await post(client, webhook)
            assert accepted["status"] == "pending"
        for webhook in GIST_WEBHOOKS:
            key = webhook["idempotency_key"]
            body = await wait_for_status(client, key, "completed")
            assert body["event_type"] == webhook["event_type"]
            assert body["source"] == "e2e"
            _assert_completed_once(body, EMBEDDED_ID)
            duration = body["result"]["duration_ms"]
            assert 50 <= duration <= 100


async def test_duplicate_during_processing_not_reprocessed(tmp_path):
    # Long enough processing to send the duplicate while it runs.
    settings = make_settings(
        tmp_path / "dup.db",
        simulated_min_seconds=0.8,
        simulated_max_seconds=1.0,
    )
    app = create_app(
        settings, extra_lifespan=embedded_worker_lifespan(settings)
    )
    webhook = GIST_WEBHOOKS[0]
    key = webhook["idempotency_key"]
    async with serve(app) as client:
        await post(client, webhook)
        await wait_for_status(client, key, "processing")

        during = await post(client, webhook, expected=200)
        assert during["duplicate"] is True
        assert during["status"] == "processing"
        assert during["attempts"] == 1

        done = await wait_for_status(client, key, "completed")
        _assert_completed_once(done, EMBEDDED_ID)

        after = await post(client, webhook, expected=200)
        assert after["duplicate"] is True
        assert after["status"] == "completed"
        assert after["result"] == done["result"]
        assert after["attempts"] == 1
        assert after["finished_at"] == done["finished_at"]
        await assert_settled(client, key, done, settings)


async def test_restart_api_and_worker_preserves_events(tmp_path):
    settings = make_settings(tmp_path / "restart.db")
    keys = [f"restart-{i}" for i in range(3)]

    # First run: API only; events are accepted, then the process "dies".
    async with serve(create_app(settings)) as client:
        for key in keys:
            await post(client, _webhook(key))
        for key in keys:
            response = await client.get(f"/webhooks/{key}")
            assert response.json()["status"] == "pending"

    # Second run on the same file, now with a worker.
    app = create_app(
        settings, extra_lifespan=embedded_worker_lifespan(settings)
    )
    async with serve(app) as client:
        for key in keys:
            body = await wait_for_status(client, key, "completed")
            _assert_completed_once(body, EMBEDDED_ID)
        # A client that never got its 202 before the restart retries
        # (spec §4): it gets the stored event, nothing is reprocessed.
        before = (await client.get(f"/webhooks/{keys[0]}")).json()
        retried = await post(client, _webhook(keys[0]), expected=200)
        assert retried["duplicate"] is True
        assert retried["status"] == "completed"
        assert retried["attempts"] == 1
        await assert_settled(client, keys[0], before, settings)


async def test_separate_worker_process_shares_database(tmp_path):
    settings = make_settings(tmp_path / "shared.db")
    webhook = GIST_WEBHOOKS[1]
    key = webhook["idempotency_key"]
    async with serve(create_app(settings)) as client:
        with worker_process(settings, tmp_path / "worker.log") as worker:
            await post(client, webhook)
            body = await wait_for_status(
                client, key, "completed", worker=worker
            )
            # Processed by the subprocess, not by anything in this one.
            _assert_completed_once(body, worker.worker_id)
            assert worker.worker_id != EMBEDDED_ID

            exit_code = await worker.terminate()
            assert exit_code == 0, worker.diagnostics()


async def test_crashed_worker_lease_is_recovered(tmp_path):
    # The first worker is SIGKILLed mid-attempt; its lease (3 s) must
    # expire before a second worker may take the event over (spec §4).
    slow = make_settings(
        tmp_path / "crash.db",
        simulated_min_seconds=2.0,
        simulated_max_seconds=2.5,
        lease_seconds=3,
    )
    fast = make_settings(tmp_path / "crash.db", lease_seconds=3)
    webhook = GIST_WEBHOOKS[2]
    key = webhook["idempotency_key"]
    async with serve(create_app(slow)) as client:
        with worker_process(slow, tmp_path / "w1.log") as first:
            await post(client, webhook)
            claimed = await wait_for_status(
                client, key, "processing", worker=first
            )
            assert claimed["attempts"] == 1
            exit_code = await first.kill()
            assert exit_code < 0, first.diagnostics()

        # The killed attempt left the event in processing, unfinished.
        response = await client.get(f"/webhooks/{key}")
        orphan = response.json()
        assert orphan["status"] == "processing"
        assert orphan["result"] is None

        with worker_process(fast, tmp_path / "w2.log") as second:
            body = await wait_for_status(
                client, key, "completed", worker=second
            )
            assert body["attempts"] == 2
            assert body["result"]["attempt"] == 2
            assert body["result"]["processed_by"] == second.worker_id
            assert body["last_error"] is None
            assert await second.terminate() == 0, second.diagnostics()
            # Taken over through lease recovery, not a fresh claim.
            assert "lease_recovered" in second.log_text()
