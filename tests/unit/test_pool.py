import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import SecretStr

from s3mer.backends.client import S3BackendClient
from s3mer.backends.pool import BackendPool
from s3mer.backends.prober import LatencyProber
from s3mer.common.metrics import NullMetricsTracker
from s3mer.config.settings import BackendConfig
from s3mer.routing.operations import S3Operation


@pytest.fixture
def configs() -> dict[str, BackendConfig]:
    return {
        "primary": BackendConfig(
            endpoint_url="http://localhost:9000",
            access_key="key",
            secret_key=SecretStr("secret"),
            is_primary=True,
            priority=0,
        ),
        "secondary_slow": BackendConfig(
            endpoint_url="http://localhost:9002",
            access_key="key",
            secret_key=SecretStr("secret"),
            is_primary=False,
            priority=2,
        ),
        "secondary_fast": BackendConfig(
            endpoint_url="http://localhost:9003",
            access_key="key",
            secret_key=SecretStr("secret"),
            is_primary=False,
            priority=1,
        ),
    }


@pytest.mark.asyncio
async def test_latency_prober_measuring(configs: dict[str, BackendConfig]) -> None:
    # Set probe interval extremely short for testing
    pool = BackendPool(configs, NullMetricsTracker(), probe_interval=0.01)

    # Patch S3BackendClient methods at class level to avoid static type-checker re-assignment issues
    with (
        patch.object(S3BackendClient, "start", new_callable=AsyncMock) as mock_start,
        patch.object(S3BackendClient, "close", new_callable=AsyncMock) as mock_close,
        patch.object(S3BackendClient, "execute", new_callable=AsyncMock) as mock_execute,
    ):
        mock_execute.return_value = {"Buckets": []}

        # Verify LatencyProber is instantiated
        assert isinstance(pool._prober, LatencyProber)
        for client in pool._clients.values():
            assert client.last_latency == 0.0

        # Start the pool, initiating the prober
        await pool.start()

        assert pool._prober._task is not None
        assert not pool._prober._task.done()

        # Sleep a bit to allow the prober loop to run at least one tick
        await asyncio.sleep(1.2)

        # Latencies should be recorded and positive/non-zero
        for client in pool._clients.values():
            assert client.last_latency > 0.0
            assert client.last_latency != float("inf")

        # Ensure S3Operation.LIST_BUCKETS was called
        mock_execute.assert_any_call(S3Operation.LIST_BUCKETS, {})

        # Close the pool
        await pool.close()
        assert pool._prober._task is None

        assert mock_start.call_count == len(configs)
        assert mock_close.call_count == len(configs)


@pytest.mark.asyncio
async def test_latency_prober_error_handling(configs: dict[str, BackendConfig]) -> None:
    pool = BackendPool(configs, NullMetricsTracker(), probe_interval=0.01)

    # Patch S3BackendClient methods at class level to raise exceptions safely
    with (
        patch.object(S3BackendClient, "start", new_callable=AsyncMock),
        patch.object(S3BackendClient, "close", new_callable=AsyncMock),
        patch.object(S3BackendClient, "execute", new_callable=AsyncMock) as mock_execute,
    ):
        mock_execute.side_effect = Exception("S3 is down")

        # Start the pool
        await pool.start()

        # Sleep to allow the first probe to execute
        await asyncio.sleep(1.2)

        # Failed backends must register float("inf") latency rather than crashing the loop
        for client in pool._clients.values():
            assert client.last_latency == float("inf")

        # Close the pool cleanly
        await pool.close()
        assert pool._prober._task is None


@pytest.mark.asyncio
async def test_backend_pool_all_by_latency(configs: dict[str, BackendConfig]) -> None:
    pool = BackendPool(configs, NullMetricsTracker())

    primary = pool.primary
    secondary_slow = pool.get("secondary_slow")
    secondary_fast = pool.get("secondary_fast")

    # 1. Initially, all latencies are 0.0.
    # The tie-breaker (priority) should sort:
    # Primary (guaranteed first) -> secondary_fast (priority 1) -> secondary_slow (priority 2)
    assert pool.all_by_latency() == [primary, secondary_fast, secondary_slow]

    # 2. Update latencies so that secondary_slow has lower latency than secondary_fast,
    # but both are slower than primary.
    primary.last_latency = 0.5
    secondary_slow.last_latency = 1.0
    secondary_fast.last_latency = 5.0
    # Primary is still FIRST (due to read-after-write consistency),
    # followed by secondary_slow (1.0s) and secondary_fast (5.0s)
    assert pool.all_by_latency() == [primary, secondary_slow, secondary_fast]

    # 3. Update latencies so that secondary_fast has much lower latency than primary.
    secondary_fast.last_latency = 0.1
    primary.last_latency = 2.0
    # Primary MUST still be first!
    assert pool.all_by_latency() == [primary, secondary_fast, secondary_slow]
