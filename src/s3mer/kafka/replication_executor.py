"""S3 operation replication from source to target backend."""

from botocore.exceptions import ClientError

from s3mer.backends.types import BackendClient
from s3mer.common.logging import get_logger
from s3mer.common.metrics import MetricsTracker, NullMetricsTracker
from s3mer.kafka.manager import REPLICATED_OBJECT_METADATA
from s3mer.kafka.messages import ReplicationMessage
from s3mer.kafka.subscribers_config import ReplicationRetryConfig
from s3mer.routing.operations import S3Operation

logger = get_logger(__name__)


def backend_name(client: BackendClient) -> str:
    return getattr(client, "name", "unknown")


def apply_object_metadata(put_params: dict, metadata: dict) -> None:
    """Map replication metadata keys to boto3 PutObject kwargs."""
    for key in REPLICATED_OBJECT_METADATA:
        if key not in metadata:
            continue
        value = metadata[key]
        if key == "Metadata" and isinstance(value, dict):
            put_params["Metadata"] = value
        elif key == "Expires" and value is not None:
            put_params["Expires"] = value
        else:
            put_params[key] = value


async def _head_etag(client: BackendClient, bucket: str, key: str | None) -> str | None:
    if key is None:
        return None
    try:
        head = await client.execute(S3Operation.HEAD_OBJECT, {"Bucket": bucket, "Key": key})
    except ClientError:
        return None
    else:
        return head.get("ETag")


async def _replicate_put_object(
    message: ReplicationMessage,
    source: BackendClient,
    target: BackendClient,
    tracker: MetricsTracker,
    target_name: str,
) -> None:
    if ReplicationRetryConfig.skip_if_etag_matches and message.key:
        source_etag = await _head_etag(source, message.bucket, message.key)
        target_etag = await _head_etag(target, message.bucket, message.key)
        if source_etag and target_etag and source_etag == target_etag:
            tracker.record_replication_consumer_outcome(message.operation, target_name, "skipped_already_synced")
            logger.info(
                "Target ETag matches source, skipping PUT replication",
                bucket=message.bucket,
                key=message.key,
                message_id=message.message_id,
                target=target_name,
                etag=source_etag,
            )
            return

    try:
        get_response = await source.execute(
            S3Operation.GET_OBJECT,
            {"Bucket": message.bucket, "Key": message.key},
        )
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("NoSuchKey", "NoSuchBucket"):
            tracker.record_replication_consumer_outcome(message.operation, target_name, "skipped_source_gone")
            logger.warning(
                "Source object or bucket no longer exists, skipping replication",
                bucket=message.bucket,
                key=message.key,
                message_id=message.message_id,
                target=target_name,
                error=str(e),
            )
            return
        raise

    body = get_response["Body"]
    put_params: dict = {
        "Bucket": message.bucket,
        "Key": message.key,
        "Body": body,
    }
    apply_object_metadata(put_params, message.metadata)

    content_length = message.metadata.get("ContentLength")
    if content_length is None:
        try:
            head_resp = await source.execute(
                S3Operation.HEAD_OBJECT,
                {"Bucket": message.bucket, "Key": message.key},
            )
            content_length = head_resp["ContentLength"]
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code in ("NoSuchKey", "NoSuchBucket"):
                tracker.record_replication_consumer_outcome(message.operation, target_name, "skipped_source_gone")
                logger.warning(
                    "Source object or bucket no longer exists on HEAD, skipping replication",
                    bucket=message.bucket,
                    key=message.key,
                    message_id=message.message_id,
                    target=target_name,
                    error=str(e),
                )
                return
            raise

    put_params["ContentLength"] = int(content_length)

    async with body:
        await target.execute(S3Operation.PUT_OBJECT, put_params)


async def replicate_operation(  # noqa: PLR0912, PLR0915
    operation: S3Operation,
    message: ReplicationMessage,
    source: BackendClient,
    target: BackendClient,
    metrics: MetricsTracker | None = None,
) -> None:
    """Replicate a single S3 operation from source to target backend."""
    tracker = metrics or NullMetricsTracker()
    target_name = backend_name(target)

    match operation:
        case S3Operation.PUT_OBJECT:
            await _replicate_put_object(message, source, target, tracker, target_name)

        case S3Operation.CREATE_BUCKET:
            try:
                await target.execute(S3Operation.CREATE_BUCKET, {"Bucket": message.bucket})
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                    tracker.record_replication_consumer_outcome(
                        message.operation, target_name, "skipped_already_present"
                    )
                    logger.info(
                        "Bucket already exists on target, treating create as success",
                        bucket=message.bucket,
                        message_id=message.message_id,
                        target=target_name,
                    )
                    return
                raise

        case S3Operation.DELETE_BUCKET:
            try:
                await target.execute(S3Operation.DELETE_BUCKET, {"Bucket": message.bucket})
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "NoSuchBucket":
                    tracker.record_replication_consumer_outcome(
                        message.operation, target_name, "skipped_already_absent"
                    )
                    logger.info(
                        "Bucket already absent on target, treating delete as success",
                        bucket=message.bucket,
                        message_id=message.message_id,
                        target=target_name,
                    )
                    return
                raise

        case S3Operation.DELETE_OBJECT:
            try:
                await target.execute(
                    S3Operation.DELETE_OBJECT,
                    {"Bucket": message.bucket, "Key": message.key},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "NoSuchKey":
                    tracker.record_replication_consumer_outcome(
                        message.operation, target_name, "skipped_already_absent"
                    )
                    logger.info(
                        "Object already absent on target, treating delete as success",
                        bucket=message.bucket,
                        key=message.key,
                        message_id=message.message_id,
                        target=target_name,
                    )
                    return
                raise

        case S3Operation.PUT_OBJECT_TAGGING:
            try:
                tag_response = await source.execute(
                    S3Operation.GET_OBJECT_TAGGING,
                    {"Bucket": message.bucket, "Key": message.key},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchKey", "NoSuchBucket"):
                    tracker.record_replication_consumer_outcome(message.operation, target_name, "skipped_source_gone")
                    logger.warning(
                        "Source object or bucket no longer exists for tagging, skipping replication",
                        bucket=message.bucket,
                        key=message.key,
                        message_id=message.message_id,
                        target=target_name,
                        error=str(e),
                    )
                    return
                raise
            await target.execute(
                S3Operation.PUT_OBJECT_TAGGING,
                {
                    "Bucket": message.bucket,
                    "Key": message.key,
                    "Tagging": {"TagSet": tag_response.get("TagSet", [])},
                },
            )

        case S3Operation.DELETE_OBJECT_TAGGING:
            try:
                await target.execute(
                    S3Operation.DELETE_OBJECT_TAGGING,
                    {"Bucket": message.bucket, "Key": message.key},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code == "NoSuchKey":
                    tracker.record_replication_consumer_outcome(
                        message.operation, target_name, "skipped_already_absent"
                    )
                    logger.info(
                        "Object already absent on target, treating delete tagging as success",
                        bucket=message.bucket,
                        key=message.key,
                        message_id=message.message_id,
                        target=target_name,
                    )
                    return
                raise

        case S3Operation.PUT_BUCKET_LIFECYCLE:
            try:
                lifecycle_resp = await source.execute(
                    S3Operation.GET_BUCKET_LIFECYCLE,
                    {"Bucket": message.bucket},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchBucket", "NoSuchLifecycleConfiguration"):
                    tracker.record_replication_consumer_outcome(message.operation, target_name, "skipped_source_gone")
                    logger.warning(
                        "Source bucket or lifecycle configuration no longer exists, skipping replication",
                        bucket=message.bucket,
                        message_id=message.message_id,
                        target=target_name,
                        error=str(e),
                    )
                    return
                raise
            await target.execute(
                S3Operation.PUT_BUCKET_LIFECYCLE,
                {
                    "Bucket": message.bucket,
                    "LifecycleConfiguration": {"Rules": lifecycle_resp.get("Rules", [])},
                },
            )

        case S3Operation.DELETE_BUCKET_LIFECYCLE:
            try:
                await target.execute(
                    S3Operation.DELETE_BUCKET_LIFECYCLE,
                    {"Bucket": message.bucket},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchBucket", "NoSuchLifecycleConfiguration"):
                    tracker.record_replication_consumer_outcome(
                        message.operation, target_name, "skipped_already_absent"
                    )
                    logger.info(
                        "Lifecycle already absent on target, treating delete as success",
                        bucket=message.bucket,
                        message_id=message.message_id,
                        target=target_name,
                    )
                    return
                raise

        case S3Operation.PUT_BUCKET_POLICY:
            try:
                policy_resp = await source.execute(
                    S3Operation.GET_BUCKET_POLICY,
                    {"Bucket": message.bucket},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchBucket", "NoSuchBucketPolicy"):
                    tracker.record_replication_consumer_outcome(message.operation, target_name, "skipped_source_gone")
                    logger.warning(
                        "Source bucket or policy no longer exists, skipping replication",
                        bucket=message.bucket,
                        message_id=message.message_id,
                        target=target_name,
                        error=str(e),
                    )
                    return
                raise

            policy = policy_resp.get("Policy")
            if policy:
                await target.execute(
                    S3Operation.PUT_BUCKET_POLICY,
                    {"Bucket": message.bucket, "Policy": policy},
                )

        case S3Operation.DELETE_BUCKET_POLICY:
            try:
                await target.execute(
                    S3Operation.DELETE_BUCKET_POLICY,
                    {"Bucket": message.bucket},
                )
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("NoSuchBucket", "NoSuchBucketPolicy"):
                    tracker.record_replication_consumer_outcome(
                        message.operation, target_name, "skipped_already_absent"
                    )
                    logger.info(
                        "Policy already absent on target, treating delete as success",
                        bucket=message.bucket,
                        message_id=message.message_id,
                        target=target_name,
                    )
                    return
                raise

        case _:
            tracker.record_replication_consumer_outcome(operation.value, target_name, "skipped_unsupported")
            logger.warning(
                "Unsupported replication operation",
                operation=operation.value,
                message_id=message.message_id,
                target=target_name,
            )
