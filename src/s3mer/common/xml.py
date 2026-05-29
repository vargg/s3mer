"""S3 XML response builders for successful operations."""

import xml.etree.ElementTree as ET
from datetime import UTC, datetime


def list_buckets_xml(buckets: list[dict]) -> str:
    """
    Build ListBuckets XML response.

    Each bucket dict should have 'Name' and 'CreationDate' keys.
    """
    bucket_entries = []
    for b in buckets:
        name = b["Name"]
        creation_date = b.get("CreationDate", datetime.now(tz=UTC).isoformat())
        bucket_entries.append(f"    <Bucket><Name>{name}</Name><CreationDate>{creation_date}</CreationDate></Bucket>")

    buckets_xml = "\n".join(bucket_entries)

    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<ListAllMyBucketsResult>\n"
        "  <Buckets>\n"
        f"{buckets_xml}\n"
        "  </Buckets>\n"
        "</ListAllMyBucketsResult>"
    )


def create_bucket_xml(location: str = "us-east-1") -> str:
    """Build CreateBucket success XML response (Location element)."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<CreateBucketConfiguration>\n"
        f"  <LocationConstraint>{location}</LocationConstraint>\n"
        f"</CreateBucketConfiguration>"
    )


def delete_result_xml(deleted_keys: list[str], errors: list[dict] | None = None) -> str:
    """Build DeleteObjects result XML."""
    parts = ['<?xml version="1.0" encoding="UTF-8"?>', "<DeleteResult>"]

    parts.extend(f"  <Deleted><Key>{key}</Key></Deleted>" for key in deleted_keys)

    if errors:
        for err in errors:
            parts.extend(
                [
                    "  <Error>",
                    f"    <Key>{err['Key']}</Key>",
                    f"    <Code>{err['Code']}</Code>",
                    f"    <Message>{err['Message']}</Message>",
                    "  </Error>",
                ],
            )

    parts.append("</DeleteResult>")
    return "\n".join(parts)


def get_object_tagging_xml(response: dict) -> str:
    """Build GetObjectTagging XML response."""
    parts = ['<?xml version="1.0" encoding="UTF-8"?>', "<Tagging>", "  <TagSet>"]

    tag_set = response.get("TagSet", [])
    for tag in tag_set:
        parts.extend(
            [
                "    <Tag>",
                f"      <Key>{tag.get('Key', '')}</Key>",
                f"      <Value>{tag.get('Value', '')}</Value>",
                "    </Tag>",
            ]
        )

    parts.extend(["  </TagSet>", "</Tagging>"])
    return "\n".join(parts)


def list_objects_xml(bucket: str, response: dict) -> str:
    """Build ListObjects (V1) XML response."""
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"  <Name>{bucket}</Name>",
    ]

    for key in ["Prefix", "Marker", "MaxKeys", "IsTruncated", "NextMarker"]:
        if key in response:
            val = str(response[key]).lower() if isinstance(response[key], bool) else str(response[key])
            parts.append(f"  <{key}>{val}</{key}>")

    for obj in response.get("Contents", []):
        parts.append("  <Contents>")
        parts.append(f"    <Key>{obj['Key']}</Key>")
        if "LastModified" in obj:
            lm = obj["LastModified"]
            lm_str = lm.isoformat() if hasattr(lm, "isoformat") else str(lm)
            parts.append(f"    <LastModified>{lm_str}</LastModified>")
        if "ETag" in obj:
            parts.append(f"    <ETag>{obj['ETag']}</ETag>")
        if "Size" in obj:
            parts.append(f"    <Size>{obj['Size']}</Size>")
        if "StorageClass" in obj:
            parts.append(f"    <StorageClass>{obj['StorageClass']}</StorageClass>")
        parts.append("  </Contents>")

    parts.append("</ListBucketResult>")
    return "\n".join(parts)


def list_objects_v2_xml(bucket: str, response: dict) -> str:
    """Build ListObjectsV2 XML response."""
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        f"  <Name>{bucket}</Name>",
    ]

    for key in ["Prefix", "KeyCount", "MaxKeys", "IsTruncated", "ContinuationToken", "NextContinuationToken"]:
        if key in response:
            val = str(response[key]).lower() if isinstance(response[key], bool) else str(response[key])
            parts.append(f"  <{key}>{val}</{key}>")

    for obj in response.get("Contents", []):
        parts.append("  <Contents>")
        parts.append(f"    <Key>{obj['Key']}</Key>")
        if "LastModified" in obj:
            lm = obj["LastModified"]
            lm_str = lm.isoformat() if hasattr(lm, "isoformat") else str(lm)
            parts.append(f"    <LastModified>{lm_str}</LastModified>")
        if "ETag" in obj:
            parts.append(f"    <ETag>{obj['ETag']}</ETag>")
        if "Size" in obj:
            parts.append(f"    <Size>{obj['Size']}</Size>")
        if "StorageClass" in obj:
            parts.append(f"    <StorageClass>{obj['StorageClass']}</StorageClass>")
        parts.append("  </Contents>")

    parts.append("</ListBucketResult>")
    return "\n".join(parts)


def create_multipart_upload_xml(bucket: str, key: str, upload_id: str) -> str:
    """Build InitiateMultipartUploadResult XML."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
        f"  <Bucket>{bucket}</Bucket>\n"
        f"  <Key>{key}</Key>\n"
        f"  <UploadId>{upload_id}</UploadId>\n"
        "</InitiateMultipartUploadResult>"
    )


def complete_multipart_upload_xml(bucket: str, key: str, etag: str, location: str = "") -> str:
    """Build CompleteMultipartUploadResult XML."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
        f"  <Location>{location}</Location>\n"
        f"  <Bucket>{bucket}</Bucket>\n"
        f"  <Key>{key}</Key>\n"
        f"  <ETag>{etag}</ETag>\n"
        "</CompleteMultipartUploadResult>"
    )


def copy_object_result_xml(result: dict) -> str:
    """Build CopyObjectResult XML."""
    etag = result.get("ETag", "")
    last_modified = result.get("LastModified", "")
    lm_str = last_modified.isoformat() if hasattr(last_modified, "isoformat") else str(last_modified)

    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<CopyObjectResult>\n"
        f"  <LastModified>{lm_str}</LastModified>\n"
        f"  <ETag>{etag}</ETag>\n"
        "</CopyObjectResult>"
    )


def get_bucket_lifecycle_xml(  # noqa: PLR0912, PLR0915 - Standard complex mapping function
    response: dict,
) -> str:
    """Build GetBucketLifecycleConfiguration XML response."""
    parts = ['<?xml version="1.0" encoding="UTF-8"?>', "<LifecycleConfiguration>"]

    for rule in response.get("Rules", []):
        parts.append("  <Rule>")
        if "ID" in rule:
            parts.append(f"    <ID>{rule['ID']}</ID>")
        if "Prefix" in rule:
            parts.append(f"    <Prefix>{rule['Prefix']}</Prefix>")
        if "Status" in rule:
            parts.append(f"    <Status>{rule['Status']}</Status>")

        if "Filter" in rule:
            filt = rule["Filter"]
            parts.append("    <Filter>")
            if "Prefix" in filt:
                parts.append(f"      <Prefix>{filt['Prefix']}</Prefix>")
            elif "And" in filt:
                parts.append("      <And>")
                and_val = filt["And"]
                if "Prefix" in and_val:
                    parts.append(f"        <Prefix>{and_val['Prefix']}</Prefix>")
                parts.extend(
                    f"        <Tag><Key>{tag['Key']}</Key><Value>{tag['Value']}</Value></Tag>"
                    for tag in and_val.get("Tags", [])
                )
                parts.append("      </And>")
            elif "Tag" in filt:
                tag = filt["Tag"]
                parts.append(f"      <Tag><Key>{tag['Key']}</Key><Value>{tag['Value']}</Value></Tag>")
            parts.append("    </Filter>")

        if "Expiration" in rule:
            exp = rule["Expiration"]
            parts.append("    <Expiration>")
            if "Days" in exp:
                parts.append(f"      <Days>{exp['Days']}</Days>")
            if "Date" in exp:
                parts.append(f"      <Date>{exp['Date']}</Date>")
            if "ExpiredObjectDeleteMarker" in exp:
                marker_val = str(exp["ExpiredObjectDeleteMarker"]).lower()
                parts.append(f"      <ExpiredObjectDeleteMarker>{marker_val}</ExpiredObjectDeleteMarker>")
            parts.append("    </Expiration>")

        for trans in rule.get("Transitions", []):
            parts.append("    <Transition>")
            if "Days" in trans:
                parts.append(f"      <Days>{trans['Days']}</Days>")
            if "Date" in trans:
                parts.append(f"      <Date>{trans['Date']}</Date>")
            if "StorageClass" in trans:
                parts.append(f"      <StorageClass>{trans['StorageClass']}</StorageClass>")
            parts.append("    </Transition>")

        if "NoncurrentVersionExpiration" in rule:
            nve = rule["NoncurrentVersionExpiration"]
            parts.append("    <NoncurrentVersionExpiration>")
            if "NoncurrentDays" in nve:
                parts.append(f"      <NoncurrentDays>{nve['NoncurrentDays']}</NoncurrentDays>")
            parts.append("    </NoncurrentVersionExpiration>")

        for nvt in rule.get("NoncurrentVersionTransitions", []):
            parts.append("    <NoncurrentVersionTransition>")
            if "NoncurrentDays" in nvt:
                parts.append(f"      <NoncurrentDays>{nvt['NoncurrentDays']}</NoncurrentDays>")
            if "StorageClass" in nvt:
                parts.append(f"      <StorageClass>{nvt['StorageClass']}</StorageClass>")
            parts.append("    </NoncurrentVersionTransition>")

        if "AbortIncompleteMultipartUpload" in rule:
            aimu = rule["AbortIncompleteMultipartUpload"]
            parts.append("    <AbortIncompleteMultipartUpload>")
            if "DaysAfterInitiation" in aimu:
                parts.append(f"      <DaysAfterInitiation>{aimu['DaysAfterInitiation']}</DaysAfterInitiation>")
            parts.append("    </AbortIncompleteMultipartUpload>")

        parts.append("  </Rule>")

    parts.append("</LifecycleConfiguration>")
    return "\n".join(parts)


def parse_lifecycle_configuration_xml(  # noqa: PLR0912, PLR0915 - Standard complex XML-to-dict parser function
    body: bytes,
) -> dict:
    """Parse LifecycleConfiguration XML into a dictionary compatible with botocore."""
    root = ET.fromstring(body)
    rules = []

    def clean_tag(tag: str) -> str:
        return tag.rsplit("}", maxsplit=1)[-1] if "}" in tag else tag

    for rule_node in root:
        if clean_tag(rule_node.tag) != "Rule":
            continue

        rule: dict = {}
        transitions = []
        noncurrent_transitions = []

        for child in rule_node:
            tag = clean_tag(child.tag)

            if tag == "ID":
                rule["ID"] = child.text
            elif tag == "Prefix":
                rule["Prefix"] = child.text or ""
            elif tag == "Status":
                rule["Status"] = child.text
            elif tag == "Filter":
                filt: dict = {}
                for f_child in child:
                    f_tag = clean_tag(f_child.tag)
                    if f_tag == "Prefix":
                        filt["Prefix"] = f_child.text or ""
                    elif f_tag == "Tag":
                        tag_key = ""
                        tag_val = ""
                        for t_child in f_child:
                            t_tag = clean_tag(t_child.tag)
                            if t_tag == "Key":
                                tag_key = t_child.text or ""
                            elif t_tag == "Value":
                                tag_val = t_child.text or ""
                        filt["Tag"] = {"Key": tag_key, "Value": tag_val}
                    elif f_tag == "And":
                        and_val: dict = {}
                        and_tags = []
                        for a_child in f_child:
                            a_tag = clean_tag(a_child.tag)
                            if a_tag == "Prefix":
                                and_val["Prefix"] = a_child.text or ""
                            elif a_tag == "Tag":
                                tag_key = ""
                                tag_val = ""
                                for t_child in a_child:
                                    t_tag = clean_tag(t_child.tag)
                                    if t_tag == "Key":
                                        tag_key = t_child.text or ""
                                    elif t_tag == "Value":
                                        tag_val = t_child.text or ""
                                and_tags.append({"Key": tag_key, "Value": tag_val})
                        if and_tags:
                            and_val["Tags"] = and_tags
                        filt["And"] = and_val
                rule["Filter"] = filt
            elif tag == "Expiration":
                exp: dict = {}
                for e_child in child:
                    e_tag = clean_tag(e_child.tag)
                    if e_tag == "Days":
                        exp["Days"] = int(e_child.text or 0)
                    elif e_tag == "Date":
                        exp["Date"] = e_child.text
                    elif e_tag == "ExpiredObjectDeleteMarker":
                        exp["ExpiredObjectDeleteMarker"] = (e_child.text or "").lower() == "true"
                rule["Expiration"] = exp
            elif tag == "Transition":
                trans: dict = {}
                for t_child in child:
                    t_tag = clean_tag(t_child.tag)
                    if t_tag == "Days":
                        trans["Days"] = int(t_child.text or 0)
                    elif t_tag == "Date":
                        trans["Date"] = t_child.text
                    elif t_tag == "StorageClass":
                        trans["StorageClass"] = t_child.text
                transitions.append(trans)
            elif tag == "NoncurrentVersionExpiration":
                nve: dict = {}
                for n_child in child:
                    n_tag = clean_tag(n_child.tag)
                    if n_tag == "NoncurrentDays":
                        nve["NoncurrentDays"] = int(n_child.text or 0)
                rule["NoncurrentVersionExpiration"] = nve
            elif tag == "NoncurrentVersionTransition":
                nvt: dict = {}
                for n_child in child:
                    n_tag = clean_tag(n_child.tag)
                    if n_tag == "NoncurrentDays":
                        nvt["NoncurrentDays"] = int(n_child.text or 0)
                    elif n_tag == "StorageClass":
                        nvt["StorageClass"] = n_child.text
                noncurrent_transitions.append(nvt)
            elif tag == "AbortIncompleteMultipartUpload":
                aimu: dict = {}
                for a_child in child:
                    a_tag = clean_tag(a_child.tag)
                    if a_tag == "DaysAfterInitiation":
                        aimu["DaysAfterInitiation"] = int(a_child.text or 0)
                rule["AbortIncompleteMultipartUpload"] = aimu

        if transitions:
            rule["Transitions"] = transitions
        if noncurrent_transitions:
            rule["NoncurrentVersionTransitions"] = noncurrent_transitions

        rules.append(rule)

    return {"Rules": rules}
