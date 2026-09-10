import csv
import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import ijson

MAX_RECORD_BYTES = 131072
csv.field_size_limit(MAX_RECORD_BYTES)


class SourceError(ValueError):
    pass


@dataclass(frozen=True)
class InputRow:
    number: int
    offset: int
    value: object = None
    error: str | None = None


class BoundedLines:
    def __init__(self, source):
        self.source = source

    def __iter__(self):
        return self

    def __next__(self):
        line = self.source.readline(MAX_RECORD_BYTES + 1)
        if not line:
            raise StopIteration
        if len(line) > MAX_RECORD_BYTES:
            raise SourceError("physical_line_exceeds_128_kib")
        try:
            return line.decode("utf-8").removeprefix("\ufeff")
        except UnicodeDecodeError:
            raise SourceError("source_must_be_utf8") from None


def rows(source, job):
    processed = job["processed_rows"]
    checkpoint = job["checkpoint_bytes"]
    try:
        if job["format"] == "csv":
            reader = csv.reader(BoundedLines(source), delimiter=job["delimiter"], strict=True)
            header = next(reader, None)
            if not header or len(header) > 50 or len(header) != len(set(header)):
                raise SourceError("csv_header_missing_duplicate_or_too_wide")
            if not set(job["column_map"].values()) <= set(header):
                raise SourceError("csv_required_columns_missing")
            if checkpoint:
                source.seek(checkpoint)
                reader = csv.reader(BoundedLines(source), delimiter=job["delimiter"], strict=True)
            start = source.tell()
            for number, values in enumerate(reader, start=processed + 1):
                end = source.tell()
                if end - start > MAX_RECORD_BYTES:
                    raise SourceError("record_exceeds_128_kib")
                yield InputRow(
                    number,
                    end,
                    dict(zip(header, values, strict=True)) if len(values) == len(header) else None,
                    None if len(values) == len(header) else "column_count_mismatch",
                )
                start = end
        elif job["format"] == "jsonl":
            source.seek(checkpoint)
            for number, line in enumerate(BoundedLines(source), start=processed + 1):
                try:
                    value = json.loads(
                        line,
                        parse_float=Decimal,
                        parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
                    )
                    yield InputRow(number, source.tell(), value)
                except (ValueError, RecursionError):
                    yield InputRow(number, source.tell(), error="invalid_json_record")
        else:
            first = b" "
            while first and first.isspace():
                first = source.read(1)
            if first != b"[":
                raise SourceError("json_root_must_be_array")
            source.seek(0)
            for number, value in enumerate(ijson.items(source, "item"), start=1):
                if number > processed:
                    yield InputRow(number, 0, value)
    except (csv.Error, ijson.JSONError, UnicodeDecodeError):
        raise SourceError("malformed_source") from None


def normalize(raw, mapping):
    if not isinstance(raw, dict):
        return None, "record_must_be_object"
    try:
        sku = raw[mapping["sku"]]
        name = raw[mapping["name"]]
        price = raw[mapping["price"]]
        stock = raw[mapping["stock"]]
    except KeyError:
        return None, "required_field_missing"
    if not isinstance(sku, str) or not re.fullmatch(
        r"[A-Z0-9][A-Z0-9_.-]{0,79}", sku.strip().upper()
    ):
        return None, "invalid_sku"
    sku = sku.strip().upper()
    if not isinstance(name, str) or not 1 <= len(" ".join(name.split())) <= 200:
        return None, "invalid_name"
    name = " ".join(name.split())
    try:
        if isinstance(price, bool):
            raise ValueError
        price = Decimal(str(price).strip())
        if (
            not price.is_finite()
            or not 0 <= price <= Decimal("999999999.99")
            or price.quantize(Decimal("0.01")) != price
        ):
            raise ValueError
        price = price.quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None, "invalid_price"
    if isinstance(stock, bool) or not re.fullmatch(r"[0-9]{1,10}", str(stock).strip()):
        return None, "invalid_stock"
    stock = int(str(stock).strip())
    if stock > 1_000_000_000:
        return None, "invalid_stock"
    try:
        fingerprint = hashlib.sha256(
            json.dumps(
                [sku, name, str(price), stock], ensure_ascii=False, separators=(",", ":")
            ).encode()
        ).hexdigest()
    except UnicodeEncodeError:
        return None, "invalid_unicode"
    return {
        "sku": sku,
        "name": name,
        "price": price,
        "stock": stock,
        "fingerprint": fingerprint,
    }, None
