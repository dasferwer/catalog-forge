import csv
import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

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
    def __init__(self, source, *, bound_record=False):
        self.source = source
        self.bound_record = bound_record
        self.record_bytes = 0

    def finish_record(self):
        self.record_bytes = 0

    def __iter__(self):
        return self

    def __next__(self):
        line = self.source.readline(MAX_RECORD_BYTES + 1)
        if not line:
            raise StopIteration
        if len(line) > MAX_RECORD_BYTES:
            raise SourceError("physical_line_exceeds_128_kib")
        self.record_bytes += len(line)
        if self.bound_record and self.record_bytes > MAX_RECORD_BYTES:
            raise SourceError("record_exceeds_128_kib")
        try:
            return line.decode("utf-8").removeprefix("\ufeff")
        except UnicodeDecodeError:
            raise SourceError("source_must_be_utf8") from None


def reject_constant(_):
    raise ValueError("non_finite_json_number")


def json_array_values(source):
    """Ограничивает сырые байты элемента до декодирования и сборки объекта."""

    def bytes_stream():
        while chunk := source.read(4096):
            yield from chunk

    stream = bytes_stream()

    def next_nonspace():
        for byte in stream:
            if byte not in b" \t\r\n":
                return byte
        return None

    if next_nonspace() != ord("["):
        raise SourceError("json_root_must_be_array")
    byte = next_nonspace()
    if byte == ord("]"):
        if next_nonspace() is not None:
            raise SourceError("malformed_source")
        return
    while True:
        record = bytearray()
        depth = 0
        quoted = escaped = False
        while byte is not None:
            if not quoted and depth == 0 and byte in b",]":
                break
            record.append(byte)
            if len(record) > MAX_RECORD_BYTES:
                raise SourceError("record_exceeds_128_kib")
            if quoted:
                if escaped:
                    escaped = False
                elif byte == ord("\\"):
                    escaped = True
                elif byte == ord('"'):
                    quoted = False
            elif byte == ord('"'):
                quoted = True
            elif byte in b"[{":
                depth += 1
            elif byte in b"]}":
                depth -= 1
                if depth < 0:
                    raise SourceError("malformed_source")
            byte = next(stream, None)
        if byte is None or not record:
            raise SourceError("malformed_source")
        try:
            value = json.loads(record, parse_float=Decimal, parse_constant=reject_constant)
        except (ValueError, RecursionError):
            raise SourceError("malformed_source") from None
        yield value
        if byte == ord("]"):
            if next_nonspace() is not None:
                raise SourceError("malformed_source")
            return
        byte = next_nonspace()
        if byte is None or byte == ord("]"):
            raise SourceError("malformed_source")


def rows(source, job):
    processed = job["processed_rows"]
    checkpoint = job["checkpoint_bytes"]
    try:
        if job["format"] == "csv":
            lines = BoundedLines(source, bound_record=True)
            reader = csv.reader(lines, delimiter=job["delimiter"], strict=True)
            header = next(reader, None)
            lines.finish_record()
            if not header or len(header) > 50 or len(header) != len(set(header)):
                raise SourceError("csv_header_missing_duplicate_or_too_wide")
            if not set(job["column_map"].values()) <= set(header):
                raise SourceError("csv_required_columns_missing")
            if checkpoint:
                source.seek(checkpoint)
                lines = BoundedLines(source, bound_record=True)
                reader = csv.reader(lines, delimiter=job["delimiter"], strict=True)
            start = source.tell()
            for number, values in enumerate(reader, start=processed + 1):
                end = source.tell()
                if end - start > MAX_RECORD_BYTES:
                    raise SourceError("record_exceeds_128_kib")
                lines.finish_record()
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
                        parse_constant=reject_constant,
                    )
                    yield InputRow(number, source.tell(), value)
                except (ValueError, RecursionError):
                    yield InputRow(number, source.tell(), error="invalid_json_record")
        else:
            source.seek(0)
            for number, value in enumerate(json_array_values(source), start=1):
                if number > processed:
                    yield InputRow(number, 0, value)
    except (csv.Error, UnicodeDecodeError):
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
