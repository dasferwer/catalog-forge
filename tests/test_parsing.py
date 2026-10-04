import io
import json
from decimal import Decimal

import pytest

from catalogforge.parsing import MAX_RECORD_BYTES, SourceError, rows


def job(format="json", processed=0, checkpoint=0):
    return {
        "format": format,
        "processed_rows": processed,
        "checkpoint_bytes": checkpoint,
        "delimiter": ",",
        "column_map": {key: key for key in ("sku", "name", "price", "stock")},
    }


class TrackedSource(io.BytesIO):
    def read(self, size=-1):
        assert 0 <= size <= 65536, "unbounded read"
        return super().read(size)


@pytest.mark.parametrize(
    "extra", ["x" * (MAX_RECORD_BYTES * 8), ["x" * MAX_RECORD_BYTES] * 8], ids=["string", "nested"]
)
def test_json_rejects_unused_oversized_field_before_reading_whole_record(extra):
    record = {"sku": "A", "name": "Name", "price": "1.25", "stock": 1, "unused": extra}
    source = TrackedSource(json.dumps([record]).encode())
    with pytest.raises(SourceError, match="record_exceeds_128_kib"):
        list(rows(source, job()))
    assert source.tell() <= MAX_RECORD_BYTES + 4096


@pytest.mark.parametrize("format", ["csv", "jsonl", "json"])
def test_all_formats_reject_oversized_logical_record(format):
    padding = "x" * MAX_RECORD_BYTES
    if format == "csv":
        payload = f'sku,name,price,stock,unused\nA,Name,1.25,1,"{padding}"\n'.encode()
    else:
        record = {"sku": "A", "name": "Name", "price": "1.25", "stock": 1, "unused": padding}
        payload = json.dumps(record).encode()
        payload = b"[" + payload + b"]" if format == "json" else payload + b"\n"
    with pytest.raises(SourceError):
        list(rows(io.BytesIO(payload), job(format)))


def test_csv_limits_multiline_record_before_materializing_all_fields():
    line = "x" * 1024 + "\n"
    payload = ('sku,name,price,stock,unused\nA,Name,1,1,"' + line * 1024 + '"\n').encode()
    source = io.BytesIO(payload)
    with pytest.raises(SourceError, match="record_exceeds_128_kib"):
        list(rows(source, job("csv")))
    assert source.tell() <= MAX_RECORD_BYTES + 4096


@pytest.mark.parametrize("size", [MAX_RECORD_BYTES - 1, MAX_RECORD_BYTES])
def test_json_accepts_record_at_limit(size):
    payload = b'{"unused":"' + b"x" * (size - len(b'{"unused":""}')) + b'"}'
    result = list(rows(io.BytesIO(b"[" + payload + b"]"), job()))
    assert result[0].value == {"unused": "x" * (size - len(b'{"unused":""}'))}


def test_json_stream_preserves_escaping_nested_values_and_decimal_precision():
    payload = (
        '[{"name":"юникод \\" , ] } \\\\", "nested":[{},[1,2]],"price":1.25},false,null]'.encode()
    )
    result = list(rows(io.BytesIO(payload), job()))
    assert [row.number for row in result] == [1, 2, 3]
    assert result[0].value["price"] == Decimal("1.25")
    assert result[0].value["nested"] == [{}, [1, 2]]
    assert result[1].value is False and result[2].value is None


@pytest.mark.parametrize(
    "payload",
    [b"{}", b"[", b"[1,]", b"[,1]", b"[1]x", b"[1 2]", b"[NaN]", b'["unterminated]', b'[{"a":1]'],
)
def test_json_rejects_malformed_array(payload):
    with pytest.raises(SourceError):
        list(rows(io.BytesIO(payload), job()))


def test_json_resume_skips_committed_rows_and_keeps_record_numbers():
    result = list(rows(io.BytesIO(b"[1,2,3,4]"), job(processed=2)))
    assert [(row.number, row.offset, row.value) for row in result] == [(3, 0, 3), (4, 0, 4)]


def test_json_iterator_reads_only_bounded_lookahead_before_first_record():
    source = TrackedSource(b"[1," + b"2," * 1_000_000 + b"3]")
    iterator = rows(source, job())
    assert next(iterator).value == 1
    assert source.tell() <= 4096
    iterator.close()


@pytest.mark.parametrize("name", ["before\x00after", "\x00", "ok\x00"])
def test_nul_name_is_invalid_before_copy(name):
    from catalogforge.parsing import normalize

    value = {"sku": "A", "name": name, "price": "1.25", "stock": 1}
    assert normalize(value, job()["column_map"]) == (None, "invalid_name")


def test_initial_bom_is_removed_but_multiline_field_bom_is_preserved():
    payload = '\ufeffsku,name,price,stock\r\nA,"first\r\n\ufeffsecond",1.25,1\r\n'.encode()
    result = list(rows(io.BytesIO(payload), job("csv")))
    assert result[0].value["name"] == "first\r\n\ufeffsecond"
    assert result[0].offset == len(payload)


def test_csv_resume_preserves_bom_at_nonzero_source_offset():
    first = b"\xef\xbb\xbfsku,name,price,stock\nA,First,1.25,1\n"
    second = "\ufeffB,Second,2.50,1\n".encode()
    result = list(rows(io.BytesIO(first + second), job("csv", 1, len(first))))
    assert result[0].number == 2
    assert result[0].value["sku"] == "\ufeffB"
    assert result[0].offset == len(first + second)


def test_jsonl_bom_only_belongs_to_absolute_source_start():
    payload = '\ufeff{"sku":"A"}\n\ufeff{"sku":"B"}\n'.encode()
    result = list(rows(io.BytesIO(payload), job("jsonl")))
    assert result[0].value == {"sku": "A"}
    assert result[1].error == "invalid_json_record"
