#!/usr/bin/env python3
"""Consumer đọc topic `ai-events` và ghi xuống bảng stream — NHIỆM VỤ 5.

Chạy tay:
    python ingest/consumer.py --db data/crash/crash.duckdb \
        --topic data/crash/topic.jsonl --offset data/crash/offsets.json

Kịch bản sự cố (tools/crash_test.py tự lo):
    thêm --crash-at-batch 7  -> tiến trình tự chết ở lô thứ 7, y hệt kill -9.

KHUNG THỰC HIỆN — NHIỆM VỤ 5

  Chạy `make crash-test` trước. Đọc kết quả: bạn MẤT bản ghi hay bạn có bản
  ghi TRÙNG? Con số đó xác định consumer đang ở ngữ nghĩa nào.

      at-most-once   : commit offset TRƯỚC khi ghi  -> crash = mất dữ liệu
      at-least-once  : commit offset SAU khi ghi    -> crash = trùng dữ liệu
      exactly-once   : không tồn tại ở tầng giao vận

  Hai hạng mục cần xử lý, thiếu một là chưa đủ:

    (a) Thứ tự thao tác trong consume() — xem khối được đánh dấu bên dưới.
        Đổi thứ tự chuyển ngữ nghĩa từ nhóm này sang nhóm kia. Câu hỏi: nếu
        tiến trình chết ở điểm maybe_crash(), lô hiện tại đã được ghi chưa,
        offset đã dịch chưa, và lần khởi động lại sẽ đọc từ đâu?

    (b) Tính idempotent của write_batch() — đổi thứ tự ở (a) khiến một số lô
        được phát lại. Với câu lệnh INSERT hiện tại, phát lại nghĩa là gì?

            INSERT INTO <bảng> VALUES (...)
            ON CONFLICT (<cột khoá>) DO <UPDATE ... | NOTHING>

        DuckDB chỉ chấp nhận mệnh đề ON CONFLICT khi cột khoá có ràng buộc
        PRIMARY KEY hoặc UNIQUE — xem hằng DDL ngay bên dưới.

        Câu hỏi cho báo cáo: DO UPDATE và DO NOTHING khác nhau ở đâu khi một
        message được phát lại với nội dung ĐÃ ĐỔI? Bạn chọn cái nào, vì sao?
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from ingest.log_client import LogConsumer  # noqa: E402

TABLE = "bronze_events_stream"

DDL = f"""
create table if not exists {TABLE} (
    -- primary key là điều kiện để DuckDB chấp nhận mệnh đề ON CONFLICT bên
    -- dưới. Không có nó, write_batch() không thể idempotent, và at-least-once
    -- sẽ biến mọi lần phát lại thành một lần nhân bản.
    event_id      varchar primary key,
    ticket_id     varchar,
    customer_id   varchar,
    customer_name varchar,
    event_type    varchar,
    latency_ms    integer,
    event_time    timestamp,
    _ingested_at  timestamp
);
"""


def write_batch(con: duckdb.DuckDBPyConnection, batch: list[dict]) -> None:
    """Ghi một lô message xuống kho — nhiệm vụ 5, hạng mục (b).

    Câu lệnh hiện tại là INSERT thuần: ghi lại cùng một event_id sẽ tạo thêm
    một hàng mới. Xem khung mã giả ở đầu file.
    """
    # Chọn DO UPDATE thay vì DO NOTHING: nếu một message được phát lại với nội
    # dung ĐÃ ĐỔI (bản sửa của cùng event_id), DO NOTHING giữ lại bản cũ đã
    # lỗi thời, còn DO UPDATE hội tụ về bản mới nhất. Cả hai đều chống trùng
    # hàng; chỉ DO UPDATE mới cho kết quả cuối không phụ thuộc thứ tự phát lại.
    #
    # Ghi cả lô bằng MỘT câu lệnh thay vì executemany: với ON CONFLICT,
    # executemany chạy từng dòng một và phải cập nhật index sau mỗi dòng —
    # 20.000 message mất ~39 giây. Gộp lại còn một câu lệnh cho mỗi lô 500
    # message thì nhanh hơn hai bậc độ lớn, và ngữ nghĩa không đổi.
    if not batch:
        return
    values = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?)"] * len(batch))
    params: list = []
    for r in batch:
        params += [
            r["event_id"], r["ticket_id"], r["customer_id"], r["customer_name"],
            r["event_type"], r["latency_ms"], r["event_time"], r["_ingested_at"],
        ]
    con.execute(
        f"""insert into {TABLE} values {values}
            on conflict (event_id) do update set
                ticket_id     = excluded.ticket_id,
                customer_id   = excluded.customer_id,
                customer_name = excluded.customer_name,
                event_type    = excluded.event_type,
                latency_ms    = excluded.latency_ms,
                event_time    = excluded.event_time,
                _ingested_at  = excluded._ingested_at""",
        params,
    )


def maybe_crash(batch_no: int, crash_at: int | None) -> None:
    """Mô phỏng `kill -9`: chết ngay, không rollback, không flush."""
    if crash_at is not None and batch_no == crash_at:
        print(f"  [consumer] 💥 tiến trình bị giết ở lô {batch_no}", flush=True)
        os._exit(137)


def consume(
    db: str,
    topic: str,
    offset_file: str,
    batch_size: int = 500,
    crash_at: int | None = None,
) -> int:
    con = duckdb.connect(db)
    con.execute(DDL)

    written = 0
    with LogConsumer(topic, offset_file) as consumer:
        batch_no = 0
        while True:
            batch = consumer.poll(batch_size)
            if not batch:
                break
            batch_no += 1

            # ── KHỐI CẦN XEM XÉT — nhiệm vụ 5, hạng mục (a) ───────────────
            # Ba dòng dưới đây được phép sắp xếp lại. maybe_crash() mô phỏng
            # `kill -9`: tiến trình chết ngay tại vị trí của nó, không rollback.
            # Ghi dữ liệu TRƯỚC, commit offset SAU -> at-least-once: chết ở
            # giữa nghĩa là offset chưa dịch, lô đó được đọc lại khi khởi động
            # lại. Không mất bản ghi; phần "đọc lại" do write_batch() idempotent
            # gánh, nên đọc lại không sinh ra bản trùng.
            write_batch(con, batch)           # ghi dữ liệu
            maybe_crash(batch_no, crash_at)   # sự cố xảy ra tại đây
            consumer.commit()                 # ghi nhận offset
            # ─────────────────────────────────────────────────────────────

            written += len(batch)

    con.close()
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--offset", required=True)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--crash-at-batch", type=int, default=None)
    a = ap.parse_args()
    n = consume(a.db, a.topic, a.offset, a.batch_size, a.crash_at_batch)
    print(f"  [consumer] đã ghi {n:,} message")
    return 0


if __name__ == "__main__":
    sys.exit(main())
