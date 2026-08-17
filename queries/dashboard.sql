-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- Ba tháng trước truy vấn này chạy 2 giây. Bây giờ 38 giây.
-- Không ai sửa dòng nào trong file này.
--
-- Bạn ĐƯỢC PHÉP viết lại truy vấn, miễn là kết quả trả về không đổi
-- (tools/explain.py kiểm tra điều đó bằng hash của kết quả).

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
-- Dataset mới do tools/compact.py sinh ra: partition theo event_date, các hàng
-- trong file xếp theo customer_name. hive_partitioning cho engine đọc giá trị
-- event_date ngay từ tên thư mục.
from read_parquet('data/gold_events_v2/**/*.parquet', hive_partitioning = 1)
where customer_name = 'ACME'
  -- Cột đứng một mình ở một vế (sargable): so sánh này khớp thẳng với tên thư
  -- mục partition. Bản cũ bọc cột trong strftime() nên engine không so được
  -- với đường dẫn lẫn min/max statistics, buộc phải mở cả 5.000 file.
  and event_date = '2026-08-09'
group by 1
