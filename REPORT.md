# Báo cáo LAB 17 - Data Pipeline Engineering

**Họ tên:** Nguyễn Phú Cường  **Lớp:** E403  **Ngày:** 17/8/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy (sau khi sửa)</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 18.3s
  run 2/3 … 18.5s
  run 3/3 … 18.3s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

<details>
<summary>Trạng thái TRƯỚC khi sửa (để đối chiếu)</summary>

```
  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✗ FAIL            38,750      12,480   ✗ thừa 26,270 hàng
  gold_feature_daily    ✓ ok               8,645       9,100   ✗ thiếu 455 hàng
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                   0         312   ✗ thiếu 312 hàng

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     7c461563f4    d11657ff21    2b76a4f850   ✗
  gold_feature_daily    4eee63cd82    4eee63cd82    4eee63cd82   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    empty         empty         empty        ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 9/9 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✗ 6,606 hàng sai
  quarantine_tickets đúng số bản ghi lỗi      ✗ 0 / 312
  gold_training_set: 1 hàng / 1 ticket        ✗ 12,480 ticket bị lặp
  DAG: catchup / max_active_runs              ✗ True / None

  1/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** · hai bài mở rộng đều đạt.

Chạy thêm lượt thứ tư và thứ năm (`--runs 2 --no-reset`): checksum vẫn là
`8dd7c98653 / 3db448685c / 92d8e50131 / ebb89036fb` - không đổi so với ba lượt đầu.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` có 38.750 hàng thay vì 12.480; mỗi lượt chạy lại tăng thêm; checksum ba lượt khác nhau hoàn toàn (`7c461563f4 / d11657ff21 / 2b76a4f850`). Cả 12.480 ticket đều bị lặp, ticket lặp nhiều nhất 4 lần. Không có lỗi nào được ném ra. |
| **Nguyên nhân** | Model khai `materialized = 'incremental'` nhưng **không khai `unique_key`**. Không có khoá thì dbt không có gì để so khớp hàng cũ với hàng mới, nên nó sinh ra câu `INSERT INTO … SELECT` thuần - mỗi lượt chạy là một lần **ghi thêm**, không phải ghi đè. Vì thế phép ghi vào bảng đích tự nó **không idempotent**, và mọi cơ chế chạy lại ở tầng trên (retry của Airflow, Clear Task của người trực, replay 14 ngày của `run_pipeline`) đều biến thành cơ chế nhân bản. Nguồn CDC còn làm lỗi nặng thêm: có 1.310 bản ghi `op='u'`, nên một ticket tạo ngày D1 rồi sửa ngày D2 sẽ mang `_ingested_at` khác nhau ở hai lượt và **lọt qua mệnh đề lọc partition ở hai ngày khác nhau** - tức là ngay trong MỘT lượt chạy 14 ngày nó đã được ghi hai lần. Đây là lý do chiến lược "xoá partition ngày rồi ghi lại" không cứu được: hai bản sao nằm ở hai partition khác nhau. Grain của bảng là **entity** (1 hàng / 1 ticket), nên khoá phải là khoá tự nhiên `ticket_id`, không phải ngày. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()` - lần ghi sau thay thế lần ghi trước theo khoá. Giữ nguyên mệnh đề `WHERE run_date` (nó phục vụ backfill, không phải lỗi). `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | trước: 38.750 hàng · 12.480 ticket bị lặp · checksum 3 lượt khác nhau - sau: 12.480 hàng · 0 ticket lặp · checksum `8dd7c98653` giống hệt nhau ở cả 5 lượt |

> Hai tham số DAG **không phải** nguyên nhân. `catchup=False` chỉ ngăn Airflow tự
> schedule một loạt lần chạy bù, `max_active_runs=1` chỉ ngăn hai run ghi đồng thời
> vào cùng bảng - cả hai đều **giảm tần suất kích hoạt** lỗi chứ không sửa lỗi. Sửa DAG
> mà không sửa model thì `make verify` vẫn đỏ, vì bản thân phép ghi vẫn cộng dồn.

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` có 8.645 / 9.100 hàng (thiếu 455, đúng 5%). Bảng vẫn `ỔN ĐỊNH ✓` - chạy lại bao nhiêu lần cũng ra đúng con số sai đó. 455 cặp `(event_date, customer_id)` bị thiếu **trải đều ở các ngày 08-03 → 08-13**, không có cặp nào thiếu ở ngày mới nhất. |
| **P99 độ trễ đo được** | **2,7258 ngày** *(P50 = 0,1281 · P95 = 1,8137 · max = 2,9447 · tỷ lệ bản ghi tới muộn hơn 1 ngày = 5,05%)* |
| **Lookback đã chọn** | **3 ngày** - làm tròn lên từ P99 = 2,73 ngày. Phân bố có hai cụm tách rời: 94,95% bản ghi tới trong vòng 0–6 giờ, 5,05% còn lại tới sau 43–71 giờ; không có gì ở giữa. Cửa sổ 3 ngày phủ trọn cụm thứ hai. |
| **Nguyên nhân** | Điều kiện lọc incremental là `where event_date > (select max(event_date) from {{ this }})` - nó lấy mốc theo **thời điểm sự kiện xảy ra**, trong khi thứ quyết định "dữ liệu đã có mặt trong kho hay chưa" lại là **thời điểm nó tới kho** (`_ingested_at`). Hai đại lượng này lệch nhau tới ~3 ngày với 5% dữ liệu. Hệ quả: một event xảy ra 08-12 nhưng tới kho 08-15, tại lượt chạy 08-15 phải so với `max(event_date)` trong bảng đích đang là 08-14 - `08-12 > 08-14` sai, nên nó bị bỏ qua. Và nó bị bỏ qua **vĩnh viễn**, vì ở mọi lượt chạy sau `max(event_date)` chỉ lớn thêm. Cửa sổ chỉ tiến, không bao giờ lùi, nên mọi dữ liệu về muộn rơi vào một vùng chết không lượt chạy nào chạm tới. Đây cũng là lý do bảng vẫn "ổn định": nó bỏ sót một cách tất định. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`, hai thay đổi phải đi cùng nhau: (1) đổi điều kiện lọc thành `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`; (2) thêm `unique_key = ['event_date','customer_id']` + `incremental_strategy = 'merge'`. |
| **Bằng chứng** | trước: 8.645 hàng · 455 cặp `(ngày, khách)` có trong Silver mà không có trong Gold - sau: 9.100 hàng · 0 cặp thiếu · checksum `3db448685c` giống nhau ở cả 5 lượt |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` là **một** quan sát cực trị: nó có thể là 2,94 ngày hôm nay và 9 ngày vào tuần
> sau nếu một partition nguồn bị kẹt, nên lấy `max` làm thiết kế nghĩa là để một ngoại lệ
> hiếm quyết định chi phí của **mọi** lượt chạy về sau - mỗi ngày lookback thêm là một
> ngày dữ liệu phải đọc lại và tính lại, ở mọi lượt, mãi mãi. P99 là phát biểu có kiểm
> soát: "99% dữ liệu tới trong ngần này thời gian", chi phí biết trước và phần bỏ sót
> cũng biết trước (1%). Ở bộ dữ liệu này hai con số gần nhau (2,73 vs 2,94) nên lookback
> 3 ngày phủ được cả `max` - nhưng lý do chọn 3 vẫn là P99, không phải may mắn.
>
> Chiều ngược lại cũng có giá: window quá hẹp thì mất dữ liệu **âm thầm** - không lỗi,
> không cảnh báo, chỉ là những con số thấp hơn thực tế mà không ai biết. Đó chính là
> phiếu #1043. Giữa "tốn thêm một chút mỗi lượt" và "sai số không đo được", chọn cái
> đầu. Điều kiện đi kèm: mở rộng window thì cùng một cặp khoá sẽ được tính lại nhiều
> lượt, nên **bắt buộc** phải có `unique_key` để lần tính sau thay thế lần trước - nếu
> không thì vừa sửa xong lỗi thiếu hàng đã tự tạo lại đúng lỗi cộng dồn của nhiệm vụ 1
> trên một bảng khác.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Pipeline không dừng, `dbt test` vẫn 9/9 pass, nhưng `silver_tickets.priority` có **6.488 hàng NULL** (52% bảng) cộng với 118 hàng mang giá trị `0`, `5`, `-1` - tổng 6.606 hàng vi phạm miền 1..4. Đối chiếu theo ngày: trước 08-10 chỉ ~1% bản ghi không phải số, từ 08-10 trở đi là ~99%. Model phân loại kém hẳn từ đúng ngày đó. |
| **Nguyên nhân** | Ngày 08-10 team backend đổi cách biểu diễn `priority` từ số sang nhãn chữ (`urgent/high/medium/low`). Tầng Silver chuẩn hoá bằng `try_cast(priority_raw as integer)`, mà `try_cast` **thiết kế để không ném lỗi**: gặp giá trị không ép được thì trả `NULL` và đi tiếp. Nghĩa là một thay đổi schema phía nguồn được biến thành 6.488 giá trị rỗng một cách hoàn toàn im lặng - không exception, không test đỏ, không cảnh báo; tín hiệu duy nhất là chất lượng model tụt, và tín hiệu đó chỉ xuất hiện sau nhiều ngày. `try_cast` còn sai theo **hướng ngược lại**: `'0'`, `'5'`, `'-1'` đúng là integer nên nó nhận, dù contract nói `priority ∈ 1..4`. Cùng lúc, `contract: enforced: false` gỡ mất chốt chặn duy nhất có thể phát hiện sớm: contract bật thì dbt so kiểu từng cột với khai báo và dừng model ngay tại lượt chạy đầu tiên sau khi nguồn đổi format. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **(1) Số hợp lệ** `1 2 3 4` (6.846 bản ghi) - đúng contract cũ, **giữ nguyên**. **(2) Nhãn chữ** `urgent high medium low` (7.142 bản ghi) - đây là **schema evolution**: ý nghĩa không đổi, chỉ đổi cách biểu diễn, nên **map về 1..4** theo tài liệu API. **(3) Giá trị không hợp lệ** `P1 P2 unknown 0 5 -1 '' NULL` (312 bản ghi) - dữ liệu hỏng thật, **đưa vào quarantine**. Tiêu chí phân biệt nhóm 2 với nhóm 3: *giá trị này có mang đúng thông tin của contract cũ, chỉ khác cách viết hay không?* Có thì map, không thì quarantine. Xử lý nhóm 2 như nhóm 3 sẽ vứt bỏ 7.142 bản ghi hoàn toàn tốt chỉ vì nguồn đổi format. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng khối `CASE` xử lý đủ ba nhóm, trả `NULL` cho nhóm 3; viết thêm `priority_reject_reason` phân loại lý do bị loại. `dbt/models/silver/silver_tickets.sql`: thêm CTE `valid` **lọc bản ghi hỏng trước, xếp hạng `row_number()` sau**. `dbt/models/silver/quarantine_tickets.sql`: `where {{ normalize_priority('priority_raw') }} is null` - dùng đúng macro mà Silver dùng nên hai model không thể lệch nhau. `dbt/models/silver/schema.yml`: `contract.enforced: true` + test `not_null` và `accepted_values [1,2,3,4]` trên cột `priority`. |
| **Bằng chứng** | `quarantine_tickets` = **312 hàng** (đúng grain: 312 bản ghi CDC, tất cả đều `op='u'`, thuộc 312 ticket khác nhau) · `dbt test` **11/11 pass** (9 → 11) · `silver_tickets.priority` phân bố `1:3.134 · 2:3.029 · 3:3.115 · 4:3.202`, không còn NULL, không còn giá trị ngoài miền · `silver_tickets` vẫn giữ đủ **12.480** ticket |

Lý do bị loại, thống kê từ `quarantine_tickets.reject_reason`:

| Lý do | Số bản ghi |
|---|---|
| priority là số nhưng ngoài miền 1..4 (`0`, `5`, `-1`) | 118 |
| priority là chuỗi không có trong bảng quy đổi (`P1`, `P2`, `unknown`) | 116 |
| priority rỗng (`''`) | 43 |
| priority thiếu (`NULL`) | 35 |

Vì sao **lọc trước, xếp hạng sau** chứ không ngược lại: cả 312 bản ghi hỏng đều là
`op='u'`, tức là bản cập nhật **mới nhất** của ticket đó. Nếu xếp hạng trước rồi mới lọc,
`row_number() = 1` rơi đúng vào bản ghi hỏng và cả ticket biến mất khỏi Silver - 12.480
tụt xuống 12.168. Cái cần loại là **bản ghi CDC** hỏng, không phải **ticket**: ticket đó
vẫn còn một trạng thái hợp lệ từ lần cập nhật trước.

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng
khi gặp bản ghi lỗi?

> **Chặn ở Silver.** Bronze là bản sao trung thực của nguồn - nhiệm vụ duy nhất của nó là
> giữ nguyên payload gốc, không ép kiểu, không phán xét. Nếu Bronze từ chối bản ghi lỗi
> thì bằng chứng biến mất ngay tại chỗ: khi đội vận hành hỏi "backend đã gửi cái gì, từ
> lúc nào, bao nhiêu bản ghi", ta không còn gì để trả lời, và cũng không thể tính lại
> lịch sử sau khi đã hiểu ra quy tắc chuyển đổi (như việc map `urgent → 1` ở đây). Mọi
> phán xét về chất lượng thuộc về Silver, nơi contract được phát biểu.
>
> **Không dừng pipeline**, vì tương quan quy mô: 312 bản ghi hỏng so với 12.480 ticket,
> 129.462 event và 31.200 chunk hoàn toàn bình thường. Để `dbt test` fail và dừng DAG
> nghĩa là lấy 0,2% dữ liệu xấu làm lý do chặn 99,8% dữ liệu tốt không đến được RAG
> index, model phân loại và agent định tuyến - một sự cố nhỏ bị khuếch đại thành sự cố
> toàn hệ thống. Cách làm đúng là **định tuyến**: bản ghi lỗi rẽ sang `quarantine_tickets`
> kèm lý do cụ thể, phần còn lại chảy tiếp, và bảng quarantine trở thành hàng đợi có thể
> đo được cho người trực xử lý. Dừng cả DAG chỉ hợp lý khi tỷ lệ lỗi vượt một ngưỡng đủ
> lớn để nghi ngờ chính nguồn dữ liệu đã hỏng, chứ không phải khi gặp bản ghi lỗi đầu tiên.

---

## 4 · *(mở rộng)* Bài trong EXTRA.md

### Bài A - Query dashboard chậm

| | |
|---|---|
| **Triệu chứng** | Dashboard 38 giây, ba tháng trước 2 giây, không ai sửa code. Đo bằng `make explain`: **5.000.000 rows scanned** cho một dataset chỉ có **130.683 hàng thật**, nằm rải trong **5.000 file** Parquet. |
| **Nguyên nhân** | Hai lỗi cộng hưởng, đều thuộc về **storage layout chứ không phải câu query**. (1) *Small-file problem*: 5.000 file mỗi file vài chục KB, không partition. DuckDB đọc Parquet theo lô và làm tròn lên theo từng file, nên một file 26 hàng vẫn tốn công quét tương đương ~1.000 hàng - 5.000 file × ~1.000 = 5.000.000 đơn vị công quét cho 130.683 hàng, tức là gấp **38 lần** lượng dữ liệu thật. (2) *Predicate không sargable*: điều kiện viết `strftime(event_time,'%Y-%m-%d') = '2026-08-09'` bọc cột trong một function call, nên engine không so được kết quả function với tên thư mục partition, cũng không so được với min/max statistics của row group - nó buộc phải mở **toàn bộ** file rồi mới biết file nào có ích. Đường dẫn file không mang thông tin nào của điều kiện lọc, mà thứ duy nhất engine biết *trước khi* mở file lại chính là đường dẫn. |
| **Cách khắc phục** | `tools/compact.py`: `COPY … TO 'data/gold_events_v2' (format parquet, partition_by (event_date), row_group_size 10000)` với `order by customer_name, event_time`. Ba quyết định: **partition theo `event_date`** vì dashboard lọc theo một ngày và cột này chỉ có 14 giá trị → 14 thư mục, loại được 13/14 dataset chỉ bằng đường dẫn (partition theo `customer_id` với 650 giá trị sẽ tái tạo lại đúng small-file problem); **sắp xếp theo `customer_name`** để các hàng cùng khách nằm liền nhau, min/max của row group mới hẹp lại và lọc được `'ACME'` - dataset cũ xếp ngẫu nhiên nên mọi row group đều chứa ACME, statistics vô dụng; **`row_group_size 10000`** vì một ngày ~9.300 hàng, để mặc định 122.880 thì cả ngày gói trong một row group, min/max phủ toàn bộ khách hàng và mất tác dụng lọc. `queries/dashboard.sql`: trỏ vào dataset mới với `hive_partitioning = 1`, viết lại điều kiện thành `event_date = '2026-08-09'` (cột đứng một mình một vế), giữ nguyên select list. |
| **Bằng chứng** | rows scanned **5.000.000 → 9.324** (giảm **536,3×**, yêu cầu ≥ 10×) · files **5.000 → 14** · rows on disk giữ nguyên 130.683 · result hash **`4379e4c5d9f3` không đổi** · thời gian 5,7 ms |

### Bài B - Consumer gặp sự cố giữa batch

| | |
|---|---|
| **Triệu chứng** | `make crash-test`: chạy một mạch được 20.000 hàng; bị giết ở lô 7 rồi khởi động lại chỉ còn 19.500 hàng - **mất đúng 500 message** (một batch), im lặng, không lỗi. |
| **Nguyên nhân** | `consume()` gọi `consumer.commit()` **trước** `write_batch()`. Commit offset là lời tuyên bố "những message tới vị trí này đã xử lý xong", nhưng ở thời điểm đó dữ liệu chưa hề chạm tới kho. Cửa sổ giữa hai thao tác chính là cửa sổ mất dữ liệu: tiến trình chết trong đó thì offset đã dịch mà dữ liệu thì không, và lần khởi động lại đọc tiếp từ sau lô đã mất - không có cách nào biết để đọc lại. Đây là ngữ nghĩa **at-most-once**. Gốc rễ: `commit` và `write` là hai thao tác trên hai hệ thống khác nhau, không có transaction chung, nên **thứ tự giữa chúng chính là lựa chọn delivery semantics** - và thứ tự này đang đặt ngược. |
| **Cách khắc phục** | `ingest/consumer.py`: (1) đảo thành `write_batch()` → `maybe_crash()` → `commit()` ⇒ **at-least-once**: chết ở giữa thì offset chưa dịch, lô đó được đọc lại khi khởi động lại; (2) vì at-least-once nhất định sẽ phát lại, `write_batch()` phải **idempotent** - thêm `primary key` cho `event_id` trong `DDL` và đổi `INSERT` thành `insert … on conflict (event_id) do update set …`. Thiếu một trong hai là chưa đủ: chỉ đảo thứ tự thì hết mất hàng nhưng thành trùng hàng. |
| **Bằng chứng** | trước: A = 20.000 hàng, C = **19.500 hàng - mất đúng 500** (một lô), offset commit đã nhảy tới 3.500 · sau: A = 20.000 / 20.000 event_id, B chết ở lô 7 với offset dừng ở 3.000, C khởi động lại ghi thêm 17.000 → **20.000 hàng / 20.000 event_id** · không mất ✓ không trùng ✓ C == A ✓ → **BÀI MỞ RỘNG B: ĐẠT** |

Một chi tiết hiệu năng đi kèm: `executemany` với `ON CONFLICT` chạy **từng dòng một** và
cập nhật index sau mỗi dòng - 20.000 message mất ~39 giây. `write_batch()` vì thế ghi cả
lô bằng **một câu lệnh** (500 dòng/câu), ngữ nghĩa không đổi nhưng nhanh hơn hai bậc độ
lớn. Idempotent không nhất thiết phải đắt; đắt hay không nằm ở chỗ ghi theo lô hay theo dòng.

`DO UPDATE` khác `DO NOTHING` ở điểm nào khi một message được replay với nội dung đã đổi?

> Cả hai đều chống trùng hàng, nhưng khác nhau ở **bản nào thắng**. `DO NOTHING` giữ bản
> ghi đã có và bỏ qua bản mới: nếu message được phát lại vì nội dung đã được sửa, kho sẽ
> giữ mãi bản cũ lỗi thời. `DO UPDATE` ghi đè bằng nội dung mới nhất, nên kết quả cuối
> cùng không phụ thuộc vào việc một message được phát lại bao nhiêu lần hay theo thứ tự
> nào - đó mới đúng là idempotent theo nghĩa "trạng thái cuối cùng như nhau". Tôi chọn
> `DO UPDATE`. `DO NOTHING` chỉ hợp khi message là bất biến theo thiết kế (append-only
> log), lúc đó nó rẻ hơn vì không phải ghi lại.
>
> Exactly-once không tồn tại ở tầng giao vận: không có transaction nào bao được cả
> "commit offset ở broker" lẫn "ghi dữ liệu ở kho". Thứ chọn được là **at-least-once +
> phép ghi idempotent**, và kết quả quan sát được thì tương đương exactly-once.

---

## 4b · Ghi chú môi trường (không thuộc nội dung lab)

Bài được làm trên Windows nên `Makefile` có hai chỉnh sửa nhỏ, **không đụng tới logic
chấm điểm** và vẫn chạy y như cũ trên Linux/macOS:

- `PYBIN` tự dò `\.venv/Scripts` (Windows) hay `.venv/bin` (Linux/macOS) thay vì hard-code
  `bin/`; target `setup` dùng `python3` nếu có, không thì `python`.
- `export PYTHONUTF8 := 1` - mọi file trong repo là UTF-8, còn locale mặc định của Windows
  là cp1252 khiến dbt đọc `dbt_project.yml` lỗi `UnicodeDecodeError`. Trên Linux biến này
  vô hại.

---

## 5 · Bảng tự chấm (theo RUBRIC.md)

| | Của tôi | Kỳ vọng | ✓/✗ |
|---|---|---|---|
| `gold_training_set` - số hàng | 12.480 | 12.480 | ✓ |
| `gold_training_set` - ổn định 3 lượt | `8dd7c98653` × 3 | ✓ | ✓ |
| `gold_feature_daily` - số hàng | 9.100 | 9.100 | ✓ |
| `gold_feature_daily` - ổn định 3 lượt | `3db448685c` × 3 | ✓ | ✓ |
| `gold_doc_chunks` - số hàng | 31.200 | 31.200 | ✓ |
| `quarantine_tickets` - số hàng | 312 | 312 | ✓ |
| `silver_tickets` - số ticket | 12.480 | 12.480 | ✓ |
| `dbt test` | 11/11 pass | pass, > 9 test | ✓ |
| P99 độ trễ đo được | **2,7258 ngày** | (ghi số) | ✓ |
| **Tổng verify** | 4/4 | 4/4 tiêu chí | ✓ |
| *(thưởng)* Bài A - rows scanned | 5.000.000 → 9.324 (536,3×) | ≥ 10×, hash không đổi | ✓ |
| *(thưởng)* Bài B - `make crash-test` | 20.000 / 20.000, 0 trùng | ĐẠT | ✓ |

---

## 6 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Chạy pipeline **hai lần liên tiếp** rồi so số hàng và checksum. Một hệ thống "chạy không lỗi" chưa nói lên điều gì; câu hỏi thật là chạy lại có ra cùng kết quả không. Với mọi model incremental, đọc ngay ba tham số `materialized / unique_key / incremental_strategy` - thiếu khoá nghĩa là mọi cơ chế retry ở tầng trên đều là cơ chế nhân bản. |
| 2 | Với mọi bộ lọc incremental, hỏi: mốc so sánh đang là **thời điểm sự kiện xảy ra** hay **thời điểm dữ liệu tới kho**? Rồi đo phân bố độ lệch giữa hai đại lượng đó (P50/P95/P99/max) trước khi tin vào bất kỳ con số nào. Bảng "ổn định" vẫn có thể sai một cách tất định - số hàng đúng và tính lặp lại là hai phép đo khác nhau. |
| 3 | Contract có được **enforce** không, và những hàm chuyển kiểu nào đang **nuốt lỗi** (`try_cast`, `coalesce`, `safe_cast`). Chỗ nào dữ liệu sai được biến thành `NULL` mà không ai được báo, chỗ đó là một sự cố đang chờ. Kèm câu hỏi thiết kế: dữ liệu lỗi bị **chặn**, bị **bỏ qua**, hay được **định tuyến kèm lý do** - chỉ phương án thứ ba vừa giữ được pipeline chạy vừa giữ được bằng chứng để điều tra. |
