# Clickstream Session Analysis — PySpark

Assigns session IDs to raw clickstream events using **window functions** (no joins, no UDFs), then computes sessions-per-day and average session length.

---

## Problem Statement

> Given clickstream events `(user_id, ts, url)`, assign **session IDs** where a new session begins after **30 minutes of inactivity** per user. Then compute:
> - Number of sessions per day
> - Average session length

---

## Project Structure

```
.
├── data/
│   └── clickstream.csv        # Sample data (3 users, 2 days, 31 events)
├── output/                    # Written by the job
│   ├── events_with_sessions/
│   ├── sessions_per_day/
│   └── session_stats/
├── session_analysis.py        # Main PySpark job
├── requirements.txt
└── README.md
```

---

## Algorithm — Window Function Approach

```
Step 1  lag(ts) OVER (PARTITION BY user_id ORDER BY ts)
              → prev_ts (previous event time for same user)

Step 2  gap_seconds = unix_timestamp(ts) - unix_timestamp(prev_ts)
              NULL for a user's first event

Step 3  new_session_flag = 1  if  gap_seconds > 1800  OR  gap_seconds IS NULL
                           0  otherwise

Step 4  session_index = SUM(new_session_flag)
                        OVER (PARTITION BY user_id ORDER BY ts
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
              → monotonically increasing per-user counter

Step 5  session_id = concat(user_id, date, session_index)
              → globally unique string key
```

No shuffles beyond the two window sorts — efficient on large datasets.

---

## Sample Data (`data/clickstream.csv`)

| user_id | ts | url |
|---|---|---|
| u1 | 2024-01-15 08:00:00 | /home |
| u1 | 2024-01-15 08:05:30 | /products |
| u1 | 2024-01-15 08:12:00 | /products/42 |
| u1 | 2024-01-15 08:15:45 | /cart |
| u1 | 2024-01-15 09:00:00 | /home ← new session (44 min gap) |
| … | … | … |

---

## Running

```bash
# Install PySpark (Python 3.8+)
pip install -r requirements.txt

# Run the job (from repo root)
spark-submit session_analysis.py
# or simply:
python session_analysis.py
```

---

## Expected Outputs

### Sessions Per Day

| date | sessions_count |
|---|---|
| 2024-01-15 | 7 |
| 2024-01-16 | 6 |

### Average Session Length

| avg_length_seconds | avg_length_minutes | avg_events_per_session | total_sessions |
|---|---|---|---|
| ~558 | ~9.3 | ~2.4 | 13 |

---

## Key Concepts Used

| Concept | Usage |
|---|---|
| `Window.partitionBy().orderBy()` | Isolate each user's event stream |
| `F.lag()` | Access previous row's timestamp |
| `F.sum(...).over(rowsBetween(...))` | Cumulative session counter |
| `F.countDistinct()` | Sessions per day |
| `F.min() / F.max()` | Session start / end for length |
