# NASA HTTP Web Server Log: Formal Parsing & Analytical Specification

> **Status:** Frozen Baseline Contract (Contract 2)  
> **Applies to:** All four processing engines (MongoDB, Hive, Pig, MapReduce) and reference test suites.

---

## 1. Input Processing & Record Boundaries

1. **Physical Record Definition:**
   Every physical line in the input file constitutes exactly one input record, including blank/empty lines.
2. **Line Terminators:**
   Only the trailing newline (`\n` or `\r\n`) is stripped during reading. No internal characters, leading whitespace, or trailing inline whitespace are trimmed before regex matching.
3. **Encoding:**
   Input files must be read in binary mode and decoded using **ISO-8859-1 (Latin-1)**. Latin-1 maps all 256 byte values directly to Unicode code points $0x00$ to $0xFF$, guaranteeing lossless decoding without decoding exceptions.
4. **Conservation Invariant:**
   For every batch and across the entire dataset, the counting invariant:
   $$\text{total\_records} = \text{parsed\_records} + \text{malformed\_records}$$
   must strictly hold. Malformed lines must never be dropped silently; their occurrences must be tracked and counted.

---

## 2. Line Validation & Parsing Regular Expression

A record is classified as **parsed** if and only if:
1. It matches the complete **Line Regex** anchored from start (`^`) to end (`$`).
2. The extracted month field (Group 3) is a valid calendar month identifier.

Any record failing either condition is classified as **malformed**.

### 2.1. Line Regex Pattern
```regex
^(\S+) \S+ \S+ \[(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-]\d{4})\] "(.*)" (\d{3}) (\d+|-)\s*$
```

### 2.2. Capture Group Mapping

| Group Index | Captured Entity | Type / Format Constraint | Example |
| :---: | :--- | :--- | :--- |
| **1** | `host` | Client hostname or IPv4 address (kept as-is, **case-preserved**) | `in24.inetnebr.com` |
| **2** | `day` | 2-digit day of month (`01`–`31`) | `01` |
| **3** | `month` | 3-letter month abbreviation (case-sensitive) | `Jul` |
| **4** | `year` | 4-digit year | `1995` |
| **5** | `hour` | 2-digit hour (`00`–`23`) | `00` |
| **6** | `minute` | 2-digit minute (`00`–`59`) | `00` |
| **7** | `second` | 2-digit second (`00`–`59`) | `01` |
| **8** | `timezone` | Timezone offset string (`[+-]\d{4}`) | `-0400` |
| **9** | `request` | Unparsed HTTP request string inside quotes | `GET /history/apollo/ HTTP/1.0` |
| **10** | `status` | 3-digit integer HTTP status code | `200` |
| **11** | `bytes` | Positive integer byte count or `-` | `6245` or `-` |

### 2.3. Month Validation Rule
Group 3 must exactly match one of the following 12 case-sensitive month tokens:
```
Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec
```
If Group 3 is not one of these 12 strings, the record is **malformed**.

---

## 3. Derived Field Rules

From a successfully matched record, extract and compute the following structured fields:

| Field Name | Type | Extraction & Derivation Logic |
| :--- | :--- | :--- |
| `host` | String | Exactly Group 1 (no lowercasing or trimming). |
| `timestamp` | String / DateTime | Formatted as `YYYY-MM-DD HH:MM:SS` using the log's **own local wall-clock time** (Group 4, Month numeric `01`–`12`, Group 2, Group 5, Group 6, Group 7). **Do NOT convert timezone.** |
| `log_date` | String / Date | Formatted as `YYYY-MM-DD` from the log's local time. |
| `log_hour` | Integer | Integer extracted from Group 5 (`0` to `23`). |
| `status_code` | Integer | Integer conversion of Group 10. |
| `bytes_transferred`| Integer / BigInt | If Group 11 is `"-"`, value is `0`. Otherwise, integer conversion of Group 11. |

---

## 4. Request Splitting Regular Expression

The unparsed request string (Group 9) is split using the following secondary regex:

```regex
^(\S+)\s+(.*?)(?:\s+(HTTP/\S+))?\s*$
```

### Request Decomposition Rules:
1. **If Regex Matches:**
   - `http_method`: Group 1 (e.g., `GET`, `POST`, `HEAD`).
   - `resource_path`: Group 2 (may be empty string `""` or contain internal spaces).
   - `protocol_version`: Group 3 if present; otherwise empty string `""`.
2. **If Regex Fails (e.g., empty request `""`, corrupt request string):**
   - `http_method`: `""`
   - `resource_path`: `""`
   - `protocol_version`: `""`
3. **Crucial Rule:** Request split failure **does NOT make the record malformed**. As long as the line passed the primary Line Regex and month validation, the record is **parsed**.

---

## 5. Analytical Query Specifications

All four engines must execute these queries and output schema-compliant result sets.

### Query 1: Daily Traffic Summary
- **Aggregation:** Group by `(log_date, status_code)`.
- **Metrics:**
  - `request_count`: Total number of requests ($\text{count}(*)$).
  - `total_bytes`: Total bytes transferred ($\sum \text{bytes\_transferred}$).
- **Output Columns:**
  - `log_date` (String `YYYY-MM-DD`)
  - `status_code` (Integer)
  - `request_count` (Integer)
  - `total_bytes` (BigInt)

### Query 2: Top Requested Resources
- **Aggregation:** Group by `resource_path` (including `""`).
- **Metrics:**
  - `request_count`: Total requests for this path.
  - `total_bytes`: Total bytes transferred for this path.
  - `distinct_host_count`: Number of distinct client hosts requesting this path.
- **Ordering & Filtering:**
  - Order by `request_count DESC, resource_path ASC` (standard lexicographical byte order for tie-breaking).
  - Limit to top 20 records.
- **Output Columns:**
  - `rank` (Integer, $1 \dots 20$)
  - `resource_path` (String)
  - `request_count` (Integer)
  - `total_bytes` (BigInt)
  - `distinct_host_count` (Integer)

### Query 3: Hourly Error Analysis
- **Definition of Error:** HTTP status code in range $400 \le \text{status\_code} \le 599$.
- **Aggregation:** Group by `(log_date, log_hour)` for all date/hour combinations having $\ge 1$ request.
- **Metrics:**
  - `error_request_count`: Number of requests with $400 \le \text{status\_code} \le 599$.
  - `total_request_count`: Total requests in this hour window.
  - `error_rate`: $\text{round}\left(\frac{\text{error\_request\_count}}{\text{total\_request\_count}}, 4\right)$ using **round-half-away-from-zero** (e.g., $0.00005 \to 0.0001$).
  - `distinct_error_hosts`: Count of distinct client hosts generating error requests ($0$ if `error_request_count == 0`).
- **Output Columns:**
  - `log_date` (String `YYYY-MM-DD`)
  - `log_hour` (Integer $0 \dots 23$)
  - `error_request_count` (Integer)
  - `total_request_count` (Integer)
  - `error_rate` (Float / Decimal, 4 decimal places)
  - `distinct_error_hosts` (Integer)
