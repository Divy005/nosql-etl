"""MongoDB aggregation pipelines for in-engine parsing and analytical queries.

Hard rule 3: All parsing and all three queries run inside MongoDB. Python only
inserts raw lines into MongoDB and reads the final aggregated results back.
"""

from typing import Any

# Line validation regex pattern
LINE_REGEX_PATTERN = (
    r"^(\S+) \S+ \S+ \[(\d{2})/([A-Za-z]{3})/(\d{4}):"
    r"(\d{2}):(\d{2}):(\d{2}) ([+-]\d{4})\] \"(.*)\" (\d{3}) (\d+|-)\s*$"
)

# Secondary request decomposition regex pattern
REQUEST_REGEX_PATTERN = r"^(\S+)\s+(.*?)(?:\s+(HTTP/\S+))?\s*$"

# Valid month token list for in-database index resolution
MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]


def build_parsing_pipeline(batch_id: int, target_collection: str) -> list[dict[str, Any]]:
    """Generate the aggregation pipeline that parses raw log lines inside MongoDB.

    Applies $regexFind, validates month via $indexOfArray, derives fields,
    and writes ALL documents (valid and malformed) to target_collection using $merge.
    """
    return [
        {"$match": {"batch_id": batch_id}},
        {
            "$project": {
                "batch_id": 1,
                "line_no": 1,
                "line": 1,
                "match": {"$regexFind": {"input": "$line", "regex": LINE_REGEX_PATTERN}},
            }
        },
        {
            "$project": {
                "batch_id": 1,
                "line_no": 1,
                "line": 1,
                "match": 1,
                "month_str": {
                    "$cond": [
                        {"$ne": ["$match", None]},
                        {"$arrayElemAt": ["$match.captures", 2]},
                        None,
                    ]
                },
            }
        },
        {
            "$project": {
                "batch_id": 1,
                "line_no": 1,
                "line": 1,
                "match": 1,
                "month_idx": {"$indexOfArray": [MONTHS, "$month_str"]},
            }
        },
        {
            "$project": {
                "batch_id": 1,
                "line_no": 1,
                "line": 1,
                "valid": {
                    "$and": [
                        {"$ne": ["$match", None]},
                        {"$gte": ["$month_idx", 0]},
                    ]
                },
                "match": 1,
                "month_idx": 1,
            }
        },
        {
            "$project": {
                "batch_id": 1,
                "line_no": 1,
                "line": 1,
                "valid": 1,
                "host": {
                    "$cond": [
                        "$valid",
                        {"$arrayElemAt": ["$match.captures", 0]},
                        None,
                    ]
                },
                "day": {
                    "$cond": [
                        "$valid",
                        {"$arrayElemAt": ["$match.captures", 1]},
                        None,
                    ]
                },
                "month_num": {
                    "$cond": [
                        "$valid",
                        {
                            "$cond": [
                                {"$lt": [{"$add": ["$month_idx", 1]}, 10]},
                                {
                                    "$concat": [
                                        "0",
                                        {"$toString": {"$add": ["$month_idx", 1]}},
                                    ]
                                },
                                {"$toString": {"$add": ["$month_idx", 1]}},
                            ]
                        },
                        None,
                    ]
                },
                "year": {
                    "$cond": [
                        "$valid",
                        {"$arrayElemAt": ["$match.captures", 3]},
                        None,
                    ]
                },
                "hour": {
                    "$cond": [
                        "$valid",
                        {"$arrayElemAt": ["$match.captures", 4]},
                        None,
                    ]
                },
                "minute": {
                    "$cond": [
                        "$valid",
                        {"$arrayElemAt": ["$match.captures", 5]},
                        None,
                    ]
                },
                "second": {
                    "$cond": [
                        "$valid",
                        {"$arrayElemAt": ["$match.captures", 6]},
                        None,
                    ]
                },
                "req_match": {
                    "$cond": [
                        "$valid",
                        {
                            "$regexFind": {
                                "input": {"$arrayElemAt": ["$match.captures", 8]},
                                "regex": REQUEST_REGEX_PATTERN,
                            }
                        },
                        None,
                    ]
                },
                "status_code": {
                    "$cond": [
                        "$valid",
                        {"$toInt": {"$arrayElemAt": ["$match.captures", 9]}},
                        None,
                    ]
                },
                "bytes_raw": {
                    "$cond": [
                        "$valid",
                        {"$arrayElemAt": ["$match.captures", 10]},
                        None,
                    ]
                },
            }
        },
        {
            "$project": {
                "batch_id": 1,
                "line_no": 1,
                "line": 1,
                "valid": 1,
                "host": 1,
                "log_date": {
                    "$cond": [
                        "$valid",
                        {"$concat": ["$year", "-", "$month_num", "-", "$day"]},
                        None,
                    ]
                },
                "log_hour": {
                    "$cond": [
                        "$valid",
                        {"$toInt": "$hour"},
                        None,
                    ]
                },
                "timestamp": {
                    "$cond": [
                        "$valid",
                        {
                            "$concat": [
                                "$year",
                                "-",
                                "$month_num",
                                "-",
                                "$day",
                                " ",
                                "$hour",
                                ":",
                                "$minute",
                                ":",
                                "$second",
                            ]
                        },
                        None,
                    ]
                },
                "http_method": {
                    "$cond": [
                        {"$and": ["$valid", {"$ne": ["$req_match", None]}]},
                        {"$arrayElemAt": ["$req_match.captures", 0]},
                        {"$cond": ["$valid", "", None]},
                    ]
                },
                "resource_path": {
                    "$cond": [
                        {"$and": ["$valid", {"$ne": ["$req_match", None]}]},
                        {
                            "$ifNull": [
                                {"$arrayElemAt": ["$req_match.captures", 1]},
                                "",
                            ]
                        },
                        {"$cond": ["$valid", "", None]},
                    ]
                },
                "protocol_version": {
                    "$cond": [
                        {"$and": ["$valid", {"$ne": ["$req_match", None]}]},
                        {
                            "$ifNull": [
                                {"$arrayElemAt": ["$req_match.captures", 2]},
                                "",
                            ]
                        },
                        {"$cond": ["$valid", "", None]},
                    ]
                },
                "status_code": 1,
                "bytes_transferred": {
                    "$cond": [
                        "$valid",
                        {
                            "$cond": [
                                {"$eq": ["$bytes_raw", "-"]},
                                0,
                                {"$toLong": "$bytes_raw"},
                            ]
                        },
                        None,
                    ]
                },
            }
        },
        {
            "$merge": {
                "into": target_collection,
                "on": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert",
            }
        },
    ]


def build_q1_pipeline() -> list[dict[str, Any]]:
    """Query 1: Daily traffic summary grouped by date and status code."""
    return [
        {"$match": {"valid": True}},
        {
            "$group": {
                "_id": {"log_date": "$log_date", "status_code": "$status_code"},
                "request_count": {"$sum": 1},
                "total_bytes": {"$sum": "$bytes_transferred"},
            }
        },
        {"$sort": {"_id.log_date": 1, "_id.status_code": 1}},
    ]


def build_q2_pipeline() -> list[dict[str, Any]]:
    """Query 2: Top 20 resources by request count using a 2-stage group.

    Stage 1: Group by (resource_path, host) to aggregate per-host metrics.
    Stage 2: Group by resource_path, summing counts and counting distinct hosts.
    Avoids huge in-memory $addToSet arrays.
    """
    return [
        {"$match": {"valid": True}},
        {
            "$group": {
                "_id": {"resource_path": "$resource_path", "host": "$host"},
                "request_count": {"$sum": 1},
                "total_bytes": {"$sum": "$bytes_transferred"},
            }
        },
        {
            "$group": {
                "_id": "$_id.resource_path",
                "request_count": {"$sum": "$request_count"},
                "total_bytes": {"$sum": "$total_bytes"},
                "distinct_host_count": {"$sum": 1},
            }
        },
        {"$sort": {"request_count": -1, "_id": 1}},
        {"$limit": 20},
    ]


def build_q3_pipeline() -> list[dict[str, Any]]:
    """Query 3: Hourly error analysis using a 2-stage group and half-away-from-zero rounding.

    Stage 1: Group by (log_date, log_hour, host) tracking errors and totals per host.
    Stage 2: Group by (log_date, log_hour) summing metrics and counting distinct error hosts.
    Stage 3: Calculate error_rate with round-half-away-from-zero arithmetic.
    """
    return [
        {"$match": {"valid": True}},
        {
            "$group": {
                "_id": {
                    "log_date": "$log_date",
                    "log_hour": "$log_hour",
                    "host": "$host",
                },
                "total_count": {"$sum": 1},
                "error_count": {
                    "$sum": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gte": ["$status_code", 400]},
                                    {"$lte": ["$status_code", 599]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$group": {
                "_id": {"log_date": "$_id.log_date", "log_hour": "$_id.log_hour"},
                "total_request_count": {"$sum": "$total_count"},
                "error_request_count": {"$sum": "$error_count"},
                "distinct_error_hosts": {
                    "$sum": {"$cond": [{"$gt": ["$error_count", 0]}, 1, 0]}
                },
            }
        },
        {
            "$project": {
                "_id": 1,
                "total_request_count": 1,
                "error_request_count": 1,
                "distinct_error_hosts": 1,
                "error_rate": {
                    "$cond": [
                        {"$eq": ["$total_request_count", 0]},
                        0.0,
                        {
                            "$divide": [
                                {
                                    "$floor": {
                                        "$add": [
                                            {
                                                "$multiply": [
                                                    {
                                                        "$divide": [
                                                            "$error_request_count",
                                                            "$total_request_count",
                                                        ]
                                                    },
                                                    10000,
                                                ]
                                            },
                                            0.5,
                                        ]
                                    }
                                },
                                10000.0,
                            ]
                        },
                    ]
                },
            }
        },
        {"$sort": {"_id.log_date": 1, "_id.log_hour": 1}},
    ]
