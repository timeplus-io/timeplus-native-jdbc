Timeplus 数据类型
====================

## Version
```
SELECT version()

┌─version()─┐
│   2.2.0   │
└───────────┘
```

## Data Type Families

```
SELECT * FROM system.data_type_families

┌─name──────────────────────┬─case_insensitive─┬─alias_to─┐
│ json                      │                1 │          │
│ map                       │                0 │          │
│ bool                      │                0 │          │
│ low_cardinality           │                0 │          │
│ interval_year             │                0 │          │
│ interval_hour             │                0 │          │
│ interval_minute           │                0 │          │
│ interval_microsecond      │                0 │          │
│ nested                    │                0 │          │
│ interval_month            │                0 │          │
│ interval_quarter          │                0 │          │
│ uint64                    │                0 │          │
│ ipv6                      │                1 │          │
│ interval_week             │                0 │          │
│ enum16                    │                0 │          │
│ uuid                      │                0 │          │
│ uint8                     │                0 │          │
│ nothing                   │                0 │          │
│ string                    │                0 │          │
│ datetime64                │                1 │          │
│ date                      │                1 │          │
│ decimal                   │                1 │          │
│ tuple                     │                0 │          │
│ decimal128                │                1 │          │
│ aggregate_function        │                0 │          │
│ decimal64                 │                1 │          │
│ nullable                  │                0 │          │
│ interval_nanosecond       │                0 │          │
│ int16                     │                0 │          │
│ int256                    │                0 │          │
│ int128                    │                0 │          │
│ uint256                   │                0 │          │
│ ipv4                      │                1 │          │
│ decimal256                │                1 │          │
│ float64                   │                0 │          │
│ fixed_string              │                0 │          │
│ uint128                   │                0 │          │
│ array                     │                0 │          │
│ int32                     │                0 │          │
│ date32                    │                1 │          │
│ simple_aggregate_function │                0 │          │
│ interval_day              │                0 │          │
│ enum                      │                0 │          │
│ int64                     │                0 │          │
│ datetime                  │                1 │          │
│ decimal32                 │                1 │          │
│ enum8                     │                0 │          │
│ int8                      │                0 │          │
│ float32                   │                0 │          │
│ interval_second           │                0 │          │
│ interval_millisecond      │                0 │          │
│ uint32                    │                0 │          │
│ datetime32                │                1 │          │
│ uint16                    │                0 │          │
│ boolean                   │                1 │ bool     │
│ inet4                     │                1 │ ipv4     │
│ byte                      │                1 │ int8     │
│ double                    │                1 │ float64  │
│ integer                   │                1 │ int32    │
│ VARCHAR                   │                1 │ string   │
│ float                     │                1 │ float32  │
│ uint                      │                1 │ uint32   │
│ bigint                    │                1 │ int64    │
│ inet6                     │                1 │ ipv6     │
│ int                       │                1 │ int32    │
│ smallint                  │                1 │ int16    │
└───────────────────────────┴──────────────────┴──────────┘
```