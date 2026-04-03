### Simple Mini Database (SMDB)

Simple-Mini-Database (smdb) is a library for a lightweight, flat-indexed database format.
See usage examples in `tests/integration_test.rs`.

## Binary Format Specification (BYK v01)

The database format is designed for O(1) access and is very simple:

1. **File Header**: 
        Starts with `0x42594B` (Magic "BYK") followed by the Version byte (current: `0x01`).

2. **Dictionaries**:
        Stores row and column names as linear maps.

3. **Data Cells**:
        `col` (2 bytes) + `row` (2 bytes) + `data_payload` + `0xED656E64DE` (End marker).

Fast, flat, and transparent.
Licensed under MPL-2.0.
