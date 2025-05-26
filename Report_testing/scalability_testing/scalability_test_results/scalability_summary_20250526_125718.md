# Scalability Test Results

**Test Date:** Mon May 26 14:23:10 UTC 2025
**Memset Percentage:** 5%
**Account Count Range:** 50M to 100M accounts
**Systems Tested:** log_optim_parameterized, state_management_parameterized

## Test Configuration

- **Transaction Generation:**
  - Batch size: 65536 transactions
  - Total batches: 50000
  - Total transactions: 3276800000 per test
  - Memset percentage: 5% (fixed)

- **Account Count Progression:**
  - 50M: 50000000 accounts
  - 100M: 100000000 accounts

## Results Summary

The detailed results are available in: `scalability_test_results/scalability_results_20250526_125718.csv`

### Performance vs Account Count (Initial Run Metrics & Recovery Times)

| Accounts | log_optim Thr. (Initial, tx/s) | log_optim Rec. Time (ms) | state_mgmt Thr. (Initial, tx/s) | state_mgmt Rec. Time (ms) | log_optim Avg Batch (Initial, ms) | state_mgmt Avg Batch (Initial, ms) | State Size (MB) |
|----------|--------------------------------|--------------------------|---------------------------------|---------------------------|-----------------------------------|------------------------------------|-----------------|
| 50M |  | 270.486 |  | 178141.801 | N/A | N/A | 381.46 |
| 100M |  | 534.065 |  | 707138.273 | N/A | N/A | 762.93 |

### Notes:
- 'N/A' indicates data was not found or an error occurred during extraction.
- Throughput and Avg Batch times are from the 'initial' run.
- Recovery Time is from the 'recovery' run.
