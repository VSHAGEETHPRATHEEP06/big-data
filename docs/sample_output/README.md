# Sample output

Daily fleet reports from our 15-day test run (simulated days 1 to 15 September 2026).
Airflow builds one HTML report and one CSV file per simulated day with the task `build_daily_report`.

- `daily_report_<date>.html`: open it in any browser (styles are inline, no internet needed).
- `daily_report_<date>.csv`: the same results, one row per vehicle, for Excel or pandas.

Good days to look at:

- `2026-09-03`: V007 (degrading vehicle) is first flagged as becoming unprofitable.
- `2026-09-04`: V013 loses money only because of a garage service bill, so it is not flagged.
- `2026-09-15`: the report shown in Figure 6 of the project report.

A running stack serves the same files at `http://localhost:8000/reports/<date>.html`.
