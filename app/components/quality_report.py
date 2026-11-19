"""Great Expectations data quality scorecard component for LogiMesh."""

import json
import os
import streamlit as st
import pandas as pd


def render_quality_report(base_dir: str):
    st.subheader("🛡️ Great Expectations Data Quality Scorecard")
    st.caption("Automated Ingestion Contract Enforcement & Referential Integrity Audits")

    report_path = os.path.join(base_dir, "outputs", "quality_reports", "data_quality_report.json")

    if not os.path.exists(report_path):
        st.info("No Great Expectations report found. Run the pipeline to execute the validation suite.")
        return

    try:
        with open(report_path, "r") as f:
            report = json.load(f)

        col1, col2, col3, col4 = st.columns(4)
        pass_rate = report.get("pass_rate_pct", 0.0)
        status = report.get("overall_status", "UNKNOWN")

        col1.metric("Overall Contract Status", "PASSED" if status == "PASSED" else "FAILED")
        col2.metric("Contract Pass Rate", f"{pass_rate:.1f}%")
        col3.metric("Passed Assertions", f"{report.get('passed_assertions', 0)} / {report.get('total_assertions', 0)}")
        col4.metric("Failed Assertions", report.get("failed_assertions", 0))

        st.divider()
        st.write("#### Detailed Expectation Results")

        checks = report.get("checks", [])
        table_rows = []
        for c in checks:
            table_rows.append({
                "Table": c["target_table"],
                "Column": c.get("target_column") or "Table-level",
                "Expectation Rule": c["expectation_type"],
                "Status": "✅ PASS" if c["success"] else "❌ FAIL",
                "Observed Value": str(c.get("observed_value")),
                "Expected Value": str(c.get("expected_value")),
                "Details": c.get("details", ""),
            })

        st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

        with st.expander("📄 View Raw Great Expectations JSON Audit Payload"):
            st.json(report)

    except Exception as e:
        st.error(f"Error loading quality report: {e}")
