import asyncio

import pandas as pd
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterDeploymentId,
    FlowRunFilterState,
    FlowRunFilterStateType
)
from prefect.client.schemas.sorting import FlowRunSort

# Map deployment display names to their actual flow/deployment name pair
deployments = {
    "new_file_832": ("new_832_file_flow", "new_file_832"),
    "nersc_recon_flow": ("nersc_recon_flow", "nersc_recon_flow"),
    "alcf_recon_flow": ("alcf_recon_flow", "alcf_recon_flow"),
}


async def fetch_deployment_runs(label: str, flow_name: str, deployment_name: str, max_runs: int = 100):
    async with get_client() as client:
        try:
            deployment = await client.read_deployment_by_name(f"{flow_name}/{deployment_name}")

            flow_run_filter = FlowRunFilter(
                deployment_id=FlowRunFilterDeploymentId(any_=[deployment.id]),
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(any_=["COMPLETED"])
                )
            )

            runs = await client.read_flow_runs(
                flow_run_filter=flow_run_filter,
                sort=FlowRunSort.START_TIME_DESC,
                limit=max_runs
            )
        except Exception as e:
            print(f"❌ Failed to fetch runs for {label}: {e}")
            return []

        results = []
        for r in runs:
            if r.start_time and r.end_time:
                duration = (r.end_time - r.start_time).total_seconds()
                results.append({
                    "deployment_label": label,
                    "run_id": str(r.id),
                    "start_time": r.start_time,
                    "end_time": r.end_time,
                    "duration_s": duration
                })
        return results


async def collect_and_save():
    all_records = []
    summary = []

    for label, (flow_name, deployment_name) in deployments.items():
        print(f"🔍 Fetching {label} (deployment: {deployment_name})...")
        records = await fetch_deployment_runs(label, flow_name, deployment_name, max_runs=100)
        if not records:
            print(f"⚠️  No successful runs for {label}")
            continue

        df = pd.DataFrame(records)
        df.to_csv(f"{label}_successful_runs.csv", index=False)
        all_records.extend(records)

        durations = df["duration_s"]
        summary.append({
            "Deployment": label,
            "Count": len(durations),
            "Mean (s)": round(durations.mean(), 2),
            "Median (s)": round(durations.median(), 2),
            "Std Dev (s)": round(durations.std(), 2) if len(durations) > 1 else 0,
            "Min (s)": round(durations.min(), 2),
            "Max (s)": round(durations.max(), 2)
        })

    if not all_records:
        print("❌ No data to write.")
        return

    all_df = pd.DataFrame(all_records)
    all_df.to_csv("all_flows_combined.csv", index=False)

    summary_df = pd.DataFrame(summary)
    with open("flow_summary_table.tex", "w") as f:
        f.write(summary_df.to_latex(
            index=False,
            caption="Summary statistics of successful Prefect flow runs",
            label="tab:flow_summary")
            )

    print("✅ Done!")
    print("- Individual CSVs written")
    print("- Combined CSV: all_flows_combined.csv")
    print("- LaTeX summary table: flow_summary_table.tex")

if __name__ == "__main__":
    asyncio.run(collect_and_save())
