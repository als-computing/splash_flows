from prefect.variables import Variable
import asyncio


async def create_variables():
    variables = {
        "alcf-allocation-root-path": {"alcf-allocation-root-path": "/eagle/IRIBeta/als"},
        "bl832-settings": {
            "delete_spot832_files_after_days": 1,
            "delete_data832_files_after_days": 35
        },
        "globus-settings": {"max_wait_seconds": 600},
        "pruning-config": {
            "max_wait_seconds": 120,
            "delete_alcf832_files_after_days": 1,
            "delete_data832_files_after_days": 14,
            "delete_nersc832_files_after_days": 7
        },
        "decision-settings": {
            "nersc_recon_flow/nersc_recon_flow": True,
            "new_832_file_flow/new_file_832": True,
            "alcf_recon_flow/alcf_recon_flow": False
        }
    }

    for name, value in variables.items():
        await Variable.set(name=name, value=value, overwrite=True)
        print(f"Created variable: {name}")


if __name__ == "__main__":
    asyncio.run(create_variables())
