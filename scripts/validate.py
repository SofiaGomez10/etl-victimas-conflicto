import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
import warnings
import os

warnings.filterwarnings("ignore")


# ── Constants ─────────────────────────────────────────────────────────────────

GX_ROOT = os.path.join(os.path.dirname(__file__), "..", "great_expectations")
SUITE_NAME = "suite_dataset_final"
DATASOURCE_NAME = "pandas_runtime"
DATA_ASSET_NAME = "dataset_final"


# ── Setup context ─────────────────────────────────────────────────────────────

def get_context(gx_root: str):
    context = gx.data_context.FileDataContext.create(gx_root)
    return context


# ── Add datasource ────────────────────────────────────────────────────────────

def add_datasource(context):
    datasource_config = {
        "name": DATASOURCE_NAME,
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            }
        },
    }

    try:
        context.get_datasource(DATASOURCE_NAME)
        print(f"Datasource '{DATASOURCE_NAME}' already exists")
    except Exception:
        context.add_datasource(**datasource_config)
        print(f"Datasource '{DATASOURCE_NAME}' created")

    return context


# ── Create suite with Onboarding Assistant ─────────────────────────────────────

def create_suite(context, df: pd.DataFrame):
    # Delete existing suite if exists
    try:
        context.delete_expectation_suite(SUITE_NAME)
        print(f"Existing suite '{SUITE_NAME}' deleted")
    except Exception:
        pass

    batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name="runtime_connector",
        data_asset_name=DATA_ASSET_NAME,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"batch_id": "profiling_batch"},
    )

    # Use Onboarding Data Assistant to auto-generate expectations
    result = context.assistants.onboarding.run(
        batch_request=batch_request,
        exclude_column_names=[],
    )

    suite = result.get_expectation_suite(expectation_suite_name=SUITE_NAME)
    context.save_expectation_suite(suite)

    print(f"Suite '{SUITE_NAME}' created with {len(suite.expectations)} expectations")
    return suite


# ── Validate ──────────────────────────────────────────────────────────────────

def run_validation(context, df: pd.DataFrame):
    batch_request = RuntimeBatchRequest(
        datasource_name=DATASOURCE_NAME,
        data_connector_name="runtime_connector",
        data_asset_name=DATA_ASSET_NAME,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"batch_id": "validation_batch"},
    )

    checkpoint_config = {
        "name": "checkpoint_dataset_final",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "expectation_suite_name": SUITE_NAME,
    }

    try:
        context.get_checkpoint("checkpoint_dataset_final")
    except Exception:
        context.add_checkpoint(**checkpoint_config)

    results = context.run_checkpoint(
        checkpoint_name="checkpoint_dataset_final",
        validations=[{"batch_request": batch_request}],
    )

    success = results.success
    print(f"\nValidation result: {'PASSED ✅' if success else 'FAILED ❌'}")

    if not success:
        raise ValueError("Great Expectations validation failed. Check the Data Docs for details.")

    return results


# ── Main entry point (called from Airflow) ────────────────────────────────────

def validate_all(dataset_final_path: str, gx_root: str):
    print("Loading dataset_final...")
    df = pd.read_parquet(dataset_final_path)
    print(f"Rows: {len(df)} · Columns: {len(df.columns)}")

    print("Setting up GX context...")
    context = get_context(gx_root)
    context = add_datasource(context)

    # Create suite if it doesn't exist yet
    suite_path = os.path.join(gx_root, "expectations", f"{SUITE_NAME}.json")
    if not os.path.exists(suite_path):
        print("Suite not found — running Onboarding Assistant to create it...")
        create_suite(context, df)
    else:
        print(f"Suite '{SUITE_NAME}' already exists — skipping profiling")

    print("Running validation...")
    run_validation(context, df)


# ── Standalone execution ──────────────────────────────────────────────────────

if __name__ == "__main__":
    validate_all(
        dataset_final_path="data/processed/dataset_final.parquet",
        gx_root="great_expectations",
    )