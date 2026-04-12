import great_expectations as gx
import pandas as pd
import json
import os

context = gx.get_context(
    mode="file",
    project_root_dir="../great_expectations"
)

def profile_source(df, suite_name):

    try:
        datasource = context.data_sources.get("pandas_source")
    except:
        datasource = context.data_sources.add_pandas("pandas_source")

    try:
        data_asset = datasource.get_asset(suite_name)
    except:
        data_asset = datasource.add_dataframe_asset(name=suite_name)

    batch_request = data_asset.build_batch_request(
        options={"dataframe": df}
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    validator.expect_table_columns_to_match_set(list(df.columns))

    for col in df.columns:
        validator.expect_column_values_to_not_be_null(col)

    suite = validator.expectation_suite

    path = f"great_expectations/gx/expectations/{suite_name}.json"

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w") as f:
        json.dump(suite.to_json_dict(), f, indent=2)

    print(f"Suite created: {path}")


#Load samples 
sample1 = pd.read_csv("data/raw/samples/sample_source1.csv")
sample2 = pd.read_csv("data/raw/samples/sample_source2.csv")
sample3 = pd.read_csv("data/raw/samples/sample_source3.csv")

#Run profiling
profile_source(sample1, "suite_source1_cali")
profile_source(sample2, "suite_source2_santander")
profile_source(sample3, "suite_source3_api")

print("All suites created and saved successfully")