import great_expectations as gx
import pandas as pd

context = gx.get_context(
    mode="file",
    project_root_dir="../great_expectations"
)

def profile_source(df, suite_name):
    
    try:
        context.suites.get(suite_name)
    except:
        context.suites.add(
            gx.ExpectationSuite(name=suite_name)
        )

    try:
        datasource = context.data_sources.get("pandas_source")
    except:
        datasource = context.data_sources.add_pandas("pandas_source")

    data_asset = datasource.add_dataframe_asset(name=suite_name)

    batch = data_asset.build_batch_request(
        dataframe=df
    )

    validator = context.get_validator(
        batch_request=batch,
        expectation_suite_name=suite_name
    )

    validator.expect_table_columns_to_match_set(list(df.columns))

    for col in df.columns:
        validator.expect_column_values_to_not_be_null(col)

    validator.save_expectation_suite()

    print(f"Profiling created: {suite_name}")


#Load samples
sample1 = pd.read_csv("data/raw/samples/sample_source1.csv")
sample2 = pd.read_csv("data/raw/samples/sample_source2.csv")
sample3 = pd.read_csv("data/raw/samples/sample_source3.csv")

#Run profiling
profile_source(sample1, "suite_source1_cali")
profile_source(sample2, "suite_source2_santander")
profile_source(sample3, "suite_source3_api")

print("All suites created successfully")