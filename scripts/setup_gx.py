import great_expectations as gx

context = gx.get_context(mode="file", project_root_dir="../great_expectations")
print("GX context created successfully")
print(context)