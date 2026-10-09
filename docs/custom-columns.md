# Migrate custom Logs columns

Copy custom Logs column names and SQL expressions between projects or organizations using the existing `braintrust-migrate` CLI. The tool reads the source directly; no JSON export or separate Python script is required.

This feature requires a checkout or release containing `--resources columns`; it is not available in v0.4.1. For an unreleased checkout containing this change, install from the repository root using Python 3.12 or newer:

```bash
pip install -e .
```

## Configure the source and destination

Create a `.env` file in the directory where you run the tool:

```dotenv
BT_SOURCE_API_KEY=<SOURCE_ORGANIZATION_API_KEY>
BT_SOURCE_URL=https://<SOURCE_API_HOST>
BT_DEST_API_KEY=<DESTINATION_ORGANIZATION_API_KEY>
BT_DEST_URL=https://<DESTINATION_API_HOST>
```

Replace the placeholders with your values. Use API keys belonging to the intended organizations, with permission to read the source project and update the destination project. Do not commit this file. For self-hosted deployments, use each deployment's API URL; both deployments must support `/v1/column`.

## Check the project selection

Run from the directory containing your `.env` file:

```bash
braintrust-migrate validate
braintrust-migrate migrate --resources columns \
  --projects "<SOURCE_PROJECT_NAME>" \
  --project-map '{"<SOURCE_PROJECT_NAME>":"<DESTINATION_PROJECT_NAME>"}' \
  --dry-run
```

Replace each project-name placeholder, including the JSON map keys and values. The tool resolves project IDs for you. An existing destination project with that name is reused; otherwise the actual migration creates it. Omit `--project-map` to keep the source project name.

The dry run checks configuration and discovers source columns for the first selected project. It does not create columns, check destination expression conflicts, or verify copied definitions. The actual migration checks conflicts for each project before writing that project's columns.

## Copy the columns

After checking the preview, run:

```bash
braintrust-migrate migrate --resources columns \
  --projects "<SOURCE_PROJECT_NAME>" \
  --project-map '{"<SOURCE_PROJECT_NAME>":"<DESTINATION_PROJECT_NAME>"}'
```

You can run this after a previous data migration to add missing definitions without migrating logs again. To also copy saved views, use `--resources columns,views`. The default `all` migration includes columns too.

For each project, the column migration:

1. Reads all custom Logs columns from the source and destination.
2. Stops that project's column migration before column writes if an existing name has a different expression.
3. Skips columns whose names and expressions match.
4. Creates missing columns with the destination project ID.
5. Reads the destination again to verify the names and expressions.

Expressions are compared exactly and copied unchanged. Resolve reported conflicts manually before rerunning. The tool does not overwrite or delete existing columns. The operation is not atomic: columns created before a later failure remain. Other resources or projects in the same run can still migrate. Rerunning checks the destination again and skips matching definitions.

## Verify in Braintrust

Open Logs in the destination project and check the column selector for the copied names. Enable them as needed and compare values on equivalent logs. An empty destination can verify definitions but cannot demonstrate calculated values; fields referenced by expressions must exist in destination logs.

This migration covers `object_type=project`, `subtype=project_log`, `variant=project_log`. It does not copy log data, rename fields inside expressions, or migrate other column scopes. Saved view settings such as filters and column visibility are separate resources handled by `views`. Copying definitions alone does not select them in a saved view. API verification confirms definitions, not displayed values or every view's references.
