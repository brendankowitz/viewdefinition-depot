# ViewDefinition Depot

SQL on FHIR ViewDefinition samples for exporting selected FHIR data to target-specific Parquet projections.

## Packs

| FHIR release | Target | Views | Guide |
| --- | --- | --- | --- |
| R4 (4.0.1) | Fabric healthcare data solutions Silver v1.4.0 | [Patient and Observation](views/r4/fabric-silver/) | [Fabric Silver pack](docs/packs/fabric-silver.md) |
| R4 (4.0.1) | All 146 HDS v1.4.0 resource tables | [146 custom projections](views/r4/fabric-silver-hds/) · [schema contracts](contracts/fabric-hds/1.4.0/r4/catalog.json) | [Full R4 catalog: export and destinations](docs/packs/fabric-silver-hds.md) |

The two-view starter pack uses synthetic [fixtures](examples/r4/fabric-silver/input/) and selected-field [contracts](contracts/fabric-hds/1.4.0/).

The full catalog covers **146/146 resource types**; this is not a claim of Silver row parity (**0/146 independently verified**). A view selects the data, not the sink: export Parquet with Ignixa and load it into a custom Fabric Lakehouse table. Writing Microsoft's predefined Silver tables is a separate native-writer project.

## Quick start

From the repository root, install .NET 10 SDK and Python with pip. The local tool manifest pins Ignixa SQL on FHIR CLI 0.6.82; the requirements file pins DuckDB 2.0.1.

```powershell
dotnet tool restore
python -m pip install -r requirements-test.txt
python scripts/check-samples.py
```

The checker verifies that contract filenames, contract resources, and view resources agree, then validates the views, exports the fixtures to a temporary directory, and checks Parquet schemas and representative rows against the contracts. CI runs the same check and the focused identity regressions (`python scripts/test_check_samples.py`).

## Manual export

After restoring the tool, replace `<output-directory>` with a writable directory and run:

```powershell
dotnet tool run ignixa-sqlonfhir -- r4 run --views views/r4/fabric-silver --input examples/r4/fabric-silver/input --out <output-directory> --format parquet --var sourceSystem=example-east
```

Ignixa writes `Patient.parquet` and `Observation.parquet` from the resource-named ViewDefinition file stems. These are selected Parquet projections, not drop-in Fabric Silver Delta tables. See the [pack guide](docs/packs/fabric-silver.md) for missing fields and loader responsibilities.
