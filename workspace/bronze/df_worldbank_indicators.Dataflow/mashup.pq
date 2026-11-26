section Section1;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_worldbank_indicators_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "indicator_id", DestinationColumnName = "indicator_id"], [SourceColumnName = "indicator_name", DestinationColumnName = "indicator_name"], [SourceColumnName = "country_id", DestinationColumnName = "country_id"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "country_iso3", DestinationColumnName = "country_iso3"], [SourceColumnName = "period", DestinationColumnName = "period"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "unit", DestinationColumnName = "unit"], [SourceColumnName = "obs_status", DestinationColumnName = "obs_status"], [SourceColumnName = "decimals", DestinationColumnName = "decimals"], [SourceColumnName = "ingested_at", DestinationColumnName = "ingested_at"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_worldbank_indicators = let
    Country = "USA",
    Indicator = "NY.GDP.MKTP.CD",
    DateRange = "2016:" & Text.From(Date.Year(DateTimeZone.UtcNow())),
    PerPage = 1000,
    Key = {"indicator_id", "country_iso3", "period"},
    GetPage = (page as number) as list =>
        Json.Document(
            Web.Contents(
                "https://api.worldbank.org",
                [
                    RelativePath = "v2/country/" & Country & "/indicator/" & Indicator,
                    Query = [format = "json", per_page = Text.From(PerPage), date = DateRange, page = Text.From(page)]
                ]
            )
        ),
    FirstPage = GetPage(1),
    PageCount = FirstPage{0}[pages],
    Pages = {FirstPage} & List.Transform({2..PageCount}, GetPage),
    Rows = List.Combine(List.Transform(Pages, each _{1}? ?? {})),
    AsTable = Table.FromList(Rows, Splitter.SplitByNothing(), {"row"}),
    ExpandedRow = Table.ExpandRecordColumn(AsTable, "row", {"indicator", "country", "countryiso3code", "date", "value", "unit", "obs_status", "decimal"}),
    ExpandedIndicator = Table.ExpandRecordColumn(ExpandedRow, "indicator", {"id", "value"}, {"indicator_id", "indicator_name"}),
    ExpandedCountry = Table.ExpandRecordColumn(ExpandedIndicator, "country", {"id", "value"}, {"country_id", "country_name"}),
    Renamed = Table.RenameColumns(ExpandedCountry, {{"countryiso3code", "country_iso3"}, {"date", "period"}, {"decimal", "decimals"}}),
    Typed = Table.TransformColumnTypes(
        Renamed,
        {
            {"indicator_id", type nullable text},
            {"indicator_name", type nullable text},
            {"country_id", type nullable text},
            {"country_name", type nullable text},
            {"country_iso3", type nullable text},
            {"period", type nullable text},
            {"value", type nullable number},
            {"unit", type nullable text},
            {"obs_status", type nullable text},
            {"decimals", Int64.Type}
        }
    ),
    Published = Table.SelectRows(Typed, each [value] <> null),
    Existing = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = true]){[workspaceId = "."]}[Data]{[lakehouseName = "lh_bronze"]}[Data]{[Id = "bronze_worldbank_indicators", ItemKind = "Table"]}?[Data]?,
    NewRows = if Existing = null then Published else Table.RemoveColumns(Table.NestedJoin(Published, Key, Table.SelectColumns(Existing, Key), Key, "existing", JoinKind.LeftAnti), {"existing"}),
    WithIngestedAt = Table.AddColumn(NewRows, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
in
    WithIngestedAt;

shared bronze_worldbank_indicators_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "."]}[Data],
    Navigation_2 = Navigation_1{[lakehouseName = "lh_bronze"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_worldbank_indicators", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
