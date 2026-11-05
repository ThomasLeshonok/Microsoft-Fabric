section Section1;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_worldbank_indicators_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_worldbank_indicators = let
    Country = "all",
    Indicator = "NY.GDP.PCAP.CD",
    DateRange = "2016:" & Text.From(Date.Year(DateTimeZone.UtcNow())),
    PerPage = 1000,
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
            {"indicator_id", type text},
            {"indicator_name", type text},
            {"country_id", type text},
            {"country_name", type text},
            {"country_iso3", type text},
            {"period", type text},
            {"value", type number},
            {"unit", type text},
            {"obs_status", type text},
            {"decimals", Int64.Type}
        }
    ),
    WithIngestedAt = Table.AddColumn(Typed, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
in
    WithIngestedAt;

shared bronze_worldbank_indicators_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "."]}[Data],
    Navigation_2 = Navigation_1{[lakehouseName = "lh_bronze"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_worldbank_indicators", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
