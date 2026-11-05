section Section1;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_ecb_exchange_rates_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_ecb_exchange_rates = let
    SeriesKey = "D..EUR.SP00.A",
    StartPeriod = "2016-03-01",
    Response = Web.Contents(
        "https://data-api.ecb.europa.eu",
        [
            RelativePath = "service/data/EXR/" & SeriesKey,
            Query = [format = "csvdata", startPeriod = StartPeriod],
            Headers = [Accept = "text/csv"]
        ]
    ),
    Source = Csv.Document(Response, [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    LowerCaseNames = Table.TransformColumnNames(PromotedHeaders, Text.Lower),
    AllText = Table.TransformColumnTypes(LowerCaseNames, List.Transform(Table.ColumnNames(LowerCaseNames), each {_, type text})),
    Typed = Table.TransformColumnTypes(AllText, {{"time_period", type date}, {"obs_value", type number}}, "en-US"),
    WithIngestedAt = Table.AddColumn(Typed, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
in
    WithIngestedAt;

shared bronze_ecb_exchange_rates_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "."]}[Data],
    Navigation_2 = Navigation_1{[lakehouseName = "lh_bronze"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_ecb_exchange_rates", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
