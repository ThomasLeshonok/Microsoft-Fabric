section Section1;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_ecb_exchange_rates_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "key", DestinationColumnName = "key"], [SourceColumnName = "freq", DestinationColumnName = "freq"], [SourceColumnName = "currency", DestinationColumnName = "currency"], [SourceColumnName = "currency_denom", DestinationColumnName = "currency_denom"], [SourceColumnName = "exr_type", DestinationColumnName = "exr_type"], [SourceColumnName = "exr_suffix", DestinationColumnName = "exr_suffix"], [SourceColumnName = "time_period", DestinationColumnName = "time_period"], [SourceColumnName = "obs_value", DestinationColumnName = "obs_value"], [SourceColumnName = "obs_status", DestinationColumnName = "obs_status"], [SourceColumnName = "obs_conf", DestinationColumnName = "obs_conf"], [SourceColumnName = "obs_pre_break", DestinationColumnName = "obs_pre_break"], [SourceColumnName = "obs_com", DestinationColumnName = "obs_com"], [SourceColumnName = "time_format", DestinationColumnName = "time_format"], [SourceColumnName = "breaks", DestinationColumnName = "breaks"], [SourceColumnName = "collection", DestinationColumnName = "collection"], [SourceColumnName = "compiling_org", DestinationColumnName = "compiling_org"], [SourceColumnName = "diss_org", DestinationColumnName = "diss_org"], [SourceColumnName = "dom_ser_ids", DestinationColumnName = "dom_ser_ids"], [SourceColumnName = "publ_ecb", DestinationColumnName = "publ_ecb"], [SourceColumnName = "publ_mu", DestinationColumnName = "publ_mu"], [SourceColumnName = "publ_public", DestinationColumnName = "publ_public"], [SourceColumnName = "unit_index_base", DestinationColumnName = "unit_index_base"], [SourceColumnName = "compilation", DestinationColumnName = "compilation"], [SourceColumnName = "coverage", DestinationColumnName = "coverage"], [SourceColumnName = "decimals", DestinationColumnName = "decimals"], [SourceColumnName = "nat_title", DestinationColumnName = "nat_title"], [SourceColumnName = "source_agency", DestinationColumnName = "source_agency"], [SourceColumnName = "source_pub", DestinationColumnName = "source_pub"], [SourceColumnName = "title", DestinationColumnName = "title"], [SourceColumnName = "title_compl", DestinationColumnName = "title_compl"], [SourceColumnName = "unit", DestinationColumnName = "unit"], [SourceColumnName = "unit_mult", DestinationColumnName = "unit_mult"], [SourceColumnName = "ingested_at", DestinationColumnName = "ingested_at"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_ecb_exchange_rates = let
    SeriesKey = "D.USD.EUR.SP00.A",
    StartPeriod = "2016-03-01",
    Columns = {"key", "freq", "currency", "currency_denom", "exr_type", "exr_suffix", "time_period", "obs_value", "obs_status", "obs_conf", "obs_pre_break", "obs_com", "time_format", "breaks", "collection", "compiling_org", "diss_org", "dom_ser_ids", "publ_ecb", "publ_mu", "publ_public", "unit_index_base", "compilation", "coverage", "decimals", "nat_title", "source_agency", "source_pub", "title", "title_compl", "unit", "unit_mult"},
    Key = {"key", "time_period"},
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
    Selected = Table.SelectColumns(LowerCaseNames, Columns),
    AllText = Table.TransformColumnTypes(Selected, List.Transform(Columns, each {_, type text})),
    Typed = Table.TransformColumnTypes(AllText, {{"time_period", type nullable date}, {"obs_value", type nullable number}}, "en-US"),
    Existing = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = true]){[workspaceId = "."]}[Data]{[lakehouseName = "lh_bronze"]}[Data]{[Id = "bronze_ecb_exchange_rates", ItemKind = "Table"]}?[Data]?,
    NewRows = if Existing = null then Typed else Table.RemoveColumns(Table.NestedJoin(Typed, Key, Table.SelectColumns(Existing, Key), Key, "existing", JoinKind.LeftAnti), {"existing"}),
    WithIngestedAt = Table.AddColumn(NewRows, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
in
    WithIngestedAt;

shared bronze_ecb_exchange_rates_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "."]}[Data],
    Navigation_2 = Navigation_1{[lakehouseName = "lh_bronze"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_ecb_exchange_rates", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
