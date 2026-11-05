section Section1;

shared OpenAQApiKey = "replace-with-openaq-api-key" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = "Text"];

shared NycBoundingBox = "-74.2591,40.4774,-73.7002,40.9162" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = "Text"];

shared DateFrom = "2016-03-01" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = "Text"];

shared DateTo = "" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = "Text"];

shared OpenAQWindow = let
    LockedScopeStart = "2016-03-01",
    Start = DateFrom,
    End = if DateTo = null or DateTo = "" then Date.ToText(Date.From(DateTimeZone.UtcNow()), [Format = "yyyy-MM-dd"]) else DateTo,
    Window =
        if Start < LockedScopeStart then error Error.Record("OpenAQ.Scope", "DateFrom is before the locked scope start " & LockedScopeStart, [DateFrom = Start])
        else if Start > End then error Error.Record("OpenAQ.Scope", "DateFrom is after DateTo", [DateFrom = Start, DateTo = End])
        else [From = Start, To = End]
in
    Window;

shared PollutantParameters = "pm25,pm10,no2,o3,so2,co" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = "Text"];

shared OpenAQGetAllPages = (relativePath as text, query as record) as list =>
    let
        PageLimit = 1000,
        Throttle = #duration(0, 0, 0, 1.2),
        RateLimitBackoff = #duration(0, 0, 1, 0),
        Request = (page as number, isRetry as logical) as binary =>
            Web.Contents(
                "https://api.openaq.org",
                [
                    RelativePath = relativePath,
                    Query = query & [limit = Text.From(PageLimit), page = Text.From(page)],
                    Headers = [#"X-API-Key" = OpenAQApiKey, Accept = "application/json"],
                    ManualStatusHandling = {429},
                    IsRetry = isRetry
                ]
            ),
        StatusOf = (response as binary) as number => Value.Metadata(response)[Response.Status],
        GetPage = (page as number) as list =>
            let
                FirstTry = Function.InvokeAfter(() => Request(page, false), Throttle),
                Response = if StatusOf(FirstTry) = 429 then Function.InvokeAfter(() => Request(page, true), RateLimitBackoff) else FirstTry
            in
                if StatusOf(Response) = 429
                then error Error.Record("OpenAQ.RateLimited", "HTTP 429 persisted after one retry", [path = relativePath, page = page])
                else Json.Document(Response)[results],
        Pages = List.Generate(
            () => [Page = 1, Results = GetPage(1)],
            each [Results] <> null,
            each [Page = [Page] + 1, Results = if List.Count([Results]) < PageLimit then null else GetPage([Page] + 1)],
            each [Results]
        )
    in
        List.Combine(Pages);

shared openaq_locations_raw = let
    Records = OpenAQGetAllPages("v3/locations", [bbox = NycBoundingBox]),
    AsTable = Table.FromList(Records, Splitter.SplitByNothing(), {"location"}),
    Expanded = Table.ExpandRecordColumn(AsTable, "location", {"id", "name", "locality", "timezone", "country", "provider", "owner", "isMobile", "isMonitor", "coordinates", "datetimeFirst", "datetimeLast", "sensors"}),
    ExpandedCountry = Table.ExpandRecordColumn(Expanded, "country", {"code", "name"}, {"country_code", "country_name"}),
    ExpandedProvider = Table.ExpandRecordColumn(ExpandedCountry, "provider", {"name"}, {"provider_name"}),
    ExpandedOwner = Table.ExpandRecordColumn(ExpandedProvider, "owner", {"name"}, {"owner_name"}),
    ExpandedCoordinates = Table.ExpandRecordColumn(ExpandedOwner, "coordinates", {"latitude", "longitude"}),
    ExpandedFirst = Table.ExpandRecordColumn(ExpandedCoordinates, "datetimeFirst", {"utc"}, {"datetime_first_utc"}),
    ExpandedLast = Table.ExpandRecordColumn(ExpandedFirst, "datetimeLast", {"utc"}, {"datetime_last_utc"}),
    Renamed = Table.RenameColumns(ExpandedLast, {{"id", "location_id"}, {"name", "location_name"}, {"isMobile", "is_mobile"}, {"isMonitor", "is_monitor"}}),
    ActiveInWindow = Table.SelectRows(
        Renamed,
        each [datetime_first_utc] <> null
            and [datetime_last_utc] <> null
            and Text.Start([datetime_first_utc], 10) <= OpenAQWindow[To]
            and Text.Start([datetime_last_utc], 10) >= OpenAQWindow[From]
    )
in
    ActiveInWindow;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_locations_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_locations = let
    Source = openaq_locations_raw,
    Selected = Table.SelectColumns(Source, {"location_id", "location_name", "locality", "timezone", "country_code", "country_name", "provider_name", "owner_name", "is_mobile", "is_monitor", "latitude", "longitude", "datetime_first_utc", "datetime_last_utc"}),
    Typed = Table.TransformColumnTypes(
        Selected,
        {
            {"location_id", Int64.Type},
            {"location_name", type text},
            {"locality", type text},
            {"timezone", type text},
            {"country_code", type text},
            {"country_name", type text},
            {"provider_name", type text},
            {"owner_name", type text},
            {"is_mobile", type logical},
            {"is_monitor", type logical},
            {"latitude", type number},
            {"longitude", type number},
            {"datetime_first_utc", type text},
            {"datetime_last_utc", type text}
        }
    ),
    WithIngestedAt = Table.AddColumn(Typed, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
in
    WithIngestedAt;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_sensors_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_sensors = let
    WantedParameters = List.Select(List.Transform(Text.Split(PollutantParameters ?? "", ","), Text.Trim), each _ <> ""),
    Source = Table.SelectColumns(openaq_locations_raw, {"location_id", "sensors"}),
    ExpandedList = Table.ExpandListColumn(Source, "sensors"),
    NonEmpty = Table.SelectRows(ExpandedList, each [sensors] <> null),
    ExpandedSensor = Table.ExpandRecordColumn(NonEmpty, "sensors", {"id", "name", "parameter"}, {"sensor_id", "sensor_name", "parameter"}),
    ExpandedParameter = Table.ExpandRecordColumn(ExpandedSensor, "parameter", {"id", "name", "units", "displayName"}, {"parameter_id", "parameter_name", "parameter_units", "parameter_display_name"}),
    Wanted = Table.SelectRows(ExpandedParameter, each List.IsEmpty(WantedParameters) or List.Contains(WantedParameters, [parameter_name])),
    Typed = Table.TransformColumnTypes(
        Wanted,
        {
            {"sensor_id", Int64.Type},
            {"sensor_name", type text},
            {"location_id", Int64.Type},
            {"parameter_id", Int64.Type},
            {"parameter_name", type text},
            {"parameter_units", type text},
            {"parameter_display_name", type text}
        }
    ),
    WithIngestedAt = Table.AddColumn(Typed, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
in
    WithIngestedAt;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_sensor_days_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_sensor_days = let
    Sensors = Table.SelectColumns(bronze_openaq_sensors, {"sensor_id", "location_id"}),
    WithDays = Table.AddColumn(
        Sensors,
        "day",
        each OpenAQGetAllPages("v3/sensors/" & Text.From([sensor_id]) & "/days", [date_from = OpenAQWindow[From], date_to = OpenAQWindow[To]]),
        type list
    ),
    ExpandedList = Table.ExpandListColumn(WithDays, "day"),
    NonEmpty = Table.SelectRows(ExpandedList, each [day] <> null),
    ExpandedDay = Table.ExpandRecordColumn(NonEmpty, "day", {"value", "parameter", "period", "summary", "coverage", "flagInfo"}),
    ExpandedParameter = Table.ExpandRecordColumn(ExpandedDay, "parameter", {"name", "units"}, {"parameter_name", "parameter_units"}),
    ExpandedPeriod = Table.ExpandRecordColumn(ExpandedParameter, "period", {"datetimeFrom", "datetimeTo"}),
    ExpandedFrom = Table.ExpandRecordColumn(ExpandedPeriod, "datetimeFrom", {"utc", "local"}, {"datetime_from_utc", "datetime_from_local"}),
    ExpandedTo = Table.ExpandRecordColumn(ExpandedFrom, "datetimeTo", {"utc"}, {"datetime_to_utc"}),
    ExpandedSummary = Table.ExpandRecordColumn(ExpandedTo, "summary", {"min", "q25", "median", "q75", "max", "avg", "sd"}),
    ExpandedCoverage = Table.ExpandRecordColumn(ExpandedSummary, "coverage", {"observedCount", "expectedCount", "percentComplete"}, {"observed_count", "expected_count", "percent_complete"}),
    ExpandedFlags = Table.ExpandRecordColumn(ExpandedCoverage, "flagInfo", {"hasFlags"}, {"has_flags"}),
    WithDate = Table.AddColumn(ExpandedFlags, "date_local", each Date.FromText(Text.Start([datetime_from_local], 10)), type date),
    Typed = Table.TransformColumnTypes(
        WithDate,
        {
            {"sensor_id", Int64.Type},
            {"location_id", Int64.Type},
            {"value", type number},
            {"parameter_name", type text},
            {"parameter_units", type text},
            {"datetime_from_utc", type text},
            {"datetime_from_local", type text},
            {"datetime_to_utc", type text},
            {"min", type number},
            {"q25", type number},
            {"median", type number},
            {"q75", type number},
            {"max", type number},
            {"avg", type number},
            {"sd", type number},
            {"observed_count", Int64.Type},
            {"expected_count", Int64.Type},
            {"percent_complete", type number},
            {"has_flags", type logical}
        }
    ),
    WithIngestedAt = Table.AddColumn(Typed, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
in
    WithIngestedAt;

shared bronze_openaq_locations_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "."]}[Data],
    Navigation_2 = Navigation_1{[lakehouseName = "lh_bronze"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_openaq_locations", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

shared bronze_openaq_sensors_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "."]}[Data],
    Navigation_2 = Navigation_1{[lakehouseName = "lh_bronze"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_openaq_sensors", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;

shared bronze_openaq_sensor_days_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "."]}[Data],
    Navigation_2 = Navigation_1{[lakehouseName = "lh_bronze"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_openaq_sensor_days", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
