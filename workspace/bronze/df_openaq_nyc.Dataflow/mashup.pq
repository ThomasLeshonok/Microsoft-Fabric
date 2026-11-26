section Section1;

shared OpenAQApiKey = "" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = "Text"];

shared NycBoundingBox = "-74.2591,40.4774,-73.7002,40.9162" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = "Text"];

shared LookbackHours = "3" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = "Text"];

shared OpenAQRunWindow = let
    Hours = Number.FromText(LookbackHours, "en-US"),
    Window =
        if Hours < 1 then error Error.Record("OpenAQ.Lookback", "LookbackHours must be at least 1", [LookbackHours = LookbackHours])
        else [End = DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), Lookback = #duration(0, Hours, 0, 0)]
in
    Window;

shared OpenAQUtcFromText = (moment as nullable text) as nullable datetime =>
    if moment = null then null else DateTimeZone.RemoveZone(DateTimeZone.ToUtc(DateTimeZone.FromText(moment)));

shared OpenAQBronzeTable = (tableName as text) as nullable table =>
    let
        Lakehouse = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = true]){[workspaceId = "."]}[Data]{[lakehouseName = "lh_bronze"]}[Data]
    in
        Lakehouse{[Id = tableName, ItemKind = "Table"]}?[Data]?;

shared ExcludedParameters = "temperature,relativehumidity" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = "Text"];

shared OpenAQGetAllPages = (relativePath as text, query as record) as list =>
    let
        PageLimit = 1000,
        Throttle = #duration(0, 0, 0, 1.2),
        RateLimitBackoff = #duration(0, 0, 1, 0),
        RetryableStatuses = {429, 500, 502, 503, 504},
        MaxAttempts = 4,
        ServerErrorBackoff = (attempt as number) as duration => #duration(0, 0, 0, 15 * attempt),
        Request = (page as number, isRetry as logical) as binary =>
            Web.Contents(
                "https://api.openaq.org",
                [
                    RelativePath = relativePath,
                    Query = query & [limit = Text.From(PageLimit), page = Text.From(page)],
                    Headers = [#"X-API-Key" = OpenAQApiKey, Accept = "application/json"],
                    ManualStatusHandling = RetryableStatuses,
                    IsRetry = isRetry
                ]
            ),
        StatusOf = (response as binary) as nullable number => Record.FieldOrDefault(Value.Metadata(response), "Response.Status", null),
        GetResults = (page as number, attempt as number, delay as duration) as list =>
            let
                Response = Function.InvokeAfter(() => Request(page, attempt > 1), delay),
                Status = StatusOf(Response),
                Parsed = try Json.Document(Response)[results],
                Retryable = List.Contains(RetryableStatuses, Status) or (Status = null and Parsed[HasError])
            in
                if not Retryable then (if Parsed[HasError] then error Parsed[Error] else Parsed[Value])
                else if attempt >= MaxAttempts then
                    if Status = 429
                    then error Error.Record("OpenAQ.RateLimited", "HTTP 429 persisted after " & Text.From(MaxAttempts) & " attempts", [path = relativePath, page = page])
                    else if Status = null then error Parsed[Error]
                    else error Error.Record("OpenAQ.ServerError", "HTTP " & Text.From(Status) & " persisted after " & Text.From(MaxAttempts) & " attempts", [path = relativePath, page = page, status = Status])
                else @GetResults(page, attempt + 1, if Status = 429 then RateLimitBackoff else ServerErrorBackoff(attempt)),
        GetPage = (page as number) as list => GetResults(page, 1, Throttle),
        Pages = List.Generate(
            () => [Page = 1, Results = GetPage(1)],
            each [Results] <> null,
            each [Page = [Page] + 1, Results = if List.Count([Results]) < PageLimit then null else GetPage([Page] + 1)],
            each [Results]
        )
    in
        List.Combine(Pages);

shared OpenAQWithoutExistingKeys = (fetched as table, existing as nullable table, key as list) as table =>
    if existing = null then fetched
    else Table.RemoveColumns(Table.NestedJoin(fetched, key, Table.SelectColumns(existing, key), key, "existing", JoinKind.LeftAnti), {"existing"});

shared OpenAQUtcToText = (moment as datetime) as text => DateTime.ToText(moment, [Format = "yyyy-MM-dd'T'HH:mm:ss'Z'", Culture = "en-US"]);

shared OpenAQMonthWindows = (fromMoment as datetime, toMoment as datetime) as list =>
    let
        NextMonth = (moment as datetime) as datetime => DateTime.From(Date.StartOfMonth(Date.AddMonths(Date.From(moment), 1))),
        Starts = List.Generate(() => fromMoment, each _ < toMoment, each NextMonth(_))
    in
        List.Transform(Starts, each [datetime_from = OpenAQUtcToText(_), datetime_to = OpenAQUtcToText(List.Min({NextMonth(_), toMoment}))]);

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
    Renamed = Table.RenameColumns(ExpandedLast, {{"id", "location_id"}, {"name", "location_name"}, {"isMobile", "is_mobile"}, {"isMonitor", "is_monitor"}})
in
    Renamed;

shared openaq_sensors_raw = let
    ExcludedParameterNames = List.Select(List.Transform(Text.Split(ExcludedParameters ?? "", ","), Text.Trim), each _ <> ""),
    Source = Table.SelectColumns(openaq_locations_raw, {"location_id", "sensors"}),
    ExpandedList = Table.ExpandListColumn(Source, "sensors"),
    NonEmpty = Table.SelectRows(ExpandedList, each [sensors] <> null),
    ExpandedSensor = Table.ExpandRecordColumn(NonEmpty, "sensors", {"id", "name", "parameter"}, {"sensor_id", "sensor_name", "parameter"}),
    ExpandedParameter = Table.ExpandRecordColumn(ExpandedSensor, "parameter", {"id", "name", "units", "displayName"}, {"parameter_id", "parameter_name", "parameter_units", "parameter_display_name"})
in
    Table.SelectRows(ExpandedParameter, each not List.Contains(ExcludedParameterNames, [parameter_name]));

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_locations_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "location_name", DestinationColumnName = "location_name"], [SourceColumnName = "locality", DestinationColumnName = "locality"], [SourceColumnName = "timezone", DestinationColumnName = "timezone"], [SourceColumnName = "country_code", DestinationColumnName = "country_code"], [SourceColumnName = "country_name", DestinationColumnName = "country_name"], [SourceColumnName = "provider_name", DestinationColumnName = "provider_name"], [SourceColumnName = "owner_name", DestinationColumnName = "owner_name"], [SourceColumnName = "is_mobile", DestinationColumnName = "is_mobile"], [SourceColumnName = "is_monitor", DestinationColumnName = "is_monitor"], [SourceColumnName = "latitude", DestinationColumnName = "latitude"], [SourceColumnName = "longitude", DestinationColumnName = "longitude"], [SourceColumnName = "datetime_first_utc", DestinationColumnName = "datetime_first_utc"], [SourceColumnName = "datetime_last_utc", DestinationColumnName = "datetime_last_utc"], [SourceColumnName = "ingested_at", DestinationColumnName = "ingested_at"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_locations = let
    Source = openaq_locations_raw,
    Selected = Table.SelectColumns(Source, {"location_id", "location_name", "locality", "timezone", "country_code", "country_name", "provider_name", "owner_name", "is_mobile", "is_monitor", "latitude", "longitude", "datetime_first_utc", "datetime_last_utc"}),
    Typed = Table.TransformColumnTypes(
        Selected,
        {
            {"location_id", Int64.Type},
            {"location_name", type nullable text},
            {"locality", type nullable text},
            {"timezone", type nullable text},
            {"country_code", type nullable text},
            {"country_name", type nullable text},
            {"provider_name", type nullable text},
            {"owner_name", type nullable text},
            {"is_mobile", type nullable logical},
            {"is_monitor", type nullable logical},
            {"latitude", type nullable number},
            {"longitude", type nullable number},
            {"datetime_first_utc", type nullable text},
            {"datetime_last_utc", type nullable text}
        }
    ),
    NewRows = OpenAQWithoutExistingKeys(Typed, OpenAQBronzeTable("bronze_openaq_locations"), {"location_id"}),
    WithIngestedAt = Table.AddColumn(NewRows, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
in
    WithIngestedAt;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_sensors_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "sensor_name", DestinationColumnName = "sensor_name"], [SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "parameter_id", DestinationColumnName = "parameter_id"], [SourceColumnName = "parameter_name", DestinationColumnName = "parameter_name"], [SourceColumnName = "parameter_units", DestinationColumnName = "parameter_units"], [SourceColumnName = "parameter_display_name", DestinationColumnName = "parameter_display_name"], [SourceColumnName = "ingested_at", DestinationColumnName = "ingested_at"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_sensors = let
    Source = openaq_sensors_raw,
    Typed = Table.TransformColumnTypes(
        Source,
        {
            {"sensor_id", Int64.Type},
            {"sensor_name", type nullable text},
            {"location_id", Int64.Type},
            {"parameter_id", Int64.Type},
            {"parameter_name", type nullable text},
            {"parameter_units", type nullable text},
            {"parameter_display_name", type nullable text}
        }
    ),
    NewRows = OpenAQWithoutExistingKeys(Typed, OpenAQBronzeTable("bronze_openaq_sensors"), {"sensor_id"}),
    WithIngestedAt = Table.AddColumn(NewRows, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
in
    WithIngestedAt;

shared OpenAQFetchPlan = (tableName as text, lookback as duration, fetchTo as datetime) as table =>
    let
        CatchUpLimit = #duration(30, 0, 0, 0),
        Existing = OpenAQBronzeTable(tableName),
        Watermarks =
            if Existing = null then #table(type table [sensor_id = Int64.Type, watermark_utc = nullable text], {})
            else Table.Group(Existing, {"sensor_id"}, {{"watermark_utc", each List.Max([datetime_to_utc]), type nullable text}}),
        Locations = Table.RenameColumns(Table.SelectColumns(openaq_locations_raw, {"location_id", "datetime_last_utc"}), {"location_id", "last_location_id"}),
        Sensors = Table.Join(Table.SelectColumns(openaq_sensors_raw, {"sensor_id", "location_id"}), "location_id", Locations, "last_location_id"),
        WithWatermark = Table.Join(Sensors, "sensor_id", Table.RenameColumns(Watermarks, {"sensor_id", "watermark_sensor_id"}), "watermark_sensor_id", JoinKind.LeftOuter),
        Parsed = Table.TransformColumns(WithWatermark, {{"datetime_last_utc", OpenAQUtcFromText, type nullable datetime}, {"watermark_utc", OpenAQUtcFromText, type nullable datetime}}),
        WithFrom = Table.AddColumn(
            Parsed,
            "fetch_from",
            each if [watermark_utc] = null then fetchTo - CatchUpLimit else List.Max({[watermark_utc] - lookback, fetchTo - CatchUpLimit}),
            type datetime
        ),
        HasNewReadings = Table.SelectRows(WithFrom, each [datetime_last_utc] <> null and [datetime_last_utc] > ([watermark_utc] ?? [fetch_from])),
        WithTo = Table.AddColumn(HasNewReadings, "fetch_to", each fetchTo, type datetime)
    in
        Table.SelectColumns(WithTo, {"sensor_id", "location_id", "fetch_from", "fetch_to"});

shared OpenAQOnlyNewRows = (tableName as text, fetched as table, fetchPlan as table) as table =>
    let
        PeriodSlack = #duration(2, 0, 0, 0),
        Existing = OpenAQBronzeTable(tableName),
        Key = {"sensor_id", "datetime_from_utc"},
        FetchedSince = List.Min(fetchPlan[fetch_from]),
        KeysFrom = if FetchedSince = null then null else OpenAQUtcToText(FetchedSince - PeriodSlack),
        RecentExisting = if Existing = null then null else Table.SelectRows(Existing, each [datetime_from_utc] >= KeysFrom)
    in
        OpenAQWithoutExistingKeys(fetched, RecentExisting, Key);

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_sensor_days_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "parameter_name", DestinationColumnName = "parameter_name"], [SourceColumnName = "parameter_units", DestinationColumnName = "parameter_units"], [SourceColumnName = "datetime_from_utc", DestinationColumnName = "datetime_from_utc"], [SourceColumnName = "datetime_from_local", DestinationColumnName = "datetime_from_local"], [SourceColumnName = "datetime_to_utc", DestinationColumnName = "datetime_to_utc"], [SourceColumnName = "min", DestinationColumnName = "min"], [SourceColumnName = "q25", DestinationColumnName = "q25"], [SourceColumnName = "median", DestinationColumnName = "median"], [SourceColumnName = "q75", DestinationColumnName = "q75"], [SourceColumnName = "max", DestinationColumnName = "max"], [SourceColumnName = "avg", DestinationColumnName = "avg"], [SourceColumnName = "sd", DestinationColumnName = "sd"], [SourceColumnName = "observed_count", DestinationColumnName = "observed_count"], [SourceColumnName = "expected_count", DestinationColumnName = "expected_count"], [SourceColumnName = "percent_complete", DestinationColumnName = "percent_complete"], [SourceColumnName = "has_flags", DestinationColumnName = "has_flags"], [SourceColumnName = "date_local", DestinationColumnName = "date_local"], [SourceColumnName = "ingested_at", DestinationColumnName = "ingested_at"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_sensor_days = let
    TableName = "bronze_openaq_sensor_days",
    Window = OpenAQRunWindow,
    SettledUntil = Window[End] - Window[Lookback],
    Plan = OpenAQFetchPlan(TableName, #duration(0, 0, 0, 0), SettledUntil),
    WithFullDayPending = Table.SelectRows(Plan, each [fetch_to] - [fetch_from] >= #duration(1, 0, 0, 0)),
    AsDateText = (moment as datetime, offsetDays as number) as text => Date.ToText(Date.AddDays(Date.From(moment), offsetDays), [Format = "yyyy-MM-dd"]),
    EndsInWindow = (day as record, sensor as record) as logical =>
        let
            DayEnd = OpenAQUtcFromText(day[period][datetimeTo][utc])
        in
            DayEnd > sensor[fetch_from] and DayEnd <= sensor[fetch_to],
    WithDays = Table.AddColumn(
        WithFullDayPending,
        "day",
        (sensor) => List.Select(
            OpenAQGetAllPages(
                "v3/sensors/" & Text.From(sensor[sensor_id]) & "/days",
                [date_from = AsDateText(sensor[fetch_from], -1), date_to = AsDateText(sensor[fetch_to], 1)]
            ),
            (day) => EndsInWindow(day, sensor)
        ),
        type list
    ),
    ExpandedList = Table.ExpandListColumn(Table.SelectColumns(WithDays, {"sensor_id", "location_id", "day"}), "day"),
    NonEmpty = Table.SelectRows(ExpandedList, each [day] <> null),
    ExpandedDay = Table.ExpandRecordColumn(NonEmpty, "day", {"value", "parameter", "period", "summary", "coverage", "flagInfo"}),
    ExpandedParameter = Table.ExpandRecordColumn(ExpandedDay, "parameter", {"name", "units"}, {"parameter_name", "parameter_units"}),
    ExpandedPeriod = Table.ExpandRecordColumn(ExpandedParameter, "period", {"datetimeFrom", "datetimeTo"}),
    ExpandedFrom = Table.ExpandRecordColumn(ExpandedPeriod, "datetimeFrom", {"utc", "local"}, {"datetime_from_utc", "datetime_from_local"}),
    ExpandedTo = Table.ExpandRecordColumn(ExpandedFrom, "datetimeTo", {"utc"}, {"datetime_to_utc"}),
    ExpandedSummary = Table.ExpandRecordColumn(ExpandedTo, "summary", {"min", "q25", "median", "q75", "max", "avg", "sd"}),
    ExpandedCoverage = Table.ExpandRecordColumn(ExpandedSummary, "coverage", {"observedCount", "expectedCount", "percentComplete"}, {"observed_count", "expected_count", "percent_complete"}),
    ExpandedFlags = Table.ExpandRecordColumn(ExpandedCoverage, "flagInfo", {"hasFlags"}, {"has_flags"}),
    WithDate = Table.AddColumn(ExpandedFlags, "date_local", each Date.FromText(Text.Start([datetime_from_local], 10)), type nullable date),
    Typed = Table.TransformColumnTypes(
        WithDate,
        {
            {"sensor_id", Int64.Type},
            {"location_id", Int64.Type},
            {"value", type nullable number},
            {"parameter_name", type nullable text},
            {"parameter_units", type nullable text},
            {"datetime_from_utc", type nullable text},
            {"datetime_from_local", type nullable text},
            {"datetime_to_utc", type nullable text},
            {"min", type nullable number},
            {"q25", type nullable number},
            {"median", type nullable number},
            {"q75", type nullable number},
            {"max", type nullable number},
            {"avg", type nullable number},
            {"sd", type nullable number},
            {"observed_count", Int64.Type},
            {"expected_count", Int64.Type},
            {"percent_complete", type nullable number},
            {"has_flags", type nullable logical}
        }
    ),
    NewRows = OpenAQOnlyNewRows(TableName, Typed, Plan),
    WithIngestedAt = Table.AddColumn(NewRows, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
in
    WithIngestedAt;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze_openaq_sensor_measurements_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "sensor_id", DestinationColumnName = "sensor_id"], [SourceColumnName = "location_id", DestinationColumnName = "location_id"], [SourceColumnName = "value", DestinationColumnName = "value"], [SourceColumnName = "parameter_name", DestinationColumnName = "parameter_name"], [SourceColumnName = "parameter_units", DestinationColumnName = "parameter_units"], [SourceColumnName = "period_label", DestinationColumnName = "period_label"], [SourceColumnName = "period_interval", DestinationColumnName = "period_interval"], [SourceColumnName = "datetime_from_utc", DestinationColumnName = "datetime_from_utc"], [SourceColumnName = "datetime_from_local", DestinationColumnName = "datetime_from_local"], [SourceColumnName = "datetime_to_utc", DestinationColumnName = "datetime_to_utc"], [SourceColumnName = "datetime_to_local", DestinationColumnName = "datetime_to_local"], [SourceColumnName = "has_flags", DestinationColumnName = "has_flags"], [SourceColumnName = "ingested_at", DestinationColumnName = "ingested_at"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared bronze_openaq_sensor_measurements = let
    TableName = "bronze_openaq_sensor_measurements",
    Window = OpenAQRunWindow,
    Plan = OpenAQFetchPlan(TableName, Window[Lookback], Window[End]),
    WithWindows = Table.AddColumn(
        Plan,
        "window",
        each OpenAQMonthWindows([fetch_from], [fetch_to]),
        type list
    ),
    ExpandedWindows = Table.ExpandListColumn(WithWindows, "window"),
    NonEmptyWindows = Table.SelectRows(ExpandedWindows, each [window] <> null),
    WithMeasurements = Table.AddColumn(
        NonEmptyWindows,
        "measurement",
        each OpenAQGetAllPages("v3/sensors/" & Text.From([sensor_id]) & "/measurements", [window]),
        type list
    ),
    ExpandedList = Table.ExpandListColumn(Table.SelectColumns(WithMeasurements, {"sensor_id", "location_id", "measurement"}), "measurement"),
    NonEmpty = Table.SelectRows(ExpandedList, each [measurement] <> null),
    ExpandedMeasurement = Table.ExpandRecordColumn(NonEmpty, "measurement", {"value", "parameter", "period", "flagInfo"}),
    ExpandedParameter = Table.ExpandRecordColumn(ExpandedMeasurement, "parameter", {"name", "units"}, {"parameter_name", "parameter_units"}),
    ExpandedPeriod = Table.ExpandRecordColumn(ExpandedParameter, "period", {"label", "interval", "datetimeFrom", "datetimeTo"}, {"period_label", "period_interval", "datetimeFrom", "datetimeTo"}),
    ExpandedFrom = Table.ExpandRecordColumn(ExpandedPeriod, "datetimeFrom", {"utc", "local"}, {"datetime_from_utc", "datetime_from_local"}),
    ExpandedTo = Table.ExpandRecordColumn(ExpandedFrom, "datetimeTo", {"utc", "local"}, {"datetime_to_utc", "datetime_to_local"}),
    ExpandedFlags = Table.ExpandRecordColumn(ExpandedTo, "flagInfo", {"hasFlags"}, {"has_flags"}),
    WithoutBoundaryRepeats = Table.Distinct(ExpandedFlags, {"sensor_id", "datetime_from_utc"}),
    Typed = Table.TransformColumnTypes(
        WithoutBoundaryRepeats,
        {
            {"sensor_id", Int64.Type},
            {"location_id", Int64.Type},
            {"value", type nullable number},
            {"parameter_name", type nullable text},
            {"parameter_units", type nullable text},
            {"period_label", type nullable text},
            {"period_interval", type nullable text},
            {"datetime_from_utc", type nullable text},
            {"datetime_from_local", type nullable text},
            {"datetime_to_utc", type nullable text},
            {"datetime_to_local", type nullable text},
            {"has_flags", type nullable logical}
        }
    ),
    NewRows = OpenAQOnlyNewRows(TableName, Typed, Plan),
    WithIngestedAt = Table.AddColumn(NewRows, "ingested_at", each DateTimeZone.RemoveZone(DateTimeZone.UtcNow()), type datetime)
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

shared bronze_openaq_sensor_measurements_DataDestination = let
    Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "."]}[Data],
    Navigation_2 = Navigation_1{[lakehouseName = "lh_bronze"]}[Data],
    TableNavigation = Navigation_2{[Id = "bronze_openaq_sensor_measurements", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
