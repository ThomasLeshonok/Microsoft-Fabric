# Lineage

How each source travels from its API or file server through bronze (`lh_bronze`), silver (`lh_silver`) and gold
(`wh_gold`) into the `sm_gold` semantic model and the pages of `rpt_nyc_insights`. Column level detail is in
[data_dictionary.md](data_dictionary.md).

```mermaid
flowchart LR
    subgraph sources[Sources]
        tlc[NYC TLC<br/>d37ci6vzurychx.cloudfront.net]
        ecb[ECB Data Portal<br/>data-api.ecb.europa.eu]
        wb[World Bank API<br/>api.worldbank.org]
        oaq[OpenAQ v3<br/>api.openaq.org]
    end

    subgraph bronze_items[Bronze items]
        pl_taxi[pl_ingest_taxi]
        nb_zip[nb_download_zip_members]
        df_ecb[df_ecb_exchange_rates]
        df_wb[df_worldbank_indicators]
        pl_oaq[pl_ingest_openaq]
        df_oaq[df_openaq_nyc]
        nb_oaq[nb_ingest_openaq]
    end

    subgraph lh_bronze[lh_bronze]
        b_trips[/"Files/trips/nyctaxi_yellow/yyyy/MM/*.parquet"/]
        b_shp[/"Files/nyctaxi_yellow/taxi_zones/taxi_zones .shp .dbf .prj"/]
        b_lookup[(taxi_zone_lookup)]
        b_ecb[(bronze_ecb_exchange_rates)]
        b_wb[(bronze_worldbank_indicators)]
        b_oaq_loc[(bronze_openaq_locations)]
        b_oaq_sen[(bronze_openaq_sensors)]
        b_oaq_day[(bronze_openaq_sensor_days)]
        b_oaq_mea[(bronze_openaq_sensor_measurements)]
        b_oaq_raw[/"Files/OpenAQ/entity/run_date=.../*.jsonl"/]
    end

    subgraph silver_notebooks[Silver notebooks]
        nb_trips[nb_silver_taxi_trips]
        nb_zones[nb_silver_taxi_zones]
        nb_ecb[nb_silver_ecb_exchange_rates]
        nb_wb[nb_silver_worldbank_indicators]
        nb_aq[nb_silver_air_quality]
    end

    subgraph lh_silver[lh_silver]
        s_trips[(silver_taxi_trips)]
        s_zones[(silver_taxi_zones)]
        s_ecb[(silver_ecb_exchange_rates)]
        s_wb[(silver_worldbank_indicators)]
        s_sen[(silver_air_quality_sensors)]
        s_day[(silver_air_quality_daily)]
        s_hour[(silver_air_quality_hourly)]
    end

    subgraph wh_gold[wh_gold via nb_gold_star_schema]
        DimDate[[DimDate]]
        DimHour[[DimHour]]
        DimTripEnd[[DimTripEnd]]
        DimZone[[DimZone]]
        DimFX[[DimFX]]
        DimGDP[[DimGDP]]
        FactTaxiDaily[[FactTaxiDaily]]
        FactTaxiHourly[[FactTaxiHourly]]
        FactAirQualityDaily[[FactAirQualityDaily]]
        FactAirQualityHourly[[FactAirQualityHourly]]
    end

    sm[sm_gold<br/>Direct Lake]

    subgraph report[rpt_nyc_insights]
        p_mob[Mobility]
        p_aq[Air Quality]
        p_corr[Mobility vs Air Quality]
        p_tod[Time of Day]
        p_no2[NO2]
        p_econ[Economic Impact]
    end

    tlc --> pl_taxi --> b_trips
    pl_taxi --> b_lookup
    tlc --> nb_zip --> b_shp
    ecb --> df_ecb --> b_ecb
    wb --> df_wb --> b_wb
    oaq --> pl_oaq
    pl_oaq --> df_oaq
    pl_oaq --> nb_oaq
    df_oaq --> b_oaq_loc & b_oaq_sen & b_oaq_day & b_oaq_mea
    nb_oaq --> b_oaq_loc & b_oaq_sen & b_oaq_day & b_oaq_mea & b_oaq_raw

    b_trips --> nb_trips --> s_trips
    b_lookup --> nb_zones
    b_shp --> nb_zones --> s_zones
    b_ecb --> nb_ecb --> s_ecb
    b_wb --> nb_wb --> s_wb
    b_oaq_loc & b_oaq_sen & b_oaq_day & b_oaq_mea --> nb_aq
    s_zones --> nb_aq
    nb_aq --> s_sen & s_day & s_hour

    s_zones --> DimZone
    s_ecb --> DimFX
    s_wb --> DimGDP
    s_trips --> FactTaxiDaily & FactTaxiHourly
    s_ecb --> FactTaxiDaily
    s_day --> FactAirQualityDaily
    s_hour --> FactAirQualityHourly

    DimDate & DimHour & DimTripEnd & DimZone & DimFX & DimGDP --> sm
    FactTaxiDaily & FactTaxiHourly & FactAirQualityDaily & FactAirQualityHourly --> sm

    sm --> p_mob & p_aq & p_corr & p_tod & p_no2 & p_econ
```

## Gold tables per report page

| Page | Gold tables behind its visuals |
|---|---|
| Mobility | `FactTaxiDaily`, `DimDate`, `DimZone`, `DimTripEnd` |
| Air Quality | `FactAirQualityDaily`, `DimDate`, `DimZone` |
| Mobility vs Air Quality | `FactTaxiDaily`, `FactTaxiHourly`, `FactAirQualityDaily`, `FactAirQualityHourly`, `DimDate`, `DimZone`, `DimTripEnd` |
| Time of Day | `FactTaxiHourly`, `FactAirQualityHourly`, `DimHour`, `DimDate`, `DimZone`, `DimTripEnd` |
| NO2 | `FactAirQualityDaily`, `FactAirQualityHourly`, `FactTaxiHourly`, `DimHour`, `DimDate`, `DimZone` |
| Economic Impact | `FactTaxiDaily`, `FactAirQualityDaily`, `DimFX`, `DimGDP`, `DimDate` |

Notes:

* `DimDate`, `DimHour` and `DimTripEnd` are generated in `nb_gold_star_schema`, not read from silver.
* `nb_ingest_openaq` is the backfill path of `pl_ingest_openaq` (`run_backfill = true`); the scheduled runs use
  `df_openaq_nyc`. Both write the same `bronze_openaq_*` tables, and silver keeps the latest `ingested_at` per key.
* `nb_silver_air_quality` reads `silver_taxi_zones` to place every sensor in a taxi zone, so
  `pl_silver_startup` rebuilds taxi zones first.
* `nb_compact_bronze_tables` compacts the OpenAQ fact tables in place; it changes files, not lineage.

## Fabric lineage view

Fabric draws item level lineage on its own: in the workspace, switch the view to **Lineage** (or open an item's
**Lineage** from its menu) to see the dataflows, pipelines, notebooks, lakehouses, warehouse, semantic model and
report with their upstream and downstream items. It follows the links Fabric knows about (data destinations, pipeline
activities, the Direct Lake connection), so it shows which items depend on which, while this page adds the tables in
between and which report page reads which gold table.
