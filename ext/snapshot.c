#include "config.h"
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"


#ifndef AVRO
PG_FUNCTION_INFO_V1(bottledwater_key_schema);
PG_FUNCTION_INFO_V1(bottledwater_row_schema);
PG_FUNCTION_INFO_V1(bottledwater_frame_schema);
PG_FUNCTION_INFO_V1(bottledwater_export);

static Datum bottledwater_avro_not_supported(PG_FUNCTION_ARGS) {
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("this version of bottledwater was built without AVRO format support")));

    PG_RETURN_NULL();
}

Datum bottledwater_key_schema(PG_FUNCTION_ARGS) {
    return bottledwater_avro_not_supported(fcinfo);
}

Datum bottledwater_row_schema(PG_FUNCTION_ARGS) {
    return bottledwater_avro_not_supported(fcinfo);
}

Datum bottledwater_frame_schema(PG_FUNCTION_ARGS) {
    return bottledwater_avro_not_supported(fcinfo);
}

Datum bottledwater_export(PG_FUNCTION_ARGS) {
    return bottledwater_avro_not_supported(fcinfo);
}
#endif

#ifndef JSON
PG_FUNCTION_INFO_V1(bottledwater_schema_json);
PG_FUNCTION_INFO_V1(bottledwater_export_json);

static Datum bottledwater_json_not_supported(PG_FUNCTION_ARGS) {
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("this version of bottledwater was built without JSON format support")));

    PG_RETURN_NULL();
}

Datum bottledwater_schema_json(PG_FUNCTION_ARGS) {
    return bottledwater_json_not_supported(fcinfo);
}

Datum bottledwater_export_json(PG_FUNCTION_ARGS) {
    return bottledwater_json_not_supported(fcinfo);
}
#endif
