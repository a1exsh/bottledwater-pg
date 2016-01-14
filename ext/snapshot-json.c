#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

#include "format-json.h"
#include "oid_util.h"

typedef struct {
    MemoryContext memcontext;
    Portal cursor;
    TupleDesc tupdesc;
    StringInfoData template;
    int reset_len;
} export_json_state;

static char *get_attr_default_expression(Oid reloid, int16 attnum);


PG_FUNCTION_INFO_V1(bottledwater_schema_json);

Datum bottledwater_schema_json(PG_FUNCTION_ARGS) {
    const char *relname;
    const char *relnamespace;
    Oid reloid;
    Oid schemaoid;
    Relation rel, pkey_index;
    TupleDesc desc;
    int i;
    bool need_sep = false;
    char *def_expr;
    StringInfoData result;

    if (PG_ARGISNULL(0)) {
        elog(ERROR, "bottledwater_schema_json: 'relname' cannot be null");
    }
    relname = text_to_cstring(PG_GETARG_TEXT_P(0));

    if (PG_ARGISNULL(1)) {
        reloid = RelnameGetRelid(relname);
    } else {
        relnamespace = text_to_cstring(PG_GETARG_TEXT_P(1));
        schemaoid = LookupExplicitNamespace(relnamespace, false);
        reloid = get_relname_relid(relname, schemaoid);
    }
    if (reloid == InvalidOid) {
        elog(ERROR, "bottledwater_schema_json: relation not found");
    }

    rel = relation_open(reloid, AccessShareLock);
    desc = RelationGetDescr(rel);

    initStringInfo(&result);

    appendStringInfoString(&result, "{ \"relname\": ");
    escape_json(&result, RelationGetRelationName(rel));

    appendStringInfoString(&result, ", \"relnamespace\": ");
    escape_json(&result, get_namespace_name(RelationGetNamespace(rel)));

    pkey_index = table_key_index(rel);
    if (pkey_index) {
        appendStringInfoString(&result, ", \"key\": ");
        output_json_relation_key(&result, pkey_index);

        relation_close(pkey_index, AccessShareLock);
    }

    appendStringInfoString(&result, ", \"attributes\": [");

    for (i = 0; i < desc->natts; i++) {
        Form_pg_attribute attr = desc->attrs[i];

        if (attr->attisdropped) {
            continue;
        }
        if (need_sep) {
            appendStringInfoString(&result, ", ");
        }
        need_sep = true;

        appendStringInfoString(&result, "{ \"name\": ");
        escape_json(&result, NameStr(attr->attname));

        appendStringInfoString(&result, ", \"type\": ");
        if (attr->atttypmod != -1) {
            escape_json(&result, format_type_with_typemod(attr->atttypid, attr->atttypmod));
        } else {
            escape_json(&result, format_type_be(attr->atttypid));
        }

        appendStringInfo(&result, ", \"notnull\": %s",
                         attr->attnotnull ? "true" : "false");

        if (attr->atthasdef) {
            def_expr = get_attr_default_expression(reloid, i+1);
            if (def_expr != NULL) {
                appendStringInfoString(&result, ", \"default\": ");
                escape_json(&result, def_expr);
            }
        }

        appendStringInfoString(&result, " }");
    }
    appendStringInfoString(&result, "] }");

    relation_close(rel, AccessShareLock);
    PG_RETURN_TEXT_P(cstring_to_text_with_len(result.data, result.len));
}

static char *get_attr_default_expression(Oid reloid, int16 attnum) {
    Relation attrdefDesc;
    ScanKeyData skey[2];
    SysScanDesc adscan;
    HeapTuple tup;
    Datum adsrc;
    bool isnull = false;
    char *result;

    attrdefDesc = heap_open(AttrDefaultRelationId, AccessShareLock);

    ScanKeyInit(&skey[0],
                Anum_pg_attrdef_adrelid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(reloid));

    ScanKeyInit(&skey[1],
                Anum_pg_attrdef_adnum,
                BTEqualStrategyNumber, F_INT2EQ,
                Int16GetDatum(attnum));

    adscan = systable_beginscan(attrdefDesc, AttrDefaultIndexId,
                                true, NULL, 2, skey);

    tup = systable_getnext(adscan);

    if (!HeapTupleIsValid(tup)) {
        elog(ERROR, "bottledwater_schema_json: could not find tuple for adrelid %u, adnum %d",
             reloid, attnum);
    }

    adsrc = heap_getattr(tup, 4 /* adsrc column */, RelationGetDescr(attrdefDesc), &isnull);
    if (isnull || !adsrc) {
        result = NULL;
    } else {
        result = text_to_cstring(DatumGetTextP(adsrc));
    }

    systable_endscan(adscan);
    heap_close(attrdefDesc, AccessShareLock);

    return result;
}


PG_FUNCTION_INFO_V1(bottledwater_export_json);

Datum bottledwater_export_json(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    MemoryContext oldcontext;
    const char *relident;
    int ret;
    export_json_state *state;
    SPIPlanPtr plan;
    StringInfoData query;
    CachedPlanSource *plansrc;
    Oid reloid;
    Relation rel, pkey_index;
    text *result;

    oldcontext = CurrentMemoryContext;

    if (SRF_IS_FIRSTCALL()) {
        if (PG_ARGISNULL(0)) {
            elog(ERROR, "bottledwater_export_json: 'relname' cannot be null");
        }

        funcctx = SRF_FIRSTCALL_INIT();

        /* Initialize the SPI (server programming interface), which allows us to make SQL queries
         * within this function. Note SPI_connect() switches to its own memory context, but we
         * actually want to use multi_call_memory_ctx, so we call SPI_connect() first. */
        if ((ret = SPI_connect()) < 0) {
            elog(ERROR, "bottledwater_export_json: SPI_connect returned %d", ret);
        }

        /* Things allocated in this memory context will live until SRF_RETURN_DONE(). */
        MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        state = palloc(sizeof(export_json_state));

        state->memcontext = AllocSetContextCreate(CurrentMemoryContext,
                                                  "bottledwater_export_json per-tuple context",
                                                  ALLOCSET_DEFAULT_MINSIZE,
                                                  ALLOCSET_DEFAULT_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE);
        funcctx->user_fctx = state;

        /* we want the template string to live in the multi-call context specifically */
        initStringInfo(&state->template);

        initStringInfo(&query);
        appendStringInfoString(&query, "SELECT * FROM ");

        /* Exclude data from children tables? */
        if (PG_GETARG_BOOL(2))
            appendStringInfoString(&query, " ONLY");

        if (PG_ARGISNULL(1)) {
            relident = quote_identifier(text_to_cstring(PG_GETARG_TEXT_P(0)));
        } else {
            relident = quote_qualified_identifier(text_to_cstring(PG_GETARG_TEXT_P(1)),
                                                  text_to_cstring(PG_GETARG_TEXT_P(0)));
        }
        appendStringInfoString(&query, relident);

        plan = SPI_prepare_cursor(query.data, 0, NULL, CURSOR_OPT_NO_SCROLL);
        if (!plan) {
            elog(ERROR, "bottledwater_export_json: SPI_prepare_cursor failed with error %d", SPI_result);
        }
        state->cursor = SPI_cursor_open(NULL, plan, NULL, NULL, true);

        /* We need to figure out the oid of the relation we're streaming tuples from. */
        plansrc = (CachedPlanSource *) SPI_plan_get_plan_sources(plan)->head->data.ptr_value;
        reloid = plansrc->relationOids->head->data.oid_value;

        rel = RelationIdGetRelation(reloid);
        state->tupdesc = RelationGetDescr(rel);

        /* make a JSON template for all output to base on */
        output_json_common_header(&state->template, "INSERT", 0, 0, rel);

        pkey_index = table_key_index(rel);
        if (pkey_index) {
            appendStringInfoString(&state->template, ", \"key\": ");
            output_json_relation_key(&state->template, pkey_index);

            relation_close(pkey_index, AccessShareLock);
        }

        appendStringInfoString(&state->template, ", \"newtuple\": ");

        /* save the reset position at end of template */
        state->reset_len = state->template.len;

        RelationClose(rel);
    }

    funcctx = SRF_PERCALL_SETUP();

    state = (export_json_state *) funcctx->user_fctx;
    SPI_cursor_fetch(state->cursor, true, 1);
    if (SPI_processed == 0) {
        SPI_cursor_close(state->cursor);
        SPI_freetuptable(SPI_tuptable);
        SPI_finish();

        SRF_RETURN_DONE(funcctx);
    }
    if (SPI_processed != 1) {
        elog(ERROR, "bottledwater_export_json: expected exactly 1 row from cursor, but got %d rows", SPI_processed);
    }

    /* SPI_cursor_fetch() leaves us in the SPI mem. context */
    MemoryContextSwitchTo(state->memcontext);

    /* clear any prior tuple result memory */
    MemoryContextReset(state->memcontext);

    /* reset template length before spitting next tuple */
    state->template.len = state->reset_len;

    /* NB: using descriptor obtained from the relation to avoid registering a record type */
    output_json_tuple(&state->template, SPI_tuptable->vals[0], state->tupdesc);

    appendStringInfoString(&state->template, " }");

    /* finally allocate the result, while still in the per-tuple context */
    result = cstring_to_text_with_len(state->template.data, state->template.len);

    MemoryContextSwitchTo(oldcontext);

    /* don't forget to clear the SPI temp context */
    SPI_freetuptable(SPI_tuptable);

    SRF_RETURN_NEXT(funcctx, PointerGetDatum(result));
}
