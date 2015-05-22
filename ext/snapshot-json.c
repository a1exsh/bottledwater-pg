#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "format-json.h"

typedef struct {
    Portal cursor;
    StringInfoData template;
    int reset_len;
} export_json_state;

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
    Relation rel;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        /* Initialize the SPI (server programming interface), which allows us to make SQL queries
         * within this function. Note SPI_connect() switches to its own memory context, but we
         * actually want to use multi_call_memory_ctx, so we call SPI_connect() first. */
        if ((ret = SPI_connect()) < 0) {
            elog(ERROR, "bottledwater_export_json: SPI_connect returned %d", ret);
        }

        /* Things allocated in this memory context will live until SRF_RETURN_DONE(). */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        state = palloc(sizeof(export_json_state));
        if (!state) {
            elog(ERROR, "bottledwater_export_json: cannot allocate SRF state");
        }
        funcctx->user_fctx = state;

        initStringInfo(&query);
        appendStringInfoString(&query, "SELECT * FROM ");

        if (!PG_ARGISNULL(1)) {
            relident = quote_qualified_identifier(text_to_cstring(PG_GETARG_TEXT_P(0)),
                                                  text_to_cstring(PG_GETARG_TEXT_P(1)));
        } else {
            relident = quote_identifier(text_to_cstring(PG_GETARG_TEXT_P(0)));
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

        initStringInfo(&state->template);
        appendStringInfo(&state->template, 
                         "{ \"xid\": 0" /* we're not decoding WAL actually */
                         ", \"command\": \"INSERT\""
                         ", \"relname\": \"%s\""
                         ", \"relnamespace\": \"%s\""
                         ", \"newtuple\": ",
                         RelationGetRelationName(rel),
                         get_namespace_name(RelationGetNamespace(rel)));

        /* save the reset position at end of template */
        state->reset_len = state->template.len;

        RelationClose(rel);

        MemoryContextSwitchTo(oldcontext);
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

    /* reset template length before spitting next tuple */
    state->template.len = state->reset_len;

    output_json_tuple(&state->template, SPI_tuptable->vals[0], SPI_tuptable->tupdesc);

    appendStringInfoString(&state->template, " }");

    SRF_RETURN_NEXT(funcctx, PointerGetDatum(cstring_to_text_with_len(state->template.data,
                                                                      state->template.len)));
}
