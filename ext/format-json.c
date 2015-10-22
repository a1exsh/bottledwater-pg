#include "logdecoder.h"
#include "format-json.h"
#include "oid_util.h"

#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "parser/parse_coerce.h"
#include "replication/output_plugin.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/lsyscache.h"

/* String to output for infinite dates and timestamps */
#define DT_INFINITY "\"infinity\""

static void output_json_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void output_json_shutdown(LogicalDecodingContext *ctx);
static void output_json_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void output_json_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void output_json_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel, ReorderBufferChange *change);


void output_format_json_init(OutputPluginCallbacks *cb) {
    elog(DEBUG1, "bottledwater: output_format_json_init");
    cb->startup_cb = output_json_startup;
    cb->begin_cb = output_json_begin_txn;
    cb->change_cb = output_json_change;
    cb->commit_cb = output_json_commit_txn;
    cb->shutdown_cb = output_json_shutdown;
}

static void output_json_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
        bool is_init) {
    opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
}

static void output_json_shutdown(LogicalDecodingContext *ctx) {
}

static void output_json_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    OutputPluginPrepareWrite(ctx, true);

    output_json_common_header(ctx->out, "BEGIN", txn->xid, InvalidXLogRecPtr, NULL);
    appendStringInfoString(ctx->out, " }");

    OutputPluginWrite(ctx, true);
}

static void output_json_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                                   XLogRecPtr commit_lsn) {
    OutputPluginPrepareWrite(ctx, true);

    output_json_common_header(ctx->out, "COMMIT", txn->xid, InvalidXLogRecPtr, NULL);
    appendStringInfoString(ctx->out, " }");

    OutputPluginWrite(ctx, true);
}

static void output_json_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                               Relation rel, ReorderBufferChange *change) {
    HeapTuple oldtuple = NULL, newtuple = NULL;
    Relation pkey_index = NULL;
    const char *command = NULL;

    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
            if (!change->data.tp.newtuple) {
                elog(ERROR, "output_json_change: insert action without a tuple");
            }
            newtuple = &change->data.tp.newtuple->tuple;

            command = "INSERT";
            break;

        case REORDER_BUFFER_CHANGE_UPDATE:
            if (!change->data.tp.newtuple) {
                elog(ERROR, "output_json_change: update action without a tuple");
            }
            newtuple = &change->data.tp.newtuple->tuple;

            if (change->data.tp.oldtuple) {
                oldtuple = &change->data.tp.oldtuple->tuple;
            }
            command = "UPDATE";
            break;

        case REORDER_BUFFER_CHANGE_DELETE:
            if (change->data.tp.oldtuple) {
                oldtuple = &change->data.tp.oldtuple->tuple;
            }
            command = "DELETE";
            break;

        default:
            elog(ERROR, "output_json_change: unknown change action %d", change->action);
    }

    OutputPluginPrepareWrite(ctx, true);

    output_json_common_header(ctx->out, command, txn->xid, change->lsn, rel);

    pkey_index = table_key_index(rel);
    if (pkey_index) {
        appendStringInfoString(ctx->out, ", \"key\": ");
        output_json_relation_key(ctx->out, pkey_index);

        relation_close(pkey_index, AccessShareLock);
    }

    if (newtuple) {
        appendStringInfoString(ctx->out, ", \"newtuple\": ");
        output_json_tuple(ctx->out, newtuple, RelationGetDescr(rel));
    }
    if (oldtuple) {
        appendStringInfoString(ctx->out, ", \"oldtuple\": ");
        output_json_tuple(ctx->out, oldtuple, RelationGetDescr(rel));
    }
    appendStringInfoString(ctx->out, " }");

    OutputPluginWrite(ctx, true);
}

void output_json_common_header(StringInfo out, const char *cmd,
                               TransactionId xid, XLogRecPtr lsn,
                               Relation rel) {
    appendStringInfo(out,
                     "{ \"command\": \"%s\""
                     ", \"xid\": %u",
                     cmd, xid);

    if (lsn != InvalidXLogRecPtr) {
        appendStringInfo(out,
                         ", \"wal_pos\": \"%X/%X\"",
                         (uint32) (lsn >> 32),
                         (uint32) (lsn & 0xFFFFFFFF));
    }

    appendStringInfoString(out, ", \"dbname\": ");
    escape_json(out, get_database_name(MyDatabaseId));

    if (rel) {
        appendStringInfoString(out, ", \"relname\": ");
        escape_json(out, RelationGetRelationName(rel));

        appendStringInfoString(out, ", \"relnamespace\": ");
        escape_json(out, get_namespace_name(RelationGetNamespace(rel)));
    }
}

void output_json_relation_key(StringInfo out, Relation key) {
    TupleDesc desc = RelationGetDescr(key);
    int i, n = 0;

    appendStringInfoChar(out, '[');

    for (i = 0; i < desc->natts; i++) {
        Form_pg_attribute attr = desc->attrs[i];

        if (attr->attisdropped)
            continue;

        if (n > 0) {
            appendStringInfoString(out, ", ");
        }
        n++;

        escape_json(out, NameStr(attr->attname));
    }

    appendStringInfoChar(out, ']');
}

void output_json_tuple(StringInfo out, HeapTuple tuple, TupleDesc desc) {
    Datum datum, json;
    text *jtext;

    datum = heap_copy_tuple_as_datum(tuple, desc);
    json = DirectFunctionCall1(row_to_json, datum);
    jtext = DatumGetTextP(json);

    appendBinaryStringInfo(out, VARDATA_ANY(jtext), VARSIZE_ANY_EXHDR(jtext));
}
