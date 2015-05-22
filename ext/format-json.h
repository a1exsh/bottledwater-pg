#ifndef FORMAT_JSON_H
#define FORMAT_JSON_H

#include "replication/output_plugin.h"

void output_format_json_init(OutputPluginCallbacks *cb);
void output_json_relation_header(StringInfo out, const char *cmd,
                                 TransactionId xid, XLogRecPtr lsn, Relation rel);
void output_json_tuple(StringInfo out, HeapTuple tuple, TupleDesc desc);

#endif /* FORMAT_JSON_H */
