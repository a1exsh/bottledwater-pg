#include "oid_util.h"
#include "access/heapam.h"

/* Returns the relation object for the index that we're going to use as key for a
 * particular table. (Indexes are relations too!) Returns null if the table is unkeyed.
 * The return value is opened with a shared lock; call relation_close() when finished. */
Relation table_key_index(Relation rel) {
    char replident = rel->rd_rel->relreplident;
    Oid repl_ident_oid;
    List *indexes;
    ListCell *index_cell;

    if (replident == REPLICA_IDENTITY_NOTHING) {
        return NULL;
    }

    if (replident == REPLICA_IDENTITY_INDEX) {
        repl_ident_oid = RelationGetReplicaIndex(rel);
        if (repl_ident_oid != InvalidOid) {
            return relation_open(repl_ident_oid, AccessShareLock);
        }
    }

    // There doesn't seem to be a convenient way of getting the primary key index for
    // a table, so we have to iterate over all the table's indexes.
    indexes = RelationGetIndexList(rel);

    foreach(index_cell, indexes) {
        Relation index_rel = relation_open(lfirst_oid(index_cell), AccessShareLock);
        Form_pg_index index = index_rel->rd_index;

        if (IndexIsValid(index) && IndexIsReady(index) && index->indisprimary) {
            list_free(indexes);
            return index_rel;
        }
        relation_close(index_rel, AccessShareLock);
    }

    list_free(indexes);
    return NULL;
}
