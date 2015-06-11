import psycopg2

def get_replication_slot_restart_lsn(curs, slot_name):
    curs.execute("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = %s",
                 (slot_name,))
    res = curs.fetchone()
    if res:
        res = res[0]
    curs.connection.commit()
    return res

def get_export_table_list(curs):
    curs.execute("""
SELECT c.relname, n.nspname

  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace

 WHERE c.relkind = 'r'
   AND n.nspname NOT LIKE 'pg_%%' AND n.nspname <> 'information_schema'
   AND c.relpersistence = 'p'
""")
    return curs.fetchall()

def export_snapshot(curs, snapshot_name, writer):
    curs.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
    curs.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_name,))

    tables = get_export_table_list(curs)

    # TODO: can be done in parallel
    for tbl in tables:
        curs.execute("SELECT * FROM bottledwater_export_json(%s, %s)", tbl) # (name, schema)
        for row in curs:
            writer.write(psycopg2.extras.ReplicationMessage(row[0]))

    curs.connection.commit()

#
# Example of writer class for the export:
#
# class SampleBottledwaterWriter(TextIOBase):
#
#     def write(self, msg):
#         print(msg.send_time)
#         print(msg.payload)
#
#         self.save_message(msg)
#
#         if self.saved_reliably(msg):
#             msg.cursor.sync_server(msg.wal_end)
#
# writer = SampleBottledwaterWriter()
# bottledwater.export(writer, dsn, slot, options={'format': 'json'})
#
def export(writer, dsn, slot_name, options=None, create_slot=False, initial_snapshot=False):
    from psycopg2.extras import ReplicationConnection, REPLICATION_LOGICAL

    #
    # We need to open two connections: one for regular queries (check
    # if replication slot exists, and export if not) and the other one
    # for streaming.
    #
    conn = psycopg2.connect(dsn)
    curs = conn.cursor()

    replconn = psycopg2.connect(dsn, connection_factory=ReplicationConnection)
    replcurs = replconn.cursor()

    restart_lsn = get_replication_slot_restart_lsn(curs, slot_name)
    if restart_lsn:
        print("Found existing replication slot.")
    else:
        print("Replication slot doesn't exist.")
        if create_slot:
            replcurs.create_replication_slot(REPLICATION_LOGICAL, slot_name, "bottledwater")
            (name, restart_lsn, snapshot_name, plugin_name) = replcurs.fetchone()
        else:
            print("Stopping.")
            return

        if initial_snapshot:
            print("Exporting tables using snapshot %s %s" % (snapshot_name, restart_lsn))
            export_snapshot(curs, snapshot_name, writer)

    # we no longer need the regular connection
    conn.close()

    print("Streaming changes on the replication slot: %s %s" % (slot_name, restart_lsn))
    replcurs.start_replication(writer, REPLICATION_LOGICAL,
                               slot_name=slot_name,
                               start_lsn=restart_lsn,
                               options=options)
    replconn.close()
