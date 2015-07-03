import psycopg2
import select


def wait_for_connections(conns):
    while True:
        (rd, wr, xc) = ([], [], [])

        for c in conns:
            state = c.poll()
            if state == psycopg2.extensions.POLL_OK:
                return c
            elif state == psycopg2.extensions.POLL_READ:
                rd.append(c)
            elif state == psycopg2.extensions.POLL_WRITE:
                wr.append(c)
            else:
                raise psycopg2.OperationalError("poll() returned %s" % state)

        try:
            select.select(rd, wr, xc)
        except InterruptedError as e:
            print(repr(e))


def get_export_table_list(curs):
    curs.execute("""
SELECT c.relname, n.nspname

  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace

 WHERE c.relkind = 'r'
   AND n.nspname NOT LIKE 'pg_%%' AND n.nspname <> 'information_schema'
   AND c.relpersistence = 'p'
""")
    wait_for_connections([curs.connection])
    return curs.fetchall()


def get_replication_slot_restart_lsn(curs, slot_name):
    curs.execute("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = %s",
                 (slot_name,))
    wait_for_connections([curs.connection])
    res = curs.fetchone()
    if res:
        return res[0]


def begin_snapshot_transaction(curs, snapshot_name):
    curs.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    wait_for_connections([curs.connection])

    curs.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_name,))
    wait_for_connections([curs.connection])


def export_snapshot(master_curs, snapshot_name, writer, snapshot_policy, max_jobs=1):
    master_conn = master_curs.connection

    begin_snapshot_transaction(master_curs, snapshot_name)
    tables = get_export_table_list(master_curs)

    #
    # We keep all open connections, including the master one in two
    # dicts: idle and active.  The mapping is from a connection to an
    # open cursor.
    #
    # We start with one idle connection: the master one.
    #
    idle = {}
    idle[master_conn] = master_curs
    active = {}

    # loop while we have more tables or some active connections
    while tables or active:
        #
        # Check if we still have tables to export, and there are some
        # idle connections (or we haven't maxed out the jobs yet).
        #
        while tables and (idle or len(active) < max_jobs):

            tbl = tables.pop(0)
            if snapshot_policy(*tbl):
                print("next table: %s" % repr(tbl))

                if idle:
                    print("using an idle connection")
                    (conn, curs) = idle.popitem()
                    active[conn] = curs
                else:
                    print("making a new connection (%d/%d max.)" % (len(active)+1, max_jobs))
                    conn = psycopg2.connect(master_conn.dsn, async=True)
                    wait_for_connections([conn])
                    curs = conn.cursor()
                    begin_snapshot_transaction(curs, snapshot_name)
                    active[conn] = curs
                #
                # Now we have conn and curs objects: actually run the
                # export statement.
                #
                # NB: using "SELECT * FROM" would try to cache the
                # whole result set on the server side before sending
                # to us, so avoid it.
                #
                curs.execute("SELECT bottledwater_export_json(%s, %s)", tbl) # (tbl, nmsp)

        # wait for any of the active connections
        conn = wait_for_connections(active.keys())
        curs = active[conn]

        row = curs.fetchone()
        if row:
            writer.write(psycopg2.extras.ReplicationMessage(curs, row[0]))
        else:
            active.pop(conn)
            idle[conn] = curs

    idle.pop(master_conn)

    # all connections should be idle at this point: close them
    for conn in idle:
        conn.close()
    #master_conn.commit()


def policy_export_all(relname, relnamespace):
    return True


class ReplicationSlotExists(RuntimeError):
    pass

class ReplicationSlotDoesNotExist(RuntimeError):
    pass

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
#             msg.cursor.send_replication_feedback(flush_lsn=msg.wal_end)
#
# writer = SampleBottledwaterWriter()
# bottledwater.export(writer, dsn, slot, options={'format': 'json'})
#
def export(writer, dsn, slot_name, options=None, create_slot=False,
           initial_snapshot=False, snapshot_policy=policy_export_all,
           max_snapshot_jobs=1, reconnect_delay=10):
    import time
    from psycopg2.extras import ReplicationConnection, REPLICATION_LOGICAL

    #
    # We need to open two connections: one for regular queries (check
    # if replication slot exists, and export if not) and the other one
    # for streaming.
    #
    conn = psycopg2.connect(dsn, async=True)
    wait_for_connections([conn])
    curs = conn.cursor()

    replconn = psycopg2.connect(dsn, connection_factory=ReplicationConnection)
    replcurs = replconn.cursor()

    restart_lsn = get_replication_slot_restart_lsn(curs, slot_name)
    if restart_lsn:
        print("Found existing replication slot.")

        if initial_snapshot:
            raise ReplicationSlotExists("Cannot make snapshot from existing replication slot.")

    else:
        print("Replication slot doesn't exist.")

        if create_slot:
            print("Creating logical replication slot %s for bottledwater..." % slot_name)

            replcurs.create_replication_slot(REPLICATION_LOGICAL, slot_name, "bottledwater")
            (name, restart_lsn, snapshot_name, plugin_name) = replcurs.fetchone()
        else:
            raise ReplicationSlotDoesNotExist("Replication slot doesn't exist.")

        if initial_snapshot:
            print("Exporting tables using snapshot %s %s (max. connections: %d)" %
                  (snapshot_name, restart_lsn, max_snapshot_jobs))

            try:
                export_snapshot(curs, snapshot_name, writer, snapshot_policy,
                                max_jobs=max_snapshot_jobs)
            except:
                replcurs.drop_replication_slot(slot_name)
                raise

    # we no longer need the regular connection
    conn.close()

    print("Streaming changes on the replication slot: %s %s" % (slot_name, restart_lsn))
    while True:
        try:
            if replconn.closed:            
                replconn = psycopg2.connect(dsn, connection_factory=ReplicationConnection)
                replcurs = replconn.cursor()

            replcurs.start_replication(REPLICATION_LOGICAL,
                                       slot_name=slot_name,
                                       writer=writer,
                                       start_lsn=restart_lsn,
                                       options=options)

        except psycopg2.DatabaseError as e:
            print(repr(e))
            replconn.close()

            print("Trying to re-attach to replication slot in 10 seconds...")
            time.sleep(reconnect_delay)