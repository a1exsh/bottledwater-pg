import bottledwater

def consume(msg):
    print(msg.payload)
    if 'COMMIT' in msg.payload:
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

bottledwater.export(consume, 'dbname=postgres', slot_name='bwtest', create_slot=True,
                    options={ 'format': 'JSON' })
