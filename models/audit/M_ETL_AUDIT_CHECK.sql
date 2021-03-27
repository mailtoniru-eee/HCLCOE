{{ config (bind=False) }}
  {{ audit_insert(odate,'ABORTED',modelexestrttime, current_timestamp(), 0, 0, 0, 'STAGING TABLE DOESNOT EXIST', 'F' ) }}
