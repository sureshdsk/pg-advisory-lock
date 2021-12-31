```
python training1.py 1001
python training1.py 1001
python training1.py 1002
```


```
-- debug
select * from pg_catalog.pg_locks
select * from pg_catalog.pg_locks where objid =1001
select * from pg_catalog.pg_locks where objid =1002
```