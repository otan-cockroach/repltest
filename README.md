# repltest

Built to test out the replication protocol for PG.

## Usage

Create a replication feed. This should output a LSN, which should be noted.

```sql
select * from pg_create_logical_replication_slot('slot_name', 'test_decoding');
```

Run the program against the given source (note: `?replication=database` is needed):

```sh
go run . --pgurl '<pgurl>?replication=database' --start_lsn '1E1/4C0002E0' --slot_name 'slot_name'
```

Make changes on the database and see the replication in action.