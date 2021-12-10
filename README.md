# Sync Tables

This is one of those DIY utilities to sync tables between two MS SQL databases.

## Usage
```
sdb -sc "<<source connection>>" -tc "<<target connection>>" -t "<<table name>>" -k "<<key columns (comma saperated)>>"
```
## How it works?

After verifying the connections to the source and target data bases, program selects key columns concatenated as a string from both the tables. This key column(s) string list is used to diff the tables. 

Then a loop runs adding the new rows from the source to a DataSet and once the process is finished, rows are inserted in to the target table.

This is not an efficient way to do things but it works.
