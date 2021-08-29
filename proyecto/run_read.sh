#!/bin/bash
#
PGPASSWORD=testPassword psql -h 10.7.84.102 -U postgres -p 5432 < read_tables.sql
#