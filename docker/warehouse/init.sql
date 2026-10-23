-- Runs only the first time the warehouse volume is created.
-- Consumers (BI tools, analysts) get this role: it can read <domain>_gold and nothing else.
-- publish_gold re-grants it on every publish because the schema object is swapped.
CREATE ROLE gold_reader NOLOGIN;
CREATE ROLE analyst LOGIN PASSWORD 'analyst' IN ROLE gold_reader;
