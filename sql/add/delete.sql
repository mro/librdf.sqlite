--
PRAGMA foreign_keys = ON;
PRAGMA recursive_triggers = ON;
PRAGMA encoding = "UTF-8"; 

----------------------------------------------------------------------------------------
-- create structure
----------------------------------------------------------------------------------------

-- https://www.sqlite.org/lang_droptrigger.html
DROP TRIGGER IF EXISTS master_delete;
DROP TRIGGER IF EXISTS ms_insert;
DROP TRIGGER IF EXISTS ms_delete;

-- https://www.sqlite.org/lang_dropview.html
DROP VIEW IF EXISTS ms;

-- https://www.sqlite.org/lang_droptable.html
DROP TABLE IF EXISTS master;
DROP TABLE IF EXISTS slave;

-- https://www.sqlite.org/lang_createtable.html
CREATE TABLE slave (
	id INTEGER PRIMARY KEY
);

CREATE TABLE master (
	id INTEGER PRIMARY KEY
  ,rel INTEGER NOT NULL REFERENCES slave(id) -- ON DELETE CASCADE
);

-- https://www.sqlite.org/lang_createtrigger.html
CREATE TRIGGER master_delete AFTER DELETE ON master
FOR EACH ROW
-- WHEN NOT EXISTS (SELECT id FROM master WHERE rel = old.rel)
-- WHEN (0=(SELECT COUNT(id) FROM master WHERE rel = old.rel))
BEGIN
	DELETE FROM slave WHERE id = OLD.rel AND (0=(SELECT COUNT(id) FROM master WHERE rel = OLD.rel));
END;

-- https://www.sqlite.org/lang_createview.html
CREATE VIEW ms AS SELECT
	master.id AS id
	,slave.id AS rel
FROM master
INNER JOIN slave ON master.rel = slave.id;

CREATE TRIGGER ms_insert INSTEAD OF INSERT ON ms
FOR EACH ROW BEGIN
	INSERT OR IGNORE INTO slave(id) VALUES(NEW.rel);
	-- SELECT LAST_INSERT_ROWID(); into a temp table.
	INSERT INTO master(id,rel) VALUES(NEW.id,NEW.rel); 
END;

CREATE TRIGGER ms_delete INSTEAD OF DELETE ON ms
FOR EACH ROW BEGIN
	DELETE FROM master WHERE id = OLD.id;
	DELETE FROM slave WHERE (0=(SELECT COUNT(id) FROM master WHERE rel = OLD.rel)) AND id = OLD.rel;
END;

----------------------------------------------------------------------------------------
-- populate
----------------------------------------------------------------------------------------

-- https://www.sqlite.org/lang_insert.html
INSERT INTO slave (id) VALUES (2);
INSERT INTO slave (id) VALUES (3);

INSERT INTO master (id,rel) VALUES (20,2);
INSERT INTO master (id,rel) VALUES (21,2);
INSERT INTO master (id,rel) VALUES (30,3);
INSERT INTO master (id,rel) VALUES (31,3);

INSERT INTO ms (id,rel) VALUES (40,4);
INSERT INTO ms (id,rel) VALUES (41,4);

----------------------------------------------------------------------------------------
-- delete some
----------------------------------------------------------------------------------------

-- https://www.sqlite.org/lang_delete.html
DELETE FROM master WHERE id=20;
DELETE FROM master WHERE id=21;

-- DELETE FROM slave WHERE id=2;

DELETE FROM ms WHERE id=40;
DELETE FROM ms WHERE id=41;

----------------------------------------------------------------------------------------
-- print result
----------------------------------------------------------------------------------------

-- https://www.sqlite.org/lang_select.html
SELECT
	master.id
	,slave.id
FROM master
LEFT JOIN slave ON master.rel = slave.id
UNION
SELECT
	master.id
	,slave.id
FROM slave
LEFT JOIN master ON master.rel = slave.id
ORDER BY master.id,slave.id;
