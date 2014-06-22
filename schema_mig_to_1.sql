-- 
-- Copyright (c) 2014, Marcus Rohrmoser mobile Software
-- All rights reserved.
-- 
-- Redistribution and use in source and binary forms, with or without modification, are permitted
-- provided that the following conditions are met:
-- 
-- 1. Redistributions of source code must retain the above copyright notice, this list of conditions
--    and the following disclaimer.
-- 
-- 2. The software must not be used for military or intelligence or related purposes nor
--    anything that's in conflict with human rights as declared in http://www.un.org/en/documents/udhr/ .
-- 
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
-- IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
-- FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
-- CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
-- DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
-- DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
-- IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
-- THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
--

CREATE TABLE so_uris (
  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1
  ,uri TEXT NOT NULL
);
CREATE UNIQUE INDEX so_uris_index ON so_uris (uri);

CREATE TABLE so_blanks (
  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1
  ,blank TEXT NULL
);
CREATE UNIQUE INDEX so_blanks_index ON so_blanks (blank);

CREATE TABLE p_uris (
  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1
  ,uri TEXT NOT NULL
);
CREATE UNIQUE INDEX p_uris_index ON p_uris (uri);

CREATE TABLE t_uris (
  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1
  ,uri TEXT NOT NULL
);
CREATE UNIQUE INDEX t_uris_index ON t_uris (uri);

CREATE TABLE o_literals (
  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1
  ,datatype INTEGER NOT NULL REFERENCES t_uris (id)
  ,text TEXT NOT NULL
  ,language TEXT NOT NULL
);
CREATE UNIQUE INDEX o_literals_index ON o_literals (text,language,datatype);

CREATE TABLE c_uris (
  id INTEGER PRIMARY KEY AUTOINCREMENT -- start with 1
  ,uri TEXT NOT NULL
);
CREATE UNIQUE INDEX c_uris_index ON c_uris (uri);

-- ensure dummies for 0 so as we can relate NOT NULL in spocs
INSERT INTO so_uris (id, uri) VALUES (0,'');
INSERT INTO so_blanks (id, blank) VALUES (0,'');
-- INSERT INTO p_uris (id, uri) VALUES (0,'');
INSERT INTO c_uris (id, uri) VALUES (0,'');
INSERT INTO t_uris (id, uri) VALUES (0,'');

CREATE TABLE spocs (
  s_uri INTEGER NOT NULL    REFERENCES so_uris (id)
  ,s_blank INTEGER NOT NULL REFERENCES so_blanks (id)
  ,p_uri INTEGER NOT NULL   REFERENCES p_uris (id)
  ,o_uri INTEGER NOT NULL   REFERENCES so_uris (id)
  ,o_blank INTEGER NOT NULL REFERENCES so_blanks (id)
  ,o_lit INTEGER NOT NULL   REFERENCES o_literals (id)
  ,c_uri INTEGER NOT NULL   REFERENCES c_uris (id)
);
CREATE UNIQUE INDEX spocs_index ON spocs (s_uri,s_blank,p_uri,o_uri,o_blank,o_lit,c_uri);
-- CREATE INDEX spocs_index_so ON spocs (s_uri,o_uri);
-- CREATE INDEX spocs_index_p ON spocs (p_uri);
-- CREATE INDEX spocs_index_s_blank ON spocs (s_blank,p_uri);
-- CREATE INDEX spocs_index_o_blank ON spocs (o_blank,p_uri);
-- CREATE INDEX spocs_index_o_lit ON spocs (o_lit);

-----------------------------------------------------------
-- Convenience VIEW
-----------------------------------------------------------

CREATE VIEW spocs_full AS
SELECT
  -- the ids:
--  rowid            AS id
  s_uris.id       AS s_uri_id
  ,s_blanks.id     AS s_blank_id
  ,p_uris.id       AS p_uri_id
  ,o_uris.id       AS o_uri_id
  ,o_blanks.id     AS o_blank_id
  ,o_literals.id   AS o_lit_id
  ,o_lit_uris.id   AS o_datatype_id
  ,c_uris.id       AS c_uri_id
  -- the values:
  ,s_uris.uri      AS s_uri
  ,s_blanks.blank  AS s_blank
  ,p_uris.uri      AS p_uri
  ,o_uris.uri      AS o_uri
  ,o_blanks.blank  AS o_blank
  ,o_literals.text AS o_text
  ,o_literals.language AS o_language
  ,o_lit_uris.uri  AS o_datatype
  ,c_uris.uri      AS c_uri
FROM spocs
INNER JOIN so_uris    AS s_uris     ON spocs.s_uri      = s_uris.id
INNER JOIN so_blanks  AS s_blanks   ON spocs.s_blank    = s_blanks.id
INNER JOIN p_uris     AS p_uris     ON spocs.p_uri      = p_uris.id
INNER JOIN so_uris    AS o_uris     ON spocs.o_uri      = o_uris.id
INNER JOIN so_blanks  AS o_blanks   ON spocs.o_blank    = o_blanks.id
LEFT OUTER JOIN o_literals AS o_literals ON spocs.o_lit = o_literals.id
LEFT OUTER JOIN t_uris     AS o_lit_uris ON o_literals.datatype   = o_lit_uris.id
INNER JOIN c_uris     AS c_uris     ON spocs.c_uri      = c_uris.id
;

-- CREATE TRIGGER spocs_full_delete INSTEAD OF DELETE ON spocs_full
-- FOR EACH ROW BEGIN
--   DELETE FROM so_uris    WHERE (1>=(SELECT COUNT(id) FROM spocs WHERE s_uri_id      = OLD.s_uri_id))      AND id = OLD.s_uri_id;
--   DELETE FROM so_blanks  WHERE (1>=(SELECT COUNT(id) FROM spocs WHERE s_blank_id    = OLD.s_blank_id))    AND id = OLD.s_blank_id;
--   DELETE FROM p_uris     WHERE (1>=(SELECT COUNT(id) FROM spocs WHERE p_uri_id      = OLD.p_uri_id))      AND id = OLD.p_uri_id;
--   DELETE FROM so_uris    WHERE (1>=(SELECT COUNT(id) FROM spocs WHERE o_uri_id      = OLD.o_uri_id))      AND id = OLD.o_uri_id;
--   DELETE FROM so_blanks  WHERE (1>=(SELECT COUNT(id) FROM spocs WHERE o_blank_id    = OLD.o_blank_id))    AND id = OLD.o_blank_id;
--   DELETE FROM o_literals WHERE (1>=(SELECT COUNT(id) FROM spocs WHERE o_lit_id      = OLD.o_lit_id))      AND id = OLD.o_lit_id;
--   DELETE FROM o_lit_uris WHERE (1>=(SELECT COUNT(id) FROM o_literals WHERE o_lit_uris.id = OLD.o_datatype_id)) AND id = OLD.o_datatype_id;
--   DELETE FROM c_uris     WHERE (1>=(SELECT COUNT(id) FROM spocs WHERE c_uri_id      = OLD.c_uri_id))      AND id = OLD.c_uri_id;
--   DELETE FROM spocs WHERE rowid = OLD.id;
-- END;

PRAGMA user_version=1;
