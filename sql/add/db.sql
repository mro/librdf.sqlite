-- 
-- Copyright (c) 2014-2015, Marcus Rohrmoser mobile Software, http://mro.name/me
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

PRAGMA foreign_keys = ON;
PRAGMA recursive_triggers = ON;
PRAGMA encoding = 'UTF-8';

 -- URIs for subjects and objects
CREATE TABLE so_uris (
  id INTEGER PRIMARY KEY
  ,uri TEXT NOT NULL UNIQUE -- semantic constraint, could be dropped to save space
);

 -- blank node IDs for subjects and objects
CREATE TABLE so_blanks (
  id INTEGER PRIMARY KEY
  ,blank TEXT NOT NULL UNIQUE -- semantic constraint, could be dropped to save space
);

 -- URIs for predicates
CREATE TABLE p_uris (
  id INTEGER PRIMARY KEY
  ,uri TEXT NOT NULL UNIQUE -- semantic constraint, could be dropped to save space
);

 -- URIs for literal types
CREATE TABLE t_uris (
  id INTEGER PRIMARY KEY
  ,uri TEXT NOT NULL UNIQUE -- semantic constraint, could be dropped to save space
);

 -- literal values
CREATE TABLE o_literals (
  id INTEGER PRIMARY KEY
  ,datatype_id INTEGER NULL REFERENCES t_uris(id)
  ,language TEXT NULL
  ,text TEXT NOT NULL
);
CREATE UNIQUE INDEX o_literals_index ON o_literals (text,language,datatype_id); -- semantic constraint, could be dropped to save space

 -- URIs for context
CREATE TABLE c_uris (
  id INTEGER PRIMARY KEY
  ,uri TEXT NOT NULL UNIQUE -- semantic constraint, could be dropped to save space
);

CREATE TABLE triple_relations (
  id INTEGER PRIMARY KEY
  ,s_uri_id   INTEGER NULL      REFERENCES so_uris(id)
  ,s_blank_id INTEGER NULL      REFERENCES so_blanks(id)
  ,p_uri_id   INTEGER NOT NULL  REFERENCES p_uris(id)
  ,o_uri_id   INTEGER NULL      REFERENCES so_uris(id)
  ,o_blank_id INTEGER NULL      REFERENCES so_blanks(id)
  ,o_lit_id   INTEGER NULL      REFERENCES o_literals(id)
  ,c_uri_id   INTEGER NULL      REFERENCES c_uris(id)
  , CONSTRAINT null_subject CHECK ( -- ensure uri/blank are mutually exclusively set
    (s_uri_id IS NOT NULL AND s_blank_id IS NULL) OR
    (s_uri_id IS NULL AND s_blank_id IS NOT NULL)
  )
  , CONSTRAINT null_object CHECK ( -- ensure uri/blank/literal are mutually exclusively set
    (o_uri_id IS NOT NULL AND o_blank_id IS NULL AND o_lit_id IS NULL) OR
    (o_uri_id IS NULL AND o_blank_id IS NOT NULL AND o_lit_id IS NULL) OR
    (o_uri_id IS NULL AND o_blank_id IS NULL AND o_lit_id IS NOT NULL)
  )
);
 -- semantic constraint, could be dropped to save space:
CREATE UNIQUE INDEX triple_relations_index     ON triple_relations(s_uri_id,s_blank_id,p_uri_id,o_uri_id,o_blank_id,o_lit_id,c_uri_id);
-- optional indexes for lookup performance, mostly on DELETE.
CREATE INDEX triple_relations_index_s_uri_id   ON triple_relations(s_uri_id); -- WHERE s_uri_id IS NOT NULL;
CREATE INDEX triple_relations_index_s_blank_id ON triple_relations(s_blank_id); -- WHERE s_blank_id IS NOT NULL;
CREATE INDEX triple_relations_index_p_uri_id   ON triple_relations(p_uri_id); -- WHERE p_uri_id IS NOT NULL;
CREATE INDEX triple_relations_index_o_uri_id   ON triple_relations(o_uri_id); -- WHERE o_uri_id IS NOT NULL;
CREATE INDEX triple_relations_index_o_blank_id ON triple_relations(o_blank_id); -- WHERE s_blank_id IS NOT NULL;
CREATE INDEX triple_relations_index_o_lit_id   ON triple_relations(o_lit_id); -- WHERE o_lit_id IS NOT NULL;
CREATE INDEX o_literals_index_datatype_id      ON o_literals(datatype_id); -- WHERE datatype_id IS NOT NULL;

CREATE VIEW triples AS
SELECT
  -- all *_id (hashes):
  triple_relations.id AS id
  ,s_uri_id
  ,s_blank_id
  ,p_uri_id
  ,o_uri_id
  ,o_blank_id
  ,o_lit_id
  ,o_literals.datatype_id AS o_datatype_id
  ,c_uri_id
  -- all joined values:
  ,s_uris.uri      AS s_uri
  ,s_blanks.blank  AS s_blank
  ,p_uris.uri      AS p_uri
  ,o_uris.uri      AS o_uri
  ,o_blanks.blank  AS o_blank
  ,o_literals.text AS o_text
  ,o_literals.language AS o_language
  ,o_lit_uris.uri  AS o_datatype
  ,c_uris.uri      AS c_uri
FROM triple_relations
LEFT OUTER JOIN so_uris    AS s_uris     ON triple_relations.s_uri_id   = s_uris.id
LEFT OUTER JOIN so_blanks  AS s_blanks   ON triple_relations.s_blank_id = s_blanks.id
INNER      JOIN p_uris     AS p_uris     ON triple_relations.p_uri_id   = p_uris.id
LEFT OUTER JOIN so_uris    AS o_uris     ON triple_relations.o_uri_id   = o_uris.id
LEFT OUTER JOIN so_blanks  AS o_blanks   ON triple_relations.o_blank_id = o_blanks.id
LEFT OUTER JOIN o_literals AS o_literals ON triple_relations.o_lit_id   = o_literals.id
LEFT OUTER JOIN t_uris     AS o_lit_uris ON o_literals.datatype_id      = o_lit_uris.id
LEFT OUTER JOIN c_uris     AS c_uris     ON triple_relations.c_uri_id   = c_uris.id
;

CREATE TRIGGER triples_insert INSTEAD OF INSERT ON triples
FOR EACH ROW BEGIN
  -- subject uri/blank
  INSERT OR IGNORE INTO so_uris   (id,uri)   VALUES (NEW.s_uri_id,      NEW.s_uri);
  INSERT OR IGNORE INTO so_blanks (id,blank) VALUES (NEW.s_blank_id,    NEW.s_blank);
  -- predicate uri
  INSERT OR IGNORE INTO p_uris    (id,uri)   VALUES (NEW.p_uri_id,      NEW.p_uri);
  -- object uri/blank
  INSERT OR IGNORE INTO so_uris   (id,uri)   VALUES (NEW.o_uri_id,      NEW.o_uri);
  INSERT OR IGNORE INTO so_blanks (id,blank) VALUES (NEW.o_blank_id,    NEW.o_blank);
  -- object literal
  INSERT OR IGNORE INTO t_uris    (id,uri)   VALUES (NEW.o_datatype_id, NEW.o_datatype);
  INSERT OR IGNORE INTO o_literals(id,datatype_id,language,text) VALUES (NEW.o_lit_id, NEW.o_datatype_id, NEW.o_language, NEW.o_text);
  -- context uri
  INSERT OR IGNORE INTO c_uris    (id,uri)   VALUES (NEW.c_uri_id,      NEW.c_uri);
  -- triple
  INSERT INTO triple_relations(id, s_uri_id, s_blank_id, p_uri_id, o_uri_id, o_blank_id, o_lit_id, c_uri_id)
  VALUES (NEW.id, NEW.s_uri_id, NEW.s_blank_id, NEW.p_uri_id, NEW.o_uri_id, NEW.o_blank_id, NEW.o_lit_id, NEW.c_uri_id);
END;

CREATE TRIGGER triples_delete INSTEAD OF DELETE ON triples
FOR EACH ROW BEGIN
  -- triple
  DELETE FROM triple_relations WHERE id = OLD.id;
  -- subject uri/blank
  DELETE FROM so_uris    WHERE (OLD.s_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE s_uri_id    = OLD.s_uri_id))      AND (id = OLD.s_uri_id);
  DELETE FROM so_blanks  WHERE (OLD.s_blank_id    IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE s_blank_id  = OLD.s_blank_id))    AND (id = OLD.s_blank_id);
  -- predicate uri
  DELETE FROM p_uris     WHERE (OLD.p_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE p_uri_id    = OLD.p_uri_id))      AND (id = OLD.p_uri_id);
  -- object uri/blank
  DELETE FROM so_uris    WHERE (OLD.o_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE o_uri_id    = OLD.o_uri_id))      AND (id = OLD.o_uri_id);
  DELETE FROM so_blanks  WHERE (OLD.o_blank_id    IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE o_blank_id  = OLD.o_blank_id))    AND (id = OLD.o_blank_id);
  -- object literal
  DELETE FROM o_literals WHERE (OLD.o_lit_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE o_lit_id    = OLD.o_lit_id))      AND (id = OLD.o_lit_id);
  DELETE FROM t_uris     WHERE (OLD.o_datatype_id IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM o_literals       WHERE datatype_id = OLD.o_datatype_id)) AND (id = OLD.o_datatype_id);
  -- context uri
  DELETE FROM c_uris     WHERE (OLD.c_uri_id      IS NOT NULL) AND (0 == (SELECT COUNT(id) FROM triple_relations WHERE c_uri_id    = OLD.c_uri_id))      AND (id = OLD.c_uri_id);
END;

PRAGMA user_version=1;




-- spo URIs
INSERT INTO triples(
  id,
  s_uri_id, s_uri,
  s_blank_id, s_blank,
  p_uri_id, p_uri,
  o_uri_id, o_uri,
  o_blank_id, o_blank,
  o_lit_id, o_text, o_language, o_datatype_id, o_datatype,
  c_uri_id, c_uri
) VALUES (
  100,
  1001, 's foo',
  NULL, NULL,
  3001, 'p foo',
  4001, 'o foo',
  NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL
);

-- so BLANKs, p URI
INSERT INTO triples(
  id,
  s_uri_id, s_uri,
  s_blank_id, s_blank,
  p_uri_id, p_uri,
  o_uri_id, o_uri,
  o_blank_id, o_blank,
  o_lit_id, o_datatype_id, o_datatype, o_language, o_text,
  c_uri_id, c_uri
) VALUES (
  101,
  NULL, NULL,
  2001, 's -',
  3002, 'p bar',
  NULL, NULL,
  5001, 'o -',
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL
);

-- s BLANK, p URI, o LITERAL
INSERT INTO triples(
  id,
  s_uri_id, s_uri,
  s_blank_id, s_blank,
  p_uri_id, p_uri,
  o_uri_id, o_uri,
  o_blank_id, o_blank,
  o_lit_id, o_datatype_id, o_datatype, o_language, o_text,
  c_uri_id, c_uri
) VALUES (
  102,
  NULL, NULL,
  2001, 's -',
  3001, 'p foo',
  NULL, NULL,
  NULL, NULL,
  6001, 7001, 'xsd:string', 'deu', 'Lorem ipsum',
  NULL, NULL
);

-- s BLANK, p URI, o LITERAL
INSERT INTO triples(
  id,
  s_uri_id, s_uri,
  s_blank_id, s_blank,
  p_uri_id, p_uri,
  o_uri_id, o_uri,
  o_blank_id, o_blank,
  o_lit_id, o_datatype_id, o_datatype, o_language, o_text,
  c_uri_id, c_uri
) VALUES (
  103,
  NULL, NULL,
  2001, 's -',
  3001, 'p foo',
  NULL, NULL,
  NULL, NULL,
  6002, NULL, NULL, NULL, 'Lorem ipsum 2',
  NULL, NULL
);

-- DELETE FROM triples;

