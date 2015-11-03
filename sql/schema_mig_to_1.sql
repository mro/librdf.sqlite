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
  ,uri TEXT NOT NULL -- UNIQUE -- redundant constraint (hash should do), could be dropped to save space
);

 -- blank node IDs for subjects and objects
CREATE TABLE so_blanks (
  id INTEGER PRIMARY KEY
  ,blank TEXT NOT NULL -- UNIQUE -- redundant constraint (hash should do), could be dropped to save space
);

 -- URIs for predicates
CREATE TABLE p_uris (
  id INTEGER PRIMARY KEY
  ,uri TEXT NOT NULL -- UNIQUE -- redundant constraint (hash should do), could be dropped to save space
);

 -- URIs for literal types
CREATE TABLE t_uris (
  id INTEGER PRIMARY KEY
  ,uri TEXT NOT NULL -- UNIQUE -- redundant constraint (hash should do), could be dropped to save space
);

 -- literal values
CREATE TABLE o_literals (
  id INTEGER PRIMARY KEY
  ,datatype_id INTEGER NULL REFERENCES t_uris(id)
  ,language TEXT NULL
  ,text TEXT NOT NULL
);
 -- CREATE UNIQUE INDEX o_literals_index ON o_literals (text,language,datatype_id); -- redundant constraint (hash should do), could be dropped to save space

 -- URIs for context
CREATE TABLE c_uris (
  id INTEGER PRIMARY KEY
  ,uri TEXT NOT NULL -- UNIQUE -- redundant constraint (hash should do), could be dropped to save space
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
  , CONSTRAINT null_subject CHECK ( -- ensure uri/blank are mutually exclusive
    (s_uri_id IS NOT NULL AND s_blank_id IS NULL) OR
    (s_uri_id IS NULL AND s_blank_id IS NOT NULL)
  )
  , CONSTRAINT null_object CHECK ( -- ensure uri/blank/literal are mutually exclusive
    (o_uri_id IS NOT NULL AND o_blank_id IS NULL AND o_lit_id IS NULL) OR
    (o_uri_id IS NULL AND o_blank_id IS NOT NULL AND o_lit_id IS NULL) OR
    (o_uri_id IS NULL AND o_blank_id IS NULL AND o_lit_id IS NOT NULL)
  )
);
 -- redundant constraint (hash should do), could be dropped to save space:
 -- CREATE UNIQUE INDEX triple_relations_index     ON triple_relations(s_uri_id,s_blank_id,p_uri_id,o_uri_id,o_blank_id,o_lit_id,c_uri_id);
 -- optional indexes for lookup performance, mostly on DELETE.
CREATE INDEX triple_relations_index_s_uri_id   ON triple_relations(s_uri_id); -- WHERE s_uri_id IS NOT NULL;
CREATE INDEX triple_relations_index_s_blank_id ON triple_relations(s_blank_id); -- WHERE s_blank_id IS NOT NULL;
CREATE INDEX triple_relations_index_p_uri_id   ON triple_relations(p_uri_id); -- WHERE p_uri_id IS NOT NULL;
CREATE INDEX triple_relations_index_o_uri_id   ON triple_relations(o_uri_id); -- WHERE o_uri_id IS NOT NULL;
CREATE INDEX triple_relations_index_o_blank_id ON triple_relations(o_blank_id); -- WHERE s_blank_id IS NOT NULL;
CREATE INDEX triple_relations_index_o_lit_id   ON triple_relations(o_lit_id); -- WHERE o_lit_id IS NOT NULL;
CREATE INDEX o_literals_index_datatype_id      ON o_literals(datatype_id); -- WHERE datatype_id IS NOT NULL;

-- continue DB schema setup in next migration to avoid
-- : warning: string length ‘7405’ is greater than the length ‘4095’ ISO C99 compilers are required to support [-Woverlength-strings]

PRAGMA user_version=1;
