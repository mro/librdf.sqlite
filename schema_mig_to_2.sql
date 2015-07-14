--
-- Copyright (c) 2015, Marcus Rohrmoser mobile Software, http://purl.mro.name/rdf/sqlite
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

DROP TRIGGER triples_delete;

CREATE TRIGGER triples_delete INSTEAD OF DELETE ON triples
FOR EACH ROW BEGIN
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
  -- triple
  DELETE FROM triple_relations WHERE id = OLD.id;
END;

PRAGMA user_version=2;
