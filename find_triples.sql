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

 -- result columns must match as in enum idx_triple_column_t
SELECT
 -- all *_id (hashes):
  id
  ,s_uri_id
  ,s_blank_id
  ,p_uri_id
  ,o_uri_id
  ,o_blank_id
  ,o_lit_id
  ,o_datatype_id
  ,c_uri_id
 -- all values:
  ,s_uri
  ,s_blank
  ,p_uri
  ,o_uri
  ,o_blank
  ,o_text
  ,o_language
  ,o_datatype
  ,c_uri
FROM triples
WHERE
 -- subject
    ((:s_uri_id   IS NULL) OR (s_uri_id   = :s_uri_id))
AND ((:s_blank_id IS NULL) OR (s_blank_id = :s_blank_id))
-- predicate
AND ((:p_uri_id   IS NULL) OR (p_uri_id   = :p_uri_id))
 -- object
AND ((:o_uri_id   IS NULL) OR (o_uri_id   = :o_uri_id))
AND ((:o_blank_id IS NULL) OR (o_blank_id = :o_blank_id))
AND ((:o_lit_id   IS NULL) OR (o_lit_id   = :o_lit_id))
 -- context node
AND ((:c_uri_id   IS NULL) OR (c_uri_id   = :c_uri_id))
