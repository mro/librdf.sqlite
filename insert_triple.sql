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

INSERT OR IGNORE INTO triples(
  id,
  s_uri_id, s_uri,
  s_blank_id, s_blank,
  p_uri_id, p_uri,
  o_uri_id, o_uri,
  o_blank_id, o_blank,
  o_lit_id, o_datatype_id, o_datatype, o_language, o_text,
  c_uri_id, c_uri
) VALUES (
  :stmt_id,
  :s_uri_id, :s_uri,
  :s_blank_id, :s_blank,
  :p_uri_id, :p_uri,
  :o_uri_id, :o_uri,
  :o_blank_id, :o_blank,
  :o_lit_id, :o_datatype_id, :o_datatype, :o_language, :o_text,
  :c_uri_id, :c_uri
)
