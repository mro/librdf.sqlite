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

-- result columns must match as used in pub_stream_get_statement
SELECT
  s_uri
  ,s_blank
  ,p_uri
  ,o_uri
  ,o_blank
  ,o_text
  ,o_language
  ,o_datatype
  ,c_uri
FROM spocs_full
WHERE
-- subject
    ((:s_type != :t_uri)     OR (s_uri = :s_uri))
AND ((:s_type != :t_blank)   OR (s_blank = :s_blank))
-- predicate
AND ((:p_type != :t_uri)     OR (p_uri = :p_uri))
-- object
AND ((:o_type != :t_uri)     OR (o_uri = :o_uri))
AND ((:o_type != :t_blank)   OR (o_blank = :o_blank))
AND ((:o_type != :t_literal) OR (o_text = :o_text AND o_datatype = :o_datatype AND o_language = :o_language))
-- context node
AND ((:c_wild)               OR (c_uri = :c_uri))
