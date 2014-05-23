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

-- find triple parts (nodes)

SELECT :part_s_u AS part, :t_uri AS node_type, id FROM so_uris WHERE (:s_type = :t_uri) AND uri = :s_uri
UNION
SELECT :part_s_b AS part, :t_blank AS node_type, id FROM so_blanks WHERE (:s_type = :t_blank) AND blank = :s_blank
UNION
SELECT :part_p_u AS part, :t_uri AS node_type, id FROM p_uris WHERE (:p_type = :t_uri) AND uri = :p_uri
UNION
SELECT :part_o_u AS part, :t_uri AS node_type, id FROM so_uris WHERE (:o_type = :t_uri) AND uri = :o_uri
UNION
SELECT :part_o_b AS part, :t_blank AS node_type, id FROM so_blanks WHERE (:o_type = :t_blank) AND blank = :o_blank
UNION
SELECT :part_o_l AS part, :t_literal AS node_type, o_literals.id FROM o_literals
INNER JOIN t_uris ON o_literals.datatype = t_uris.id
WHERE (:o_type = :t_literal) AND (text = :o_text AND language = :o_language AND t_uris.uri = :o_datatype)
UNION
SELECT :part_datatype AS part, :t_uri AS node_type, id FROM t_uris WHERE (:o_type = :t_literal) AND uri = :o_datatype
UNION
SELECT :part_c AS part, :t_uri AS node_type, id FROM c_uris WHERE uri = :c_uri
ORDER BY part DESC, node_type ASC
