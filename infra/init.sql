CREATE TABLE loja (
    id_loja       INT PRIMARY KEY,
    nome          TEXT NOT NULL,
    segmento      TEXT,
    atualizado_em TIMESTAMP DEFAULT now()
);

-- REPLICA IDENTITY FULL faz o log guardar a linha inteira no delete.
-- Sem isso você só recebe a chave primária e não consegue auditar
-- o que foi apagado.
ALTER TABLE loja REPLICA IDENTITY FULL;

-- publicação: quais tabelas entram no fluxo de CDC
CREATE PUBLICATION pub_lab FOR TABLE loja;
