-- Rode no cliente SQL (DBeaver/DataGrip) ou:
-- docker exec -it lab-cdc-postgres psql -U lab -d origem

-- 1. Criar o slot de replicação (a "posição de leitura").
--    Atenção: slot abandonado enche o disco do banco. Veja o passo 5.
SELECT pg_create_logical_replication_slot('slot_lab', 'test_decoding');

-- 2. Gerar as três operações
INSERT INTO loja VALUES (1, 'Auto Center SP', 'PJ');
INSERT INTO loja VALUES (2, 'Veículos Norte', 'PJ');
UPDATE loja SET segmento = 'PF' WHERE id_loja = 1;
DELETE FROM loja WHERE id_loja = 2;

-- 3. Ler o log (peek lê SEM avançar a posição; pode repetir à vontade)
SELECT lsn, xid, data FROM pg_logical_slot_peek_changes('slot_lab', NULL, NULL);

-- 4. get lê E consome. Só use quando for processar de verdade.
-- SELECT lsn, xid, data FROM pg_logical_slot_get_changes('slot_lab', NULL, NULL);

-- 5. Ao terminar de brincar, apague o slot:
-- SELECT pg_drop_replication_slot('slot_lab');
