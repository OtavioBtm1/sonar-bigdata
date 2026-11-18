-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 - Camada Ouro: Modelo Dimensional (Estrela), Proteção LGPD e 5 Consultas SQL
-- MAGIC **Projeto:** Sonar (`https://sonar.rodrigoviana.app.br` | Repo: `https://github.com/RodrigoResendeViana/sonar-blind`)  
-- MAGIC **Requisitos atendidos:** Requisito 07 (1 Fato + 3 Dimensões e 5 Consultas SQL, sendo 1 de impacto) e Requisito 08 (Proteção LGPD na Ouro espelhando `api/sonar_api/seguranca.py` e uso pelo MVP em `/api/painel/indicadores`).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Criação das 3 Dimensões (`ouro_dim_linha`, `ouro_dim_parada`, `ouro_dim_tempo`)

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_dim_linha AS
WITH intervalos AS (
    SELECT
        linha_letreiro,
        ROUND(AVG(intervalo_observado_min), 1) AS intervalo_historico_pico_min
    FROM workspace.gs.prata_olhovivo_posicao
    GROUP BY linha_letreiro
)
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(g.codigo_id AS BIGINT)) AS sk_linha,
    CAST(g.codigo_id AS BIGINT) AS codigo_cl,
    g.codigo_letreiro AS linha_letreiro,
    g.nome_descritivo AS nome_destino,
    g.bairro_ou_operadora AS operadora,
    COALESCE(i.intervalo_historico_pico_min, 8.5) AS intervalo_historico_pico_min
FROM workspace.gs.prata_gtfs g
LEFT JOIN intervalos i ON g.codigo_letreiro = i.linha_letreiro
WHERE g.tipo_registro = 'LINHA';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_dim_parada AS
SELECT
    ROW_NUMBER() OVER (ORDER BY codigo_id) * 10 AS sk_parada,
    codigo_id AS parada_id,
    CAST(codigo_letreiro AS BIGINT) AS codigo_sptrans,
    nome_descritivo AS nome_parada,
    bairro_ou_operadora AS bairro,
    latitude,
    longitude,
    COALESCE(possui_sonar_ponto, FALSE) AS possui_sonar_ponto
FROM workspace.gs.prata_gtfs
WHERE tipo_registro = 'PARADA';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_dim_tempo AS
SELECT
    ROW_NUMBER() OVER (ORDER BY criado_em) + 100 AS sk_tempo,
    criado_em,
    data_coleta,
    HOUR(criado_em) AS hora,
    CASE
        WHEN HOUR(criado_em) BETWEEN 6 AND 9 OR HOUR(criado_em) BETWEEN 17 AND 20 THEN 'Pico'
        ELSE 'Entre-Pico'
    END AS faixa_horaria
FROM (
    SELECT DISTINCT criado_em, data_coleta
    FROM workspace.gs.prata_esperas_sonar
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Tabela Fato (`ouro_fato_espera`) com Hash SHA-256 + Sal e Máscara (LGPD Art. 5º, II e Art. 11)
-- MAGIC Reproduz exatamente a função `hash_usuario(sal, usuario)` de `api/sonar_api/seguranca.py` e o mapa de dados de `docs/lgpd.md`:
-- MAGIC * `usuario_id_bruto` vira `SHA2(CONCAT('SONAR_SALT_2026:', usuario_id_bruto), 256)` (`usuario_hash`).
-- MAGIC * `ocorrencia_protocolo` recebe máscara parcial (`SIM-****-XXX`).
-- MAGIC * Coordenadas individuais nunca entram na Fato; apenas a chave da parada (`sk_parada`).

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.ouro_fato_espera AS
SELECT
    e.espera_id,
    l.sk_linha,
    p.sk_parada,
    t.sk_tempo,
    SHA2(CONCAT('SONAR_SALT_2026:', e.usuario_id_bruto), 256) AS usuario_hash,
    CASE
        WHEN e.ocorrencia_protocolo IS NOT NULL THEN CONCAT('SIM-****-', RIGHT(e.ocorrencia_protocolo, 3))
        ELSE NULL
    END AS protocolo_mascarado,
    e.origem,
    e.status,
    e.prefixo_avisado,
    e.aviso_enviado,
    e.motorista_ciente,
    e.validado_por_camera,
    e.passagem_confianca,
    e.passagem_tempo_parado_s,
    e.passagem_parou,
    e.tempo_espera_min,
    e.ocorrencia_consentimento
FROM workspace.gs.prata_esperas_sonar e
JOIN workspace.gs.ouro_dim_linha l ON e.passagem_linha = l.linha_letreiro
JOIN workspace.gs.ouro_dim_parada p ON e.parada_id = p.parada_id
JOIN workspace.gs.ouro_dim_tempo t ON e.criado_em = t.criado_em;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. As 5 Consultas SQL do MVP Sonar (Requisito 07)

-- COMMAND ----------

-- CONSULTA 1: Intervalo histórico vs. Espera média real por linha e faixa horária (Plano B quando o Olho Vivo cai)
SELECT
    l.linha_letreiro,
    l.nome_destino,
    l.operadora,
    t.faixa_horaria,
    COUNT(f.espera_id) AS total_esperas,
    ROUND(AVG(l.intervalo_historico_pico_min), 1) AS intervalo_historico_min,
    ROUND(AVG(f.tempo_espera_min), 1) AS espera_media_real_min
FROM workspace.gs.ouro_fato_espera f
JOIN workspace.gs.ouro_dim_linha l ON f.sk_linha = l.sk_linha
JOIN workspace.gs.ouro_dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY l.linha_letreiro, l.nome_destino, l.operadora, t.faixa_horaria
ORDER BY l.linha_letreiro, t.faixa_horaria;

-- COMMAND ----------

-- CONSULTA 2: Esperas por parada, acionamentos do botão físico do poste e priorização de novos Sonar Ponto
SELECT
    p.parada_id,
    p.codigo_sptrans,
    p.nome_parada,
    p.bairro,
    CASE WHEN p.possui_sonar_ponto THEN 'Instalado' ELSE 'Prioridade Expansão' END AS status_hardware,
    COUNT(f.espera_id) AS total_esperas,
    SUM(CASE WHEN f.origem = 'botao_poste' THEN 1 ELSE 0 END) AS acionamentos_botao_poste,
    SUM(CASE WHEN f.status = 'nao_parou' THEN 1 ELSE 0 END) AS onibus_nao_pararam
FROM workspace.gs.ouro_fato_espera f
JOIN workspace.gs.ouro_dim_parada p ON f.sk_parada = p.sk_parada
GROUP BY p.parada_id, p.codigo_sptrans, p.nome_parada, p.bairro, p.possui_sonar_ponto
ORDER BY total_esperas DESC;

-- COMMAND ----------

-- CONSULTA 3: Percentual de avisos com confirmação "Ciente" no Sonar Bordo por operadora e linha
SELECT
    l.operadora,
    l.linha_letreiro,
    COUNT(f.espera_id) AS avisos_enviados,
    SUM(CASE WHEN f.motorista_ciente THEN 1 ELSE 0 END) AS avisos_ciente,
    ROUND(100.0 * SUM(CASE WHEN f.motorista_ciente THEN 1 ELSE 0 END) / COUNT(f.espera_id), 1) AS taxa_ciente_pct
FROM workspace.gs.ouro_fato_espera f
JOIN workspace.gs.ouro_dim_linha l ON f.sk_linha = l.sk_linha
GROUP BY l.operadora, l.linha_letreiro
ORDER BY taxa_ciente_pct DESC;

-- COMMAND ----------

-- CONSULTA 4 (CONSULTA DE IMPACTO DO MVP): Taxa de embarque na 1ª tentativa e tempo médio de espera (Com Ciente vs. Sem Ciente)
SELECT
    CASE WHEN f.motorista_ciente THEN 'Com Ciente do Motorista (Sonar Bordo)' ELSE 'Sem Confirmação Ciente' END AS cenario,
    COUNT(f.espera_id) AS total_esperas,
    SUM(CASE WHEN f.status = 'embarcou' THEN 1 ELSE 0 END) AS embarques_1a_tentativa,
    ROUND(100.0 * SUM(CASE WHEN f.status = 'embarcou' THEN 1 ELSE 0 END) / COUNT(f.espera_id), 1) AS taxa_embarque_pct,
    ROUND(AVG(f.tempo_espera_min), 1) AS tempo_medio_espera_min
FROM workspace.gs.ouro_fato_espera f
GROUP BY f.motorista_ciente
ORDER BY f.motorista_ciente DESC;

-- COMMAND ----------

-- CONSULTA 5: Ônibus que passaram direto por 100 esperas e ocorrências enviadas ao robô RPA (n8n -> SP156)
SELECT
    l.operadora,
    l.linha_letreiro,
    t.faixa_horaria,
    COUNT(f.espera_id) AS esperas,
    SUM(CASE WHEN f.status = 'nao_parou' THEN 1 ELSE 0 END) AS nao_parou,
    SUM(CASE WHEN f.status = 'nao_parou' AND f.ocorrencia_consentimento THEN 1 ELSE 0 END) AS enviadas_rpa_n8n,
    ROUND(100.0 * SUM(CASE WHEN f.status = 'nao_parou' THEN 1 ELSE 0 END) / COUNT(f.espera_id), 1) AS nao_parou_por_100_esperas
FROM workspace.gs.ouro_fato_espera f
JOIN workspace.gs.ouro_dim_linha l ON f.sk_linha = l.sk_linha
JOIN workspace.gs.ouro_dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY l.operadora, l.linha_letreiro, t.faixa_horaria
HAVING SUM(CASE WHEN f.status = 'nao_parou' THEN 1 ELSE 0 END) > 0
ORDER BY nao_parou DESC, nao_parou_por_100_esperas DESC;
