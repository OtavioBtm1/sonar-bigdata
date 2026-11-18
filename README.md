# Sonar — Componente Dados (Big Data Architecture & Data Integration)
**FIAP Global Solution 2026.2 — Engenharia de Software (4º Ano - 4ES)**

## 1. Integrantes da Equipe
* **Otávio Vitoriano da Silva** — RM: `[PREENCHER_RM]`
* **Rodrigo Viana** — RM: `[PREENCHER_RM]`
* **`[Integrante 3]`** — RM: `[PREENCHER_RM]`
* **`[Integrante 4]`** — RM: `[PREENCHER_RM]`
* **`[Integrante 5]`** — RM: `[PREENCHER_RM]`

---

## 2. Ficha Técnica do MVP (Base Comum da Equipe)

| Campo | Descrição |
| :--- | :--- |
| **Nome do produto** | **Sonar** |
| **Cidadão e território** | Pessoas cegas e com baixa visão que utilizam ônibus municipais em São Paulo; piloto no corredor da Zona Leste (Itaquera — Radial Leste / Águia de Haia). |
| **Problema, em uma frase** | No ponto de ônibus, quem não enxerga não sabe qual veículo parou à sua frente, e o motorista não sabe que há um passageiro cego aguardando embarque. |
| **Proposta de valor** | Pegar o ônibus certo com autonomia sem depender de desconhecidos: o Sonar confirma o veículo por visão computacional no ponto, avisa o motorista duas paradas antes com confirmação ("ciente") e gera prova auditável quando o ônibus não para. |
| **Arquitetura (Componentes)** | App de voz acessível (PWA), API Sonar (Hub HTTPS), Agente de IA local (Ollama + RAG), Sonar Ponto (ESP32 + Ultrassom + Câmera YOLO/OCR), Sonar Bordo (ESP32 Bipper via MQTT), Pipeline de Dados (Airflow 3 + Databricks Lakehouse Medallion), Robô de Ocorrências (RPA SP156) e Deploy DevSecOps (Terraform + GitHub Actions). |
| **Fontes de dados** | 1. **API SPTrans Olho Vivo** (`/Posicao` e `/Previsao`, JSON em tempo real); 2. **GTFS SPTrans** (linhas e paradas abertas em CSV); 3. **Eventos Operacionais do Sonar** (esperas, avisos, confirmações "ciente" e detecções em JSON). |
| **Modelos de IA utilizados** | LLM aberto local (Ollama) com RAG e ferramentas + Detector customizado YOLO nano com OCR e vocabulário restrito ao GTFS. |
| **URL do MVP e Repositório** | MVP: `https://sonar.rodrigoviana.app.br` / GitHub: `https://github.com/RodrigoResendeViana/sonar-blind` |
| **Indicadores de impacto** | Taxa de embarque na 1ª tentativa (94,1% com aviso confirmado vs. 40,6% sem confirmação); ônibus que passaram direto a cada 100 esperas; tempo médio de espera em minutos; percentual de avisos com "ciente" por operadora. |
| **ODS conectados** | **ODS 11.2** (Sistemas de transporte seguros, acessíveis e sustentáveis para pessoas com deficiência) e **ODS 10.2** (Empoderar e promover a inclusão social, econômica e política de todos). |

---

## 3. Arquitetura do Pipeline de Dados (Airflow -> Databricks Medallion -> MVP)

```
[API SPTrans Olho Vivo] ──┐
[Base Aberta GTFS CSV]  ──┼─► Airflow 3 (DAG: sonar_coleta @daily) ─► Pasta Local (chegada/)
[Eventos App Sonar]     ──┘        │ (Paralelismo + conferir_chegada + Variable s/ chave no código)
                                   ▼
                     Upload em 2 Lotes ao Volume Databricks (/Volumes/workspace/gs/chegada)
                                   │
                                   ▼
                     [Camada BRONZE] Auto Loader (cloudFiles + checkpointLocation)
                       ├─ workspace.gs.bronze_olhovivo
                       ├─ workspace.gs.bronze_gtfs
                       └─ workspace.gs.bronze_esperas_sonar
                                   │
                                   ▼
                     [Camada PRATA] Tipagem + 3 Regras de Qualidade + MERGE INTO pela chave
                       ├─ workspace.gs.prata_gtfs
                       ├─ workspace.gs.prata_olhovivo_posicao
                       └─ workspace.gs.prata_esperas_sonar
                                   │
                                   ▼
                     [Camada OURO] Modelo Estrela + Proteção LGPD (SHA-256 + Máscara)
                       ├─ Fato: workspace.gs.ouro_fato_espera
                       ├─ Dimensões: ouro_dim_linha, ouro_dim_parada, ouro_dim_tempo
                       └─ 5 Consultas SQL ─► Exportação p/ Postgres ─► Painel da Cidade & Plano B do App
```

---

## 4. Estrutura do Repositório

* `dags/sonar_coleta.py`: DAG do Airflow 3 agendada (`@daily`), com 3 tarefas de coleta em paralelo (`coletar_olhovivo`, `baixar_gtfs`, `exportar_esperas_sonar`) e 1 tarefa validadora (`conferir_chegada`). Lê a chave da SPTrans via `Variable.get("SPTRANS_OLHOVIVO_TOKEN")`.
* `databricks/01_bronze_ingestao.py`: Notebook PySpark com ingestão incremental via Auto Loader (`cloudFiles`) e query de auditoria de contagens antes/depois do 2º lote e da 2ª execução.
* `databricks/02_prata_tratamento_qualidade.py`: Notebook PySpark que explode os veículos do Olho Vivo, aplica as 3 regras de qualidade (contabilizando os registros barrados por cada regra) e realiza `MERGE INTO` sem duplicação.
* `databricks/03_ouro_dimensional_lgpd_sql.sql`: Script SQL que constrói o esquema estrela (`ouro_fato_espera`, `ouro_dim_linha`, `ouro_dim_parada`, `ouro_dim_tempo`), aplica anonimização LGPD (SHA-256 com sal e máscara de CPF/telefone) e executa as 5 consultas analíticas e de impacto do MVP.
* `amostras/`: Amostras reais dos arquivos brutos gerados na pasta `chegada/` para cada uma das 3 fontes.
* `chegada_lote1/` e `chegada_lote2/`: Arquivos separados por dias de coleta (`2026-11-09` a `2026-11-11` no Lote 1; `2026-11-12` e `2026-11-13` no Lote 2) prontos para upload no Volume do Databricks Free Edition.

---

## 5. Passo a Passo para Reproduzir o Pipeline

1. **No Apache Airflow 3:**
   * No menu **Admin > Variables**, crie a variável `SPTRANS_OLHOVIVO_TOKEN` com o token gratuito da API Olho Vivo.
   * Copie `dags/sonar_coleta.py` para a pasta `dags/` do seu Airflow e ative a DAG `sonar_coleta`.
   * Os arquivos coletados serão gravados em `/opt/airflow/chegada/{olhovivo,gtfs,eventos}/` com a data no nome.
2. **No Databricks Free Edition (Upload em 2 Lotes):**
   * Crie o schema `workspace.gs` e o Volume `/Volumes/workspace/gs/chegada` (com as subpastas `olhovivo`, `gtfs` e `eventos`).
   * **Lote 1:** Faça upload dos arquivos de `chegada_lote1/` (dias 09/11 a 11/11) pela interface do Volume e execute o notebook `01_bronze_ingestao.py`. Registre o print da contagem inicial.
   * **Lote 2:** Faça upload dos arquivos de `chegada_lote2/` (dias 12/11 e 13/11) no mesmo Volume e execute novamente `01_bronze_ingestao.py`. Registre o print comprovando que apenas os novos arquivos entraram.
   * **Teste de Idempotência (2ª execução):** Execute `01_bronze_ingestao.py` mais uma vez sem subir arquivos novos e comprove que a contagem permanece idêntica.
3. **Execução das Camadas Prata e Ouro:**
   * Execute `02_prata_tratamento_qualidade.py` para visualizar a tabela de auditoria das 3 regras de qualidade e atualizar as tabelas Prata via `MERGE INTO`.
   * Execute `03_ouro_dimensional_lgpd_sql.sql` para criar o modelo dimensional Ouro com proteção LGPD e rodar as 5 consultas SQL do MVP.
