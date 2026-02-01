# Projeto Dados PL - Pipeline de Mobilidade Urbana

Solução de ETL completa para processamento de dados públicos de mobilidade urbana de Belo Horizonte. O pipeline extrai dados do Portal de Dados Abertos (PBH), realiza transformações estruturadas e carrega em um data warehouse, seguindo a arquitetura Medallion com boas práticas de governança, qualidade e teste de dados.

## Objetivo do Projeto

Este projeto implementa um pipeline de dados automatizado que:

- **Extrai** dados públicos de mobilidade urbana do Portal de Dados Abertos de Belo Horizonte
- **Armazena** em um data lake estruturado seguindo a arquitetura Medallion
- **Transforma** dados em múltiplas camadas (Bronze → Silver → Gold)
- **Carrega** em tabelas analíticas prontas para BI e Machine Learning
- **Valida** qualidade dos dados em todas as camadas
- **Documenta** mudanças e decisões técnicas

## Fonte de Dados

Os dados são obtidos do **Portal de Dados Abertos de Belo Horizonte**:

- **MCO (Mapa de Controle Operacional)**: Status operacional das linhas de ônibus em tempo real
- **GPS**: Posicionamento geográfico em tempo real dos veículos de transporte público
- **Linhas**: Cadastro das linhas de ônibus com informações de trajeto e operação

**Endpoint Base**: [dados.pbh.gov.br](https://dados.pbh.gov.br/)

---

## Arquitetura da Solução

### Diagrama da Arquitetura do Data Lake

```mermaid
graph TB
    subgraph SOURCES["Fontes Externas"]
        API["Portal de Dados Abertos PBH<br/>APIs REST"]
        API_MCO["dados.pbh.gov.br/mco"]
        API_GPS["dados.pbh.gov.br/gps"]
        API_LINHAS["dados.pbh.gov.br/linhas"]
        
        API --> API_MCO
        API --> API_GPS
        API --> API_LINHAS
    end
    
    subgraph LANDING["Landing Zone"]
        DOWN["Download HTTP<br/>Dados CSV brutos"]
    end
    
    subgraph BRONZE["Bronze Layer"]
        B_FORMAT["Formato: Parquet<br/>Imutável • Append-Only"]
        B_GPS["data/bronze/gps/"]
        B_MCO["data/bronze/mco/"]
        B_LINHAS["data/bronze/linhas/"]
    end
    
    subgraph SILVER["Silver Layer"]
        S_FORMAT["Formato: Delta Lake<br/>Versionado • ACID"]
        S_GPS["data/silver/gps/"]
        S_MCO["data/silver/mco/"]
        S_LINHAS["data/silver/linhas/"]
        S_VALID["✓ Validação de Qualidade<br/>✓ Deduplicação<br/>✓ Tipagem"]
    end
    
    subgraph GOLD["Gold Layer"]
        G_FORMAT["Formato: Delta Lake + CSV<br/>Analytics-Ready"]
        G_FATO["data/gold/fato_performance_diaria/"]
        G_CSV["data/gold/fato_performance_diaria_csv/"]
        G_METRICS["Métricas de Negócio<br/> KPIs Agregados"]
    end
    
    subgraph CONSUME["Consumo"]
        BI["Power BI / Tableau"]
        ML["Machine Learning"]
        REPORTS["Relatórios Gerenciais"]
    end
    
    API_MCO -.->|HTTP GET| DOWN
    API_GPS -.->|HTTP GET| DOWN
    API_LINHAS -.->|HTTP GET| DOWN
    
    DOWN -->|ingestao_bronze.py| B_FORMAT
    B_FORMAT --> B_GPS
    B_FORMAT --> B_MCO
    B_FORMAT --> B_LINHAS
    
    B_GPS -->|processar_gps.py| S_FORMAT
    B_MCO -->|processar_mco.py| S_FORMAT
    B_LINHAS -->|processar_linhas.py| S_FORMAT
    
    S_FORMAT --> S_GPS
    S_FORMAT --> S_MCO
    S_FORMAT --> S_LINHAS
    S_FORMAT -.-> S_VALID
    
    S_GPS -->|fato_viagem.py| G_FORMAT
    S_MCO -->|fato_viagem.py| G_FORMAT
    S_LINHAS -->|fato_viagem.py| G_FORMAT
    
    G_FORMAT --> G_FATO
    G_FORMAT --> G_CSV
    G_FORMAT -.-> G_METRICS
    
    G_FATO --> BI
    G_CSV --> REPORTS
    G_FATO --> ML
    
    style SOURCES fill:#0277bd,color:#fff,stroke:#01579b,stroke-width:3px
    style API fill:#0288d1,color:#fff
    style API_MCO fill:#039be5,color:#fff
    style API_GPS fill:#039be5,color:#fff
    style API_LINHAS fill:#039be5,color:#fff
    
    style LANDING fill:#f57c00,color:#fff,stroke:#e65100,stroke-width:3px
    style DOWN fill:#fb8c00,color:#fff
    
    style BRONZE fill:#d84315,color:#fff,stroke:#bf360c,stroke-width:3px
    style B_FORMAT fill:#e64a19,color:#fff
    style B_GPS fill:#ff6e40,color:#000
    style B_MCO fill:#ff6e40,color:#000
    style B_LINHAS fill:#ff6e40,color:#000
    
    style SILVER fill:#512da8,color:#fff,stroke:#311b92,stroke-width:3px
    style S_FORMAT fill:#5e35b1,color:#fff
    style S_GPS fill:#7e57c2,color:#fff
    style S_MCO fill:#7e57c2,color:#fff
    style S_LINHAS fill:#7e57c2,color:#fff
    style S_VALID fill:#9575cd,color:#fff
    
    style GOLD fill:#f9a825,color:#000,stroke:#f57f17,stroke-width:3px
    style G_FORMAT fill:#fbc02d,color:#000
    style G_FATO fill:#fdd835,color:#000
    style G_CSV fill:#fff59d,color:#000
    style G_METRICS fill:#ffee58,color:#000
    
    style CONSUME fill:#2e7d32,color:#fff,stroke:#1b5e20,stroke-width:3px
    style BI fill:#388e3c,color:#fff
    style ML fill:#388e3c,color:#fff
    style REPORTS fill:#388e3c,color:#fff
```

### Diagrama Estrutural do Fluxo ETL

```mermaid
graph TD
    subgraph "Dados de Entrada"
        MCO["MCO Dataset<br/>(API PBH)"]
        GPS["GPS Dataset<br/>(API PBH)"]
        LINHAS["Linhas Dataset<br/>(API PBH)"]
    end
    
    subgraph "Bronze Layer - Raw Data"
        BMCO["ingestao_mco.py"]
        BGPS["ingestao_gps.py"]
        BLINHAS["ingestao_linhas.py"]
        
        TB_BMCO[("bronze/mco/<br/>Parquet")]
        TB_BGPS[("bronze/gps/<br/>Parquet")]
        TB_BLINHAS[("bronze/linhas/<br/>Parquet")]
    end
    
    subgraph "Silver Layer - Data Cleaning"
        SMCO["processar_mco.py"]
        SGPS["processar_gps.py"]
        SLINHAS["processar_linhas.py"]
        
        TB_SMCO[("silver/mco/<br/>Delta Lake")]
        TB_SGPS[("silver/gps/<br/>Delta Lake")]
        TB_SLINHAS[("silver/linhas/<br/>Delta Lake")]
    end
    
    subgraph "Gold Layer - Data Mart"
        FATO["fato_viagem.py"]
        
        TB_FATO[("gold/fato_performance_diaria/<br/>Delta Lake")]
    end
    
    MCO --> BMCO
    GPS --> BGPS
    LINHAS --> BLINHAS
    
    BMCO --> TB_BMCO
    BGPS --> TB_BGPS
    BLINHAS --> TB_BLINHAS
    
    TB_BMCO --> SMCO
    TB_BGPS --> SGPS
    TB_BLINHAS --> SLINHAS
    
    SMCO --> TB_SMCO
    SGPS --> TB_SGPS
    SLINHAS --> TB_SLINHAS
    
    TB_SMCO --> FATO
    TB_SGPS --> FATO
    TB_SLINHAS --> FATO
    
    FATO --> TB_FATO
    
    style MCO fill:#c62828,color:#fff
    style GPS fill:#c62828,color:#fff
    style LINHAS fill:#c62828,color:#fff
    
    style BMCO fill:#d84315,color:#fff
    style BGPS fill:#d84315,color:#fff
    style BLINHAS fill:#d84315,color:#fff
    style TB_BMCO fill:#ff6e40,color:#000
    style TB_BGPS fill:#ff6e40,color:#000
    style TB_BLINHAS fill:#ff6e40,color:#000
    
    style SMCO fill:#512da8,color:#fff
    style SGPS fill:#512da8,color:#fff
    style SLINHAS fill:#512da8,color:#fff
    style TB_SMCO fill:#7e57c2,color:#fff
    style TB_SGPS fill:#7e57c2,color:#fff
    style TB_SLINHAS fill:#7e57c2,color:#fff
    
    style FATO fill:#f9a825,color:#000
    style TB_FATO fill:#fdd835,color:#000
```

## Estrutura de Diretórios

```
projeto-dados-pl/
├── README.md                         # Este arquivo
├── requirements.txt                  # Dependências Python
├── main.py                           # Ponto de entrada do pipeline
│
├── src/                              # Código-fonte
│   ├── config.py                     # Configurações globais (URLs, caminhos)
│   ├── spark_session.py              # Inicialização e configuração Spark
│   ├── utils/                        # Utilitários reutilizáveis
│   │   ├── download.py               # Download de dados das APIs
│   │   ├── quality.py                # Validações e checagens de qualidade
│   │   └── save.py                   # Escrita em Delta Lake / Parquet
│   │
│   └── etl/                          # Orquestração do pipeline
│       ├── ingestao_bronze.py        # Orquestrador da camada Bronze
│       ├── ingestao_silver.py        # Orquestrador da camada Silver
│       ├── ingestao_gold.py          # Orquestrador da camada Gold
│       │
│       ├── bronze/                   # Ingestão de dados brutos
│       │   ├── ingestao_gps.py
│       │   ├── ingestao_linhas.py
│       │   └── ingestao_mco.py
│       │
│       ├── silver/                   # Transformação e limpeza
│       │   ├── processar_gps.py
│       │   ├── processar_linhas.py
│       │   └── processar_mco.py
│       │
│       └── gold/                     # Agregações e data marts
│           └── fato_viagem.py
│
├── data/                             # Data Lake (não incluído no Git)
│   ├── bronze/                       # Dados brutos em Parquet
│   │   ├── gps/
│   │   ├── linhas/
│   │   └── mco/
│   ├── silver/                       # Dados transformados em Delta Lake
│   │   ├── gps/
│   │   ├── linhas/
│   │   └── mco/
│   └── gold/                         # Data Marts em Delta Lake
│       └── fato_performance_diaria/
│
├── dicionario/                       # Documentação de dados
│   ├── Tabela GPS.md                 # Dicionário da tabela GPS
│   ├── Tabela MCO.md                 # Dicionário da tabela MCO
│   └── Tabela Fato.md                # Dicionário da tabela Fato (Gold)
│
└── tests/                            # Testes automatizados
    ├── conftest.py                   # Fixtures e configurações Pytest
    └── test_quality.py               # Testes de validação de dados
```

---

## Uso e Execução

### Ambiente Python

Escolha **uma** das opções abaixo para garantir Python 3.11:

**Opção A — Python 3.11 local**

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python --version
```

**Opção B — uv (usa .python-version se existir)**

```bash
uv python install
uv venv
source .venv/bin/activate
```

### Instalar dependências

```bash
pip install -r requirements.txt
```

### Executar o Pipeline Completo

```bash
python main.py
```

### Executar Testes

```bash
# Todos os testes
pytest tests/ -v
```

---

## Fluxo de Dados e Processamento

### Etapa 1: Bronze - Ingestão
```
APIs PBH → Download → Parquet (Bronze)
```
- Download de dados via APIs públicas
- Armazenamento imutável em Parquet
- Sem transformações

### Etapa 2: Silver - Transformação
```
Parquet (Bronze) → Limpeza e Padronização → Delta Lake (Silver)
```
- Remoção de duplicatas
- Tratamento de nulos
- Validação de tipos e ranges
- Padronização de formatos

### Etapa 3: Gold - Agregação
```
Delta Lake (Silver) → Joins e Agregações → Delta Lake (Gold)
```
- Criação de fatos e dimensões
- Cálculo de métricas
- Exportação para BI/Analytics

---

## Documentação Complementar

Consulte os seguintes documentos para mais informações:

- **[Tabela GPS](./dicionario/Tabela%20GPS.md)** - Definições de campos e transformações
- **[Tabela MCO](./dicionario/Tabela%20MCO.md)** - Estrutura e lógica operacional
- **[Tabela Fato](./dicionario/Tabela%20Fato.md)** - Métricas de negócio na camada Gold

---