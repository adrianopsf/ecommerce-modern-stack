# ecommerce-modern-stack

Pipeline ELT orquestrado com **Apache Airflow 2.8**, transformações em camadas com **dbt Core 1.7** e **PostgreSQL 15** como data warehouse, usando o dataset [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

## Stack

| Camada | Tecnologia |
|--------|-----------|
| Orquestração | Apache Airflow 2.8 (CeleryExecutor) |
| Transformação | dbt Core 1.7 |
| Data Warehouse | PostgreSQL 15 |
| Infraestrutura | Docker Compose |
| Testes | pytest + dbt tests |
| Qualidade | ruff |

## Arquitetura

```
data/raw/ (CSVs Olist)
    │
    ▼
[load_raw_to_postgres.py]
    │
    ▼
PostgreSQL: schema raw
    │
    ▼
[dbt staging]  →  views (limpeza e tipagem)
    │
    ▼
[dbt intermediate]  →  ephemeral (enriquecimento)
    │
    ▼
[dbt mart]  →  tables (agregações analíticas)
```

## Pré-requisitos

- Docker e Docker Compose
- Python 3.11+
- Dataset Olist extraído em `data/raw/`

## Início rápido

```bash
# 1. Copie e configure variáveis
cp .env.example .env

# 2. Inicialize o banco e o Airflow
make init

# 3. Suba todos os serviços
make up

# 4. Carregue os dados brutos
make load-raw

# 5. Execute as transformações dbt
make dbt-run

# 6. Rode os testes
make dbt-test
make test
```

Airflow UI: http://localhost:8080 (admin / admin)

## Estrutura

```
.
├── airflow/
│   ├── dags/          # DAGs de ingestão e transformação
│   ├── plugins/       # Operadores customizados
│   └── tests/         # Testes das DAGs
├── dbt/olist_dw/
│   ├── models/
│   │   ├── staging/       # Views de limpeza
│   │   ├── intermediate/  # Modelos efêmeros de enriquecimento
│   │   └── mart/          # Tabelas analíticas finais
│   ├── tests/         # Testes customizados dbt
│   ├── macros/        # Macros reutilizáveis
│   └── seeds/         # Dados de referência estáticos
├── scripts/           # Scripts utilitários
├── data/raw/          # CSVs Olist (gitignored)
└── docker-compose.yml
```

## Modelos dbt

### Staging
| Modelo | Descrição |
|--------|-----------|
| `stg_orders` | Pedidos com datas tipadas |
| `stg_customers` | Clientes com geolocalização |
| `stg_order_items` | Itens de pedido com preços |
| `stg_products` | Produtos com categorias |
| `stg_order_payments` | Pagamentos por pedido |
| `stg_sellers` | Vendedores por estado |

### Mart
| Modelo | Descrição |
|--------|-----------|
| `mart_orders` | Visão completa de pedidos |
| `mart_customers` | Perfil e histórico de clientes |
| `mart_products` | Performance por produto |
| `mart_monthly_sales` | Vendas mensais agregadas |
