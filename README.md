# UniasselviBD

![photo_2026-02-28_17-12-27](https://github.com/user-attachments/assets/d6823a48-87e7-4a1b-8b51-3d2a6cc238b0)


    
<img width="8192" height="5698" alt="Core Telemetry Analytics-2026-02-28-195630" src="https://github.com/user-attachments/assets/a0f198c3-f9a0-48a2-9ec2-d8d7e2dc541f" />


<img width="2074" height="8192" alt="Core Telemetry Analytics-2026-02-28-195544" src="https://github.com/user-attachments/assets/4f9c58c6-3a94-427f-b780-8127196a8dce" />


# Faculda Data Platform

Arquitetura SQL Server em camadas para um sistema academico e financeiro com observabilidade completa.

## Visao Geral

Este repositorio implementa uma plataforma de dados enterprise com foco em:

- dominio transacional (`core`)
- auditoria e rastreabilidade (`log`)
- eventos e metricas de uso (`telemetry`)
- analise e indicadores (`analytics`)

O projeto foi desenhado para demonstrar modelagem relacional avancada, procedures orientadas a operacao real, governanca de acesso (DCL) e camada analitica pronta para BI.

## Arquitetura

Camadas principais:

- `core`: entidades de negocio (aluno, curso, turma, matricula, avaliacao, financeiro).
- `security`: usuarios internos e perfis operacionais.
- `log`: auditoria de mudancas, erros, execucoes de processo e acesso.
- `telemetry`: sessoes, stream de eventos, agregacao por minuto.
- `analytics`: dimensoes, fatos e KPIs diarios para consumo gerencial.

## Estrutura do Repositorio

- `faculda.sql`: cria toda a base, schemas, tabelas, constraints, indices, columnstore e carga inicial.
- `complementacao.sql`: funcoes, procedures, views, triggers, grants e bootstrap analitico.
- `faculda.mmd`: diagrama ER completo da arquitetura.
- `complementacao.mmd`: diagrama de dependencias de objetos da camada de complementacao.
- `faculda_diagrama_arquitetura.mmd`: versao detalhada do ER por camadas.
- `complementacao_diagrama_dependencias.mmd`: versao detalhada do grafo de dependencias.

## Funcionalidades Tecnicas

- DDL completa com PK, FK, CHECK, UNIQUE e defaults.
- DML de seed para ambiente de demonstracao.
- DQL com views executivas e analiticas.
- DCL com roles (`rl_core_ops`, `rl_finance_ops`, `rl_analytics_ops`, `rl_observer`).
- funcoes para CRA, saldo financeiro e chave temporal.
- procedures para matricula, nota final, pagamento, ingestao de eventos e rebuild de warehouse.
- triggers para auditoria, derivacao de nota final e rollup financeiro/telemetria.
- `NONCLUSTERED COLUMNSTORE INDEX` em fatos para desempenho analitico.

## Como Executar

### Opcao 1: SSMS

1. Abra `faculda.sql` e execute completo.
2. Abra `complementacao.sql` e execute completo.
3. Consulte as views:
   - `analytics.vw_VisaoUnicaFaculda`
   - `analytics.vw_PainelExecutivo`
   - `analytics.vw_RankingTurma`

### Opcao 2: sqlcmd

```powershell
sqlcmd -S .\LOREZZ -E -b -i faculda.sql
sqlcmd -S .\LOREZZ -E -b -i complementacao.sql
```

> Ajuste a instancia (`-S`) para o seu ambiente.

## Diagramas Mermaid

Visualize os `.mmd` em:

- GitHub (preview nativo Mermaid)
- Mermaid Live Editor
- extensoes Mermaid no VS Code

## Boas Praticas para Publicar no GitHub

- publique apenas arquivos de codigo/documentacao (`.sql`, `.mmd`, `README`).
- nao publique backups reais (`.bak`), dados sensiveis, credenciais ou logs de producao.
- inclua um `.gitignore` para artefatos de banco.

Exemplo minimo de `.gitignore`:

```gitignore
*.bak
*.mdf
*.ldf
*.trn
*.log
```

## Roadmap

- particionamento temporal dos fatos analiticos.
- jobs SQL Agent para refresh incremental.
- policy de retencao por camada (hot/warm/cold).
- camada de qualidade de dados com regras automatizadas.

## Autor

- Projeto base: plataforma `Faculda Data Platform`
- Estrutura e modelagem: foco em portfolio tecnico de engenharia de dados SQL Server

---

Trabalho D Faculdade.. Empresarial

Extras ..

<img width="786" height="449" alt="12" src="https://github.com/user-attachments/assets/c82fb7d8-4e50-42bb-8f2b-8ae3dbe3a031" />

<img width="842" height="668" alt="12hg" src="https://github.com/user-attachments/assets/24399558-3753-44fc-8dd5-ddbbb7b942a2" />


