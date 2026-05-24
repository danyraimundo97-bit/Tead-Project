# Relatório TEAD — Compilação LaTeX (LLNCS)

## Pré-requisitos

- Distribuição LaTeX (TeX Live / MiKTeX) com `pdflatex`
- Pacotes: `llncs`, `babel-portuguese`, `booktabs`, `tabularx`, `graphicx`, `listings`, `hyperref`

## Compilar

Requer `llncs.cls` no PATH ou no mesmo directório (template Springer LNCS).

```bash
cd docs/Relatório
pdflatex main.tex
pdflatex main.tex   # segundo passe para índices (TOC, LOF, LOT)
```

O PDF resultante é `main.pdf`. Alternativa: importar `main.tex` + pasta `figuras/` no [Overleaf](https://www.overleaf.com) com template LLNCS.

> **Nota:** `pdflatex` não está no PATH desta máquina. Compilação via Docker requer Docker Desktop a correr e imagem TeX (ex.: `blang/latex:ubuntu` ou `texlive/texlive`).

**Estimativa:** ~12--14 páginas com figuras incluídas e placeholders Superset/Grafana (caixas compactas).

## Diagramas BPMN (bpmn.io)

Ficheiros BPMN 2.0 nativos em `docs/diagrams/`:

| Fluxo | Ficheiro |
|-------|----------|
| Batch (`jdpt_lakehouse_pipeline`) | [`batch-jdpt_lakehouse_pipeline.bpmn`](../diagrams/batch-jdpt_lakehouse_pipeline.bpmn) |
| Streaming (`jdpt_streaming_full_sync`) | [`streaming-jdpt_streaming_full_sync.bpmn`](../diagrams/streaming-jdpt_streaming_full_sync.bpmn) |

### Abrir no [bpmn.io](https://demo.bpmn.io/)

1. Ir a https://demo.bpmn.io/
2. **Open File** → escolher o `.bpmn`
3. Ajustar layout se necessário (arrastar nós)
4. **Export** → PNG ou SVG para o relatório

### Incluir no LaTeX (`main.tex`)

Exportar para `figuras/` com estes nomes (ou actualizar `\includegraphics`):

```
figuras/batch-bpmn.pdf      # ou .png
figuras/streaming-bpmn.pdf
```

Comando sugerido após export SVG/PNG: converter para PDF se o Overleaf preferir PDF.

## Outros diagramas (Mermaid — linhagem de dados)

Diagramas de linhagem/ER em `docs/diagrams/*.mmd` (opcional, anexos):

```bash
npx @mermaid-js/mermaid-cli -i ../diagrams/batch-flow.mmd -o figuras/batch-flow.pdf
```

## Placeholders a preencher antes da entrega

Substituir URLs em `main.tex`:

- `<link-er-detalhado-a-preencher>`
- `<link-grafana-dashboard-a-preencher>`
- `<link-superset-dashboard-a-preencher>`

Ou substituir os `\fbox{...}` por `\includegraphics{...}` quando tiver screenshots.

## Ficheiros relacionados

- Contratos: `docs/Data_Products - Data_Contracts/`
- Diagramas BPMN: `docs/diagrams/*.bpmn`
- Documentação técnica: `docs/transicao_*.md`, `docs/streaming.md`
