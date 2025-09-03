## SLTI Research: Legal Document Ingestion and Analysis

### Overview
This repository provides a small, focused pipeline to parse infrastructure/legal documents (PDF and XML), normalize and enrich them, chunk them for retrieval, and optionally index them into Postgres (pgvector). It also includes a driver that scans `./data` and writes a high-level `corpus_analysis.json` summary plus per-document JSONs to `./corpus_output`.

### Key Components
- **`src/schema.py`**: Pydantic models defining the canonical structure of a `LegalDocument`, its `Section`s, `Definition`s, and `ProcessedChunk`s, along with legal term synonym maps.
- **`src/parsers/pdf_parser.py`**: `PDFLegalParser` built on PyMuPDF. Extracts metadata, sections, and definitions from PDFs.
- **`src/parsers/xml_parser.py`**: `XMLLegalParser` for TEI-like XML. Extracts metadata, sections, and definitions from XML.
- **`src/ingestion.py`**: `DocumentIngestionPipeline` orchestrates parsing, terminology/tag normalization, chunking, saving processed docs, and basic corpus stats.
- **`src/indexing.py`**: Optional `PgIndexer` for inserting documents and chunks into Postgres with pgvector and simple vector search helpers.
- **`analyze_corpus.py`**: Driver script that walks `./data` for `.xml` and `.pdf`, runs the pipeline, aggregates tag counts, and writes `corpus_analysis.json`.
- **`sql/init.sql`**: SQL to initialize required tables and pgvector extension in Postgres.

### How It Fits Together
```mermaid
flowchart TD
  A["./data/*.pdf, *.xml"] --> B["Parsers: PDFLegalParser, XMLLegalParser"]
  B --> C["LegalDocument (schema)"]
  C --> D["Normalization: terminology + tags"]
  D --> E["Chunking → ProcessedChunk"]
  E --> F["Write JSON per doc to ./corpus_output"]
  E --> G["Tag counts from chunks"]
  C --> H["Corpus stats"]
  F --> I["corpus_analysis.json"]
  G --> I
  H --> I
  E --> J["(Optional) PgIndexer → Postgres (pgvector)"]
```

### Installation
You can use Conda or pip. Example with Conda (CPU):

```bash
# From repository root
conda env create -f setup/environment-cpu.yml
conda activate slti-research
```

Or with pip:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r setup/requirements.txt
```

Requirements of note:
- PyMuPDF (`fitz`) for PDF parsing
- Pydantic for schema validation
- psycopg for Postgres (only if using indexing)

### Prepare Input Data
Place your `.pdf` and `.xml` files anywhere under the `./data` directory. The driver will recursively discover them.

### Run: Ingest, Chunk, and Analyze
The driver will:
- Walk `./data` for `*.xml` / `*.pdf`
- Use `DocumentIngestionPipeline(output_dir="./corpus_output")`
- Ingest and chunk each document
- Compute `pipeline.get_corpus_stats(docs)` and tag counts from chunks
- Write `corpus_analysis.json` at the project root
- Save per-document JSONs in `./corpus_output`

```bash
python analyze_corpus.py
```

Outputs:
- `corpus_analysis.json` (at project root)
- `corpus_output/` containing one JSON per processed document (filename = `document_id.json`)

### DocumentIngestionPipeline Details
- Parses using `PDFLegalParser` or `XMLLegalParser` based on extension
- Normalizes terminology using synonym maps in `LEGAL_TERM_SYNONYMS`
- Enhances tags using `INFRA_FINANCE_TERMS` and section/title heuristics
- Chunks sections with overlap and sentence-aware splitting; also creates chunks for definitions
- Saves the normalized `LegalDocument` as JSON to `./corpus_output`
- Provides `get_corpus_stats(documents)` to summarize document counts by industry, jurisdiction, clause types, and document types

### Optional: Index Into Postgres (pgvector)
If you want to index chunks into Postgres for vector search:

1) Provision Postgres with pgvector and base tables:
```bash
# Adjust connection parameters as needed
psql "postgresql://postgres:changeme_local_pw@localhost:5433/infra_rag" -f sql/init.sql
```

2) Environment variables (defaults shown):
- `DB_HOST`=`localhost`
- `DB_PORT`=`5433`
- `DB_NAME`=`infra_rag`
- `DB_USER`=`postgres`
- `DB_PASSWORD`=`changeme_local_pw`
- `EMBED_MODEL`=`sentence-transformers/all-MiniLM-L6-v2` (optional)

3) Run the indexer as a module so relative imports resolve:
```bash
# Example: index two files (embeddings enabled by default)
python -m src.indexing data/sample1.pdf data/sample2.xml

# Or skip embeddings
python -m src.indexing --no-embed data/sample1.pdf
```
This will:
- Ingest and chunk each input file (same pipeline)
- Optionally compute embeddings with SentenceTransformers
- Upsert into `documents` and `clauses` (with helpful columns for headings, defined terms, etc.)

### Data Model Snapshot
- **`LegalDocument`**: `metadata`, `sections`, `definitions`
- **`Section`**: `id`, `title`, `text`, `clause_type`, `tags`, `definitions`, `parent_section`, `page_number`
- **`Definition`**: `term`, `definition`, `section_id`
- **`ProcessedChunk`**: `metadata` (chunk/document/section IDs, tags, citation, optional hierarchical anchors), `content`, optional `embedding`

### Troubleshooting
- No files found: ensure inputs are `.pdf` or `.xml` under `./data` (recursive).
- PDF parsing errors: verify PDFs are text-extractable (PyMuPDF can fail on image-only scans without OCR).
- Import errors when running CLI under `src/`: invoke with `python -m src.indexing ...` from the repo root so package-relative imports work.
- Postgres connection errors: confirm env vars and that `sql/init.sql` was executed in the target database.

### Project Layout
- `src/` ingestion, parsers, schema, and optional indexer
- `data/` your input documents
- `corpus_output/` processed per-document JSONs (generated)
- `corpus_analysis.json` corpus summary (generated)
- `sql/` Postgres initialization
- `setup/` environment setup assets (Conda envs, pip requirements)
- `execution.ipynb` example notebook workspace

### License
Internal research codebase; if you have any questions feel free to email hshindy@stanford.edu. 
