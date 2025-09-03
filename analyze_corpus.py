import os
import json
from pathlib import Path
from datetime import datetime
from typing import List

from src.ingestion import DocumentIngestionPipeline
from src.schema import LegalDocument, ProcessedChunk


def find_document_files(data_dir: Path) -> List[Path]:
	files: List[Path] = []
	if not data_dir.exists():
		return files
	for root, _, filenames in os.walk(str(data_dir)):
		for name in filenames:
			lower = name.lower()
			if lower.endswith(".xml") or lower.endswith(".pdf"):
				files.append(Path(root) / name)
	return files


def main():
	project_root = Path(__file__).parent
	data_dir = project_root / "data"
	output_dir = project_root / "corpus_output"

	pipeline = DocumentIngestionPipeline(output_dir=str(output_dir))

	file_paths = find_document_files(data_dir)
	if not file_paths:
		print("No .xml or .pdf files found under ./data")

	documents: List[LegalDocument] = []
	all_chunks: List[ProcessedChunk] = []

	for fp in file_paths:
		try:
			doc = pipeline.ingest_document(str(fp))
			documents.append(doc)
			chunks = pipeline.chunk_document(doc)
			all_chunks.extend(chunks)
			print(f"Ingested and chunked: {fp}")
		except Exception as e:
			print(f"Error processing {fp}: {e}")

	stats = pipeline.get_corpus_stats(documents)

	# Aggregate tag counts from chunks
	tag_counts = {}
	for ch in all_chunks:
		for tag in ch.metadata.tags:
			if tag:
				tag_counts[tag] = tag_counts.get(tag, 0) + 1

	analysis = {
		"generated_at": datetime.utcnow().isoformat(),
		"corpus_stats": stats,
		"num_chunks": len(all_chunks),
		"tag_counts": dict(sorted(tag_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
	}

	analysis_path = project_root / "corpus_analysis.json"
	with open(analysis_path, "w", encoding="utf-8") as f:
		json.dump(analysis, f, indent=2, ensure_ascii=False)

	print(f"Wrote analysis to {analysis_path}")
	print(f"Processed documents saved under {output_dir}")


if __name__ == "__main__":
	main() 