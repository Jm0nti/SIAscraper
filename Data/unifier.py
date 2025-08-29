from pathlib import Path
import csv
import sys


ROOT = Path(__file__).resolve().parent
FAC_PREFIX = "Facultad_"


def find_facultad_dirs(root: Path):
	return [p for p in root.iterdir() if p.is_dir() and p.name.startswith(FAC_PREFIX)]


def read_rows(path: Path):
	with path.open(newline='', encoding='utf-8') as f:
		reader = csv.DictReader(f)
		rows = list(reader)
		header = reader.fieldnames or []
	return header, rows


def write_rows(path: Path, header, rows):
	with path.open('w', newline='', encoding='utf-8') as f:
		writer = csv.DictWriter(f, fieldnames=header, extrasaction='ignore')
		writer.writeheader()
		for r in rows:
			writer.writerow(r)


def unify_by_key(files, key_column, out_path: Path):
	seen = {}
	header_union = None
	for p in files:
		if not p.exists():
			print(f"warning: file not found {p}")
			continue
		header, rows = read_rows(p)
		if header_union is None:
			header_union = header
		for r in rows:
			key = r.get(key_column, '').strip()
			if key == '':
				# skip rows without key
				continue
			if key not in seen:
				seen[key] = r
	if header_union is None:
		header_union = [key_column]
	out_rows = list(seen.values())
	write_rows(out_path, header_union, out_rows)
	print(f"Wrote {len(out_rows)} rows to {out_path}")


def unify_by_row(files, out_path: Path):
	header_union = None
	seen = set()
	out_rows = []
	for p in files:
		if not p.exists():
			print(f"warning: file not found {p}")
			continue
		header, rows = read_rows(p)
		if header_union is None:
			header_union = header
		for r in rows:

			if header_union is None:
				h = list(r.keys())
			else:
				h = header_union
			key = tuple((r.get(col, '').strip() for col in h))
			if key not in seen:
				seen.add(key)
				# create normalized row dict matching header_union
				normalized = {col: r.get(col, '').strip() for col in h}
				out_rows.append(normalized)
	if header_union is None:
		header_union = []
	write_rows(out_path, header_union, out_rows)
	print(f"Wrote {len(out_rows)} rows to {out_path}")


def main():
	dirs = find_facultad_dirs(ROOT)
	print(f"Found {len(dirs)} Facultad_ directories: {[d.name for d in dirs]}")

	# Prepare file lists
	asignaturas_files = [d / 'Asignaturas.csv' for d in dirs]
	asign_carrera_files = [d / 'AsignaturasCarrera.csv' for d in dirs]
	horarios_files = [d / 'Horarios.csv' for d in dirs]
	prereq_files = [d / 'Prerrequisitos.csv' for d in dirs]

	# Outputs
	out_asign = ROOT / 'unified_Asignaturas.csv'
	out_asig_carr = ROOT / 'unified_AsignaturasCarrera.csv'
	out_hor = ROOT / 'unified_Horarios.csv'
	out_pr = ROOT / 'unified_Prerrequisitos.csv'

	# 1: Asignaturas - dedupe por Codigo de asignatura
	print('\nUnifying Asignaturas by Codigo de asignatura...')
	unify_by_key(asignaturas_files, 'Codigo de asignatura', out_asign)

	# 2: AsignaturasCarrera - dedupe por fila completa
	print('\nUnifying AsignaturasCarrera by full row...')
	unify_by_row(asign_carrera_files, out_asig_carr)

	# 3: Horarios - dedupe por fila completa
	print('\nUnifying Horarios by full row...')
	unify_by_row(horarios_files, out_hor)

	# 4: Prerrequisitos - dedupe por fila completa
	print('\nUnifying Prerrequisitos by full row...')
	unify_by_row(prereq_files, out_pr)

	print('\nDone.')


if __name__ == '__main__':
	try:
		main()
	except Exception as e:
		print('Error:', e)
		sys.exit(1)
