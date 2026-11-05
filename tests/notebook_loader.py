from pathlib import Path
from types import SimpleNamespace

WORKSPACE = Path(__file__).resolve().parents[1] / "workspace"
CODE_CELL_HEADERS = {"# CELL ********************", "# PARAMETERS CELL ********************"}


def code_cells(notebook_name: str) -> list:
    text = (WORKSPACE / f"{notebook_name}.Notebook" / "notebook-content.py").read_text(encoding="utf-8")
    cells, current = [], None
    for line in text.splitlines():
        if line.startswith("# ") and line.endswith("********************"):
            current = [] if line in CODE_CELL_HEADERS else None
            if current is not None:
                cells.append(current)
        elif current is not None and not line.startswith("%"):
            current.append(line)
    return ["\n".join(cell) for cell in cells]


def fake_notebookutils() -> SimpleNamespace:
    return SimpleNamespace(
        fs=SimpleNamespace(mkdirs=lambda path: True, exists=lambda path: False, rm=lambda path: True, cp=lambda src, dst: True),
        notebook=SimpleNamespace(run=None, exit=lambda value: None),
        credentials=SimpleNamespace(getSecret=lambda vault, name: "secret"),
    )


def load(*notebook_cells: tuple) -> dict:
    namespace = {"notebookutils": fake_notebookutils(), "__name__": "notebook"}
    for notebook_name, cell_slice in notebook_cells:
        for source in code_cells(notebook_name)[cell_slice]:
            exec(compile(source, notebook_name, "exec"), namespace)
    return namespace
