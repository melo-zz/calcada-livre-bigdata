"""Confere o repositório antes de enviar: nomes/RMs no README, segredos, dados pessoais, arquivos proibidos.
Uso (na raiz do repositório):  python scripts/checar_entrega.py"""
import re
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
RMS = {"99840": "Carlos Ganzerli", "551137": "Paulo Lopes", "98640": "Lucas Bandeira", "98809": "Gustavo Melo Namura"}
PADROES = {
    "chave AWS": r"AKIA[0-9A-Z]{16}", "token GitHub": r"gh[pousr]_[A-Za-z0-9]{30,}",
    "chave privada": r"-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----", "senha/segredo atribuído": r"(?i)(password|senha|secret|token|api[_-]?key)\s*[:=]\s*['\"][^'\"\s]{6,}['\"]",
    "CPF": r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b",
}
IGNORAR = {".git", "__pycache__", "node_modules"}
erros, avisos = [], []

readme = (RAIZ / "README.md").read_text(encoding="utf-8") if (RAIZ / "README.md").exists() else ""
for rm, nome in RMS.items():
    if rm not in readme:
        erros.append(f"README.md sem o RM {rm} ({nome})")
if "SEU_USUARIO" in readme or "COLE_AQUI" in readme:
    avisos.append("README.md ainda tem placeholders (SEU_USUARIO / COLE_AQUI)")

for p in RAIZ.rglob("*"):
    if not p.is_file() or any(x in IGNORAR for x in p.parts):
        continue
    rel = p.relative_to(RAIZ)
    if p.name == ".env" or p.suffix in {".tfstate", ".tfvars", ".pem", ".safetensors", ".gguf"}:
        erros.append(f"arquivo que não deve ir ao GitHub: {rel}")
    if p.suffix in {".py", ".md", ".sql", ".json", ".jsonl", ".yml", ".yaml", ".tf", ".txt", ".ipynb", ".csv"} and p.stat().st_size < 5_000_000 and rel.as_posix() != "scripts/checar_entrega.py":
        txt = p.read_text(encoding="utf-8", errors="ignore")
        for nome, rx in PADROES.items():
            if re.search(rx, txt):
                erros.append(f"{nome} encontrado em {rel}")
if (RAIZ / "amostras").exists() and not any(x.name != ".gitkeep" for x in (RAIZ / "amostras").iterdir()):
    avisos.append("bigdata/amostras/ está vazia (coloque 2-3 linhas de cada fonte)")
for e in erros:
    print("ERRO :", e)
for a in avisos:
    print("AVISO:", a)
print("\nTudo certo." if not erros and not avisos else f"\n{len(erros)} erro(s), {len(avisos)} aviso(s).")
sys.exit(1 if erros else 0)
