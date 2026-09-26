"""Copia dados/painel.json para dentro do index.html (bloco <script id="painel-embedded">).

A cópia embutida deixa o site abrir com duplo clique, sem servidor.
Rode sempre que o painel.json mudar:  python dados/atualizar_json_embutido.py
"""
import re
from pathlib import Path

raiz = Path(__file__).resolve().parent.parent
painel = (raiz / "dados" / "painel.json").read_text(encoding="utf-8").strip()
caminho_site = raiz / "index.html"
html = caminho_site.read_text(encoding="utf-8")

padrao = re.compile(r'(<script id="painel-embedded" type="application/json">\s*)(.*?)(\s*</script>)', re.DOTALL)
if not padrao.search(html):
    raise SystemExit('Bloco <script id="painel-embedded"> não encontrado no index.html')

novo = padrao.sub(lambda m: m.group(1) + painel + m.group(3), html, count=1)
caminho_site.write_text(novo, encoding="utf-8")
print("index.html atualizado com o painel.json")
