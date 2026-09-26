import json
import re
from pathlib import Path


def test_painel_json_sincronizado_com_site():
    raiz = Path(__file__).parent.parent
    caminho_painel = raiz / "dados" / "painel.json"
    caminho_site = raiz / "index.html"

    assert caminho_painel.exists(), "dados/painel.json não encontrado"
    assert caminho_site.exists(), "index.html não encontrado"

    with open(caminho_painel, "r", encoding="utf-8") as f:
        dados_json = json.load(f)

    with open(caminho_site, "r", encoding="utf-8") as f:
        conteudo_html = f.read()

    match_script = re.search(
        r'<script\s+id=["\']painel-embedded["\'](?:\s+type=["\']application/json["\'])?[^>]*>(.*?)</script>',
        conteudo_html,
        re.DOTALL,
    )
    assert match_script is not None, "Script <script id='painel-embedded'> não encontrado em index.html"

    dados_embutidos = json.loads(match_script.group(1).strip())

    msg_erro = (
        "o JSON embutido no index.html está diferente de dados/painel.json — "
        "rode: python dados/atualizar_json_embutido.py"
    )
    assert dados_json == dados_embutidos, msg_erro
