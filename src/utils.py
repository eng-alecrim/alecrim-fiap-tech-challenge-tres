# =============================================================================
# BIBLIOTECAS E MÓDULOS
# =============================================================================

import os
from pathlib import Path
from typing import Callable, Union
from config.config import settings

from dotenv import find_dotenv, load_dotenv

load_dotenv(dotenv_path=find_dotenv())

# =============================================================================
# CONSTANTES
# =============================================================================

NOME_PROJETO = (
    "alecrim-fiap-tech-challenge-tres"
    if settings["NOME_PROJETO"] is None
    else settings["NOME_PROJETO"]
)

# =============================================================================
# FUNÇÕES
# =============================================================================

# -----------------------------------------------------------------------------
# Retorna o diretório do projeto
# -----------------------------------------------------------------------------


def get_path_projeto(
    path_projeto: Path = Path.cwd(), nome_projeto: str = NOME_PROJETO
) -> Union[Callable, Path]:
    if path_projeto.name == nome_projeto:
        return path_projeto
    return get_path_projeto(path_projeto.parent, nome_projeto)
