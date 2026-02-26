from dependency_injector.wiring import Provide, inject
from spacy.language import Language
from spacy.tokens import Token

from ariesql.container import Container


def _resolve_token_type(token: Token) -> str:
    if token.like_num:
        return "[NUMBER]"
    if token.ent_type_:
        return f"[{token.ent_type_}]"
    return "[O]"


@inject
def mask_ner_and_numbers(
    text: str,
    nlp: Language = Provide[Container.nlp],
) -> str:
    doc = nlp(text)
    masked_tokens: list[str] = []
    for token in doc:
        if (
            token.ent_type_
            in {
                "PERSON",
                "ORG",
                "GPE",
                "LOC",
                "DATE",
                "TIME",
                "MONEY",
                "QUANTITY",
                "PERCENT",
            }
            or token.like_num
        ):
            # Avoid duplicate [REDACTED] for consecutive tokens of the same entity
            if masked_tokens and masked_tokens[-1] == f"[{token.ent_type_}]":
                continue
            masked_tokens.append(_resolve_token_type(token))
        else:
            masked_tokens.append(token.text)
    return " ".join(masked_tokens)
