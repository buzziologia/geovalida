from matplotlib.font_manager import font_family_aliases
import reflex as rx
from ..styles import TEXT_COLOR, TEXT_FONT, PAGE_COLOR


class InfoCardState(rx.State):
    pass


def info_card() -> rx.Component:
    return rx.box(
        # --- HEADER 1: Info UTP ---
        rx.box(
            rx.text(
                "UTP NUMERO",
                color= TEXT_COLOR["amarelo_bandeira"],
                font_family=TEXT_FONT,
                font_size="16px",
                font_weight="bold",
                text_align="left",
                padding="16px",
            ),
            width="100%",
            bg=PAGE_COLOR["azul_brasil"],
            border_bottom="1px solid #EAEAEA",
        ),

        # --- HEADER 2: Info Sede ---
        rx.box(
            rx.vstack(
                rx.text(
                    "Sede",
                    color=TEXT_COLOR["azul_brasil"],
                    font_family=TEXT_FONT,
                    font_size="12px",
                    font_weight="light",
                    text_align="left",
                    padding="16px",
                ),
                rx.spacer(),
                rx.hstack(
                    rx.text(
                        "Florianópolis - SC ",
                        color=TEXT_COLOR["azul_brasil"],
                        font_family=TEXT_FONT,
                        font_size="16px",
                        font_weight="bold",
                        text_align="left",
                        padding="16px",
                    ),
                    rx.spacer(),
                    rx.text(
                        "CODIBGE",
                        color=TEXT_COLOR["azul_brasil"],
                        font_family=TEXT_FONT,
                        font_size="16px",
                        font_weight="bold",
                        text_align="left",
                        padding="16px",
                    ),
                ),
            ),
        ),
    )


