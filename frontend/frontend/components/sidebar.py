import reflex as rx
from ..styles import TEXT_COLOR, PAGE_COLOR, TEXT_FONT

def sidebar_item(text: str, icon: str, url: str) -> rx.Component:
    return rx.link(
        rx.hstack(
            rx.icon(icon, size=18),
            rx.text(text, font_family=TEXT_FONT, size="3", weight="medium"),
            spacing="3",
            align="center",
            width="100%",
            padding="12px",
            border_radius="8px",
            color=TEXT_COLOR["azul_brasil"],
            _hover={
                "bg": "rgba(7, 29, 65, 0.05)", # Efeito hover sutil
            }
        ),
        href=url,
        width="100%",
        underline="none",
    )

def sidebar() -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.heading("Menu", font_family=TEXT_FONT, size="5", margin_bottom="2em", color=TEXT_COLOR["azul_brasil"], padding_left="12px"),
            
            sidebar_item("Dashboard", "layout-dashboard", "/"),
            sidebar_item("Analytics", "bar-chart-2", "/analytics"),
            sidebar_item("Configurações", "settings", "/settings"),
            
            spacing="2", # Espaçamento entre os itens
            width="100%",
        ),
        padding="2em",
        height="100vh",
        bg=PAGE_COLOR["cinza_sidebar"],
        width="280px", # Um pouco mais largo para acomodar ícones
        border_right="1px solid #EAEAEA", # Divisória sutil
    )
