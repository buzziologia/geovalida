import reflex as rx
from .pages.login import login
from .components.header import header
from .components.sidebar import sidebar

app = rx.App(
    stylesheets=[
        "https://fonts.googleapis.com/css2?family=Noto+Sans+Devanagari:wght@400;700&display=swap",
    ],
)


def rascunho():
    return rx.center(
        sidebar(), # O componente que você quer testar
        width="100%",
        height="100vh",
        bg="gray" # Um fundo diferente pra destacar
    )
app.add_page(rascunho, route="/teste")