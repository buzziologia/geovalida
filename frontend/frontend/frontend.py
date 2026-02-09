import reflex as rx
from .pages.login import login

# importando os components para teste
from .components.header import header
from .components.sidebar import sidebar
from .components.topbar import topbar
from .components.login_card import login_card
from .components.info_card import info_card

app = rx.App(
    stylesheets=[
        "https://fonts.googleapis.com/css2?family=Noto+Sans+Devanagari:wght@400;700&display=swap",
    ],
    head_components=[
        rx.el.link(rel="icon", href="/logo.png"),
    ],
)


def rascunho():
    return rx.center(
        topbar(), # O componente que você quer testar
        width="100%",
        height="100vh",
        bg="gray" # Um fundo diferente pra destacar
    )
app.add_page(rascunho, route="/teste")
app.add_page(login, route="/login")