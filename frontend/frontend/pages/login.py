import reflex as rx

def login() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.heading("Unidade Territorial de Planejamento", size="6", color="white"),
            rx.text("Bem Vindo", size="8", color="white", weight="bold"),
            
            rx.input(placeholder="Usuário", width="100%"),
            rx.input(placeholder="Senha", type="password", width="100%"),
            
            rx.hstack(
                rx.button("Entrar", width="50%", bg="white", color="black"),
                rx.button("Acesso Público", width="50%", variant="outline", color="white"),
                width="100%",
            ),
            
            spacing="5",
            bg="#071D41", # Azul escuro do fundo do card
            padding="4em",
            border_radius="1em",
            box_shadow="lg",
            max_width="500px",
        ),
        height="100vh",
        width="100%",
        # Aqui você pode por uma imagem de fundo depois
        bg="#0C326F", # Azul Bandeira como fundo da página
    )