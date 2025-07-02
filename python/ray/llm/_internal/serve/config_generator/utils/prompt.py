from rich.prompt import IntPrompt, Prompt


class BoldPrompt(Prompt):
    @classmethod
    def ask(cls, prompt: str, **kwargs):
        # Automatically apply bold style to the BoldPrompt
        return Prompt.ask(f"[bold]{prompt}[/bold]", **kwargs)


class BoldIntPrompt(IntPrompt):
    @classmethod
    def ask(cls, prompt: str, **kwargs):
        # Automatically apply bold style to the BoldPrompt
        return IntPrompt.ask(f"[bold]{prompt}[/bold]", **kwargs)
