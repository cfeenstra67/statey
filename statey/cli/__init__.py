try:
    import click
except ImportError as err:
    raise RuntimeError(
        "CLI dependencies are not installed, the CLI will not be usable."
        " The CLI will not be usable. CLI dependencies can be included with the"
        " statey[cli] extra."
    ) from err
