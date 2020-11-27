import dataclasses as dc
import io
from typing import Optional, Any, Sequence


class RequirementsParsingError(Exception):
    """

    """

    def __init__(self, src: Any, line: int, err: Exception) -> None:
        self.src = src
        self.line = line
        self.err = err
        super().__init__(f"Error parsing requirements file, line {line}: {err}")


@dc.dataclass(frozen=True)
class PluginSpec:
    """

    """

    name: str
    original: str
    version: Optional[str] = None


def parse_requirement(name: str) -> PluginSpec:
    """
    Parse a requirement name, possibly also including a version. Versions should be
    specified with == e.g. pulumi_aws
    """
    if "==" in name:
        plugin_name, version = name.split("==")
        return PluginSpec(plugin_name, name, version)
    return PluginSpec(name, name)


def parse_requirements(file_or_string: Any) -> Sequence[PluginSpec]:
    """
    Parse a requirements file or sequence of requirements
    """
    input_file = file_or_string

    if isinstance(file_or_string, str):
        input_file = io.StringIO(file_or_string)

    reqs = []

    for idx, line in enumerate(file_or_string):
        if not line or line.lstrip().startswith("#"):
            continue
        try:
            parsed = parse_requirement(line.strip())
        except Exception as err:
            raise RequirementsParsingError(file_or_string, idx + 1, err) from err

        reqs.append(parsed)

    return reqs
