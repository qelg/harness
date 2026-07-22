from __future__ import annotations

from collections.abc import Sequence

from llm_harness.core.types import ToolSpec


class DefaultToolSet:
    name = "default"

    def tools(self, *, registry) -> Sequence[ToolSpec]:
        specs: list[ToolSpec] = []
        for tool in registry.tools.values():
            specs.append(
                ToolSpec(
                    name=tool.name,
                    description=tool.description,
                    input_schema=tool.input_schema,
                )
            )
        return specs
