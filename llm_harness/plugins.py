from __future__ import annotations

from importlib.metadata import entry_points

from llm_harness.protocols import ApiPlugin, EventConsumerPlugin, LLMProvider, Tool


class Registry:
    def __init__(self) -> None:
        self.providers: dict[str, LLMProvider] = {}
        self.tools: dict[str, Tool] = {}
        self.api_plugins: list[ApiPlugin] = []
        self.event_consumer_plugins: list[EventConsumerPlugin] = []

    def add_provider(self, provider: LLMProvider) -> None:
        self.providers[provider.name] = provider

    def add_tool(self, tool: Tool) -> None:
        self.tools[tool.name] = tool

    def add_api_plugin(self, plugin: ApiPlugin) -> None:
        self.api_plugins.append(plugin)

    def add_event_consumer_plugin(self, plugin: EventConsumerPlugin) -> None:
        self.event_consumer_plugins.append(plugin)

    def install_api_plugins(self, *, app, bus) -> None:
        for plugin in self.api_plugins:
            plugin.install_api(app=app, bus=bus, registry=self)

    def install_event_consumer_plugins(self, *, app, bus) -> None:
        for plugin in self.event_consumer_plugins:
            plugin.install_event_consumers(app=app, bus=bus, registry=self)


def load_plugins(registry: Registry) -> None:
    from llm_harness.builtin import register

    register(registry)
    for entry_point in entry_points(group="llm_harness.plugins"):
        if entry_point.name == "builtin":
            continue
        plugin_register = entry_point.load()
        plugin_register(registry)
