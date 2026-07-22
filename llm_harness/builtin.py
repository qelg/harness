from __future__ import annotations

from llm_harness.api_plugin import HarnessApiPlugin
from llm_harness.config import Settings
from llm_harness.auth_plugins.chatgpt_oauth import ChatGPTOAuthPlugin
from llm_harness.auth_plugins.openai_codex_device import OpenAICodexDeviceAuthPlugin
from llm_harness.builtin_plugins.llm_provider_runner import LlmProviderRunnerPlugin
from llm_harness.builtin_plugins.llm_run_requester import LlmRunRequesterPlugin
from llm_harness.providers.chatgpt_codex import ChatGPTCodexProvider
from llm_harness.providers.mock import MockLLMProvider
from llm_harness.providers.openai_compatible import OpenAICompatibleProvider
from llm_harness.toolsets import DefaultToolSet
from llm_harness.tools.podman_shell import PodmanShellTool


def register(registry, *, bus=None) -> None:
    settings = Settings.from_env()
    registry.add_api_plugin(HarnessApiPlugin(settings=settings))
    registry.add_provider(
        OpenAICompatibleProvider(
            name="openai-codex",
            base_url=settings.openai_base_url,
            api_key=settings.openai_api_key,
            log_provider_events=settings.log_provider_events,
        )
    )
    if bus is not None:
        registry.add_provider(ChatGPTCodexProvider(conn=bus.conn, settings=settings))
    registry.add_provider(
        OpenAICompatibleProvider(
            name="openrouter",
            base_url=settings.openrouter_base_url,
            api_key=settings.openrouter_api_key,
            extra_headers={"HTTP-Referer": "http://localhost", "X-Title": "LLM Harness"},
            log_provider_events=settings.log_provider_events,
        )
    )
    registry.add_provider(MockLLMProvider(response=settings.mock_llm_response))
    registry.add_tool(PodmanShellTool(settings=settings))
    registry.add_toolset(DefaultToolSet())
    registry.add_api_plugin(ChatGPTOAuthPlugin(settings=settings))
    registry.add_api_plugin(OpenAICodexDeviceAuthPlugin(settings=settings))
    registry.add_event_consumer_plugin(LlmRunRequesterPlugin(settings=settings))
    registry.add_event_consumer_plugin(LlmProviderRunnerPlugin())
