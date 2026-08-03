from __future__ import annotations

from llm_harness.api_plugin import HarnessApiPlugin
from llm_harness.config import Settings
from llm_harness.auth_plugins.chatgpt_oauth import ChatGPTOAuthPlugin
from llm_harness.auth_plugins.openai_codex_device import OpenAICodexDeviceAuthPlugin
from llm_harness.builtin_plugins.chatgpt_usage import ChatGPTUsageApiPlugin, ChatGPTUsagePlugin
from llm_harness.builtin_plugins.container_cleanup import ContainerCleanupPlugin, PodmanContainerManager
from llm_harness.builtin_plugins.llm_provider_runner import LlmProviderRunnerPlugin
from llm_harness.builtin_plugins.llm_run_requester import LlmRunRequesterPlugin
from llm_harness.builtin_plugins.namer import NamerPlugin
from llm_harness.builtin_plugins.server_overloaded_retry import ServerOverloadedRetryPlugin
from llm_harness.builtin_plugins.session_state import SessionStatePlugin
from llm_harness.builtin_plugins.system_prompt import SystemPromptPlugin
from llm_harness.builtin_plugins.tool_call_requester import ToolCallRequesterPlugin
from llm_harness.builtin_plugins.tool_result_llm_requester import ToolResultLlmRequesterPlugin
from llm_harness.builtin_plugins.unifiedpush import UnifiedPushPlugin
from llm_harness.providers.chatgpt_codex import ChatGPTCodexProvider
from llm_harness.providers.mock import MockLLMProvider
from llm_harness.providers.openai_compatible import OpenAICompatibleProvider
from llm_harness.toolsets import DefaultToolSet
from llm_harness.tools.podman_shell import PodmanShellTool, PodmanShellToolConsumer
from llm_harness.tools.retrieve_secret import RetrieveSecretApiPlugin, RetrieveSecretTool, RetrieveSecretToolConsumer
from llm_harness.tools.skill_view import SkillViewTool, SkillViewToolConsumer
from llm_harness.tools.subagent import SubagentPlugin, SubagentStateTool, SubagentTool
from llm_harness.tools.tasks import TasksTool, TasksToolConsumer


def register(registry, *, bus=None) -> None:
    settings = Settings.from_env()
    registry.add_api_plugin(HarnessApiPlugin(settings=settings))
    registry.add_api_plugin(ChatGPTUsageApiPlugin())
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
            prompt_cache_key=settings.prompt_cache_key,
            log_provider_events=settings.log_provider_events,
        )
    )
    registry.add_provider(MockLLMProvider(response=settings.mock_llm_response))
    terminal = PodmanShellTool(settings=settings)
    registry.add_tool(terminal)
    registry.add_event_consumer_plugin(PodmanShellToolConsumer(tool=terminal))
    retrieve_secret = RetrieveSecretTool(settings=settings)
    registry.add_tool(retrieve_secret)
    registry.add_api_plugin(RetrieveSecretApiPlugin(tool=retrieve_secret))
    registry.add_event_consumer_plugin(RetrieveSecretToolConsumer(tool=retrieve_secret))
    containers = ContainerCleanupPlugin(manager=PodmanContainerManager())
    registry.add_api_plugin(containers)
    registry.add_event_consumer_plugin(containers)
    skill_view = SkillViewTool(settings=settings)
    registry.add_tool(skill_view)
    registry.add_event_consumer_plugin(SkillViewToolConsumer(tool=skill_view))
    tasks = TasksTool()
    registry.add_tool(tasks)
    registry.add_event_consumer_plugin(TasksToolConsumer(tool=tasks))
    subagent = SubagentTool()
    subagent_state = SubagentStateTool()
    registry.add_tool(subagent)
    registry.add_tool(subagent_state)
    registry.add_event_consumer_plugin(
        SubagentPlugin(tool=subagent, state_tool=subagent_state, settings=settings)
    )
    registry.add_toolset(DefaultToolSet())
    registry.add_api_plugin(ChatGPTOAuthPlugin(settings=settings))
    registry.add_api_plugin(OpenAICodexDeviceAuthPlugin(settings=settings))
    registry.add_event_consumer_plugin(SessionStatePlugin())
    if bus is not None:
        registry.add_event_consumer_plugin(ChatGPTUsagePlugin(conn=bus.conn, settings=settings))
    registry.add_event_consumer_plugin(SystemPromptPlugin(settings=settings))
    unifiedpush = UnifiedPushPlugin()
    registry.add_api_plugin(unifiedpush)
    registry.add_event_consumer_plugin(unifiedpush)
    registry.add_event_consumer_plugin(NamerPlugin(settings=settings))
    registry.add_event_consumer_plugin(LlmRunRequesterPlugin(settings=settings))
    registry.add_event_consumer_plugin(LlmProviderRunnerPlugin())
    registry.add_event_consumer_plugin(ServerOverloadedRetryPlugin())
    registry.add_event_consumer_plugin(ToolCallRequesterPlugin())
    registry.add_event_consumer_plugin(ToolResultLlmRequesterPlugin(settings=settings))
