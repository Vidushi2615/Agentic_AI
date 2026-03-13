import os
from collections import deque
from typing import Any

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover - handled at runtime
    boto3 = None
    Config = None
    BotoCoreError = Exception
    ClientError = Exception


DEFAULT_REGION = 'eu-west-2'
MAX_TOOL_ROUNDS = 5
MAX_MEMORY_EVENTS = 8


class BedrockPlannerError(RuntimeError):
    pass


class BedrockPlanner:
    def __init__(
        self,
        *,
        system_prompt: str,
        tools: list[dict[str, Any]],
        region_name: str | None = None,
        model_id: str | None = None,
        env_file: str | None = None,
    ) -> None:
        self._load_env_file(env_file)
        self.system_prompt = system_prompt.strip()
        self.tools = tools
        self.region_name = region_name or os.getenv('AWS_DEFAULT_REGION') or DEFAULT_REGION
        self.model_id = model_id or os.getenv('BEDROCK_MODEL_ID', '').strip()
        self.memory: deque[dict[str, Any]] = deque(maxlen=MAX_MEMORY_EVENTS)
        self._client = None

    def validate_configuration(self) -> list[str]:
        issues = []
        if boto3 is None:
            issues.append('boto3 is not installed. Install it with: python3 -m pip install boto3')
        if not os.getenv('AWS_BEARER_TOKEN_BEDROCK', '').strip():
            issues.append('AWS_BEARER_TOKEN_BEDROCK is not set.')
        if not self.model_id:
            issues.append('BEDROCK_MODEL_ID is not set.')
        return issues

    def run_task(
        self,
        user_task: str,
        *,
        dynamic_context: str,
        tool_handler,
    ) -> tuple[str, list[dict[str, Any]]]:
        issues = self.validate_configuration()
        if issues:
            raise BedrockPlannerError(' '.join(issues))

        messages = list(self.memory)
        messages.append(self._user_text_message(f'Context:\n{dynamic_context}\n\nUser task:\n{user_task}'))

        last_text = ''
        executed_tools: list[dict[str, Any]] = []

        for _ in range(MAX_TOOL_ROUNDS):
            response = self._converse(messages)
            assistant_message = response['output']['message']
            messages.append(assistant_message)

            text_blocks = []
            tool_uses = []
            for block in assistant_message.get('content', []):
                if 'text' in block:
                    text_blocks.append(block['text'])
                if 'toolUse' in block:
                    tool_uses.append(block['toolUse'])

            if text_blocks:
                last_text = '\n'.join(text_blocks).strip()

            if not tool_uses:
                self._remember_clean_turn(user_task, last_text)
                return last_text, executed_tools

            tool_results = []
            for tool_use in tool_uses:
                tool_result = tool_handler(tool_use['name'], tool_use.get('input', {}))
                executed_tools.append(
                    {
                        'name': tool_use['name'],
                        'input': tool_use.get('input', {}),
                        'result': tool_result,
                    }
                )
                tool_results.append(
                    {
                        'toolResult': {
                            'toolUseId': tool_use['toolUseId'],
                            'content': [{'json': tool_result}],
                        }
                    }
                )

            messages.append({'role': 'user', 'content': tool_results})

        raise BedrockPlannerError('Tool loop exceeded the maximum number of rounds.')

    def _converse(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        try:
            return self._get_client().converse(
                modelId=self.model_id,
                system=[{'text': self.system_prompt}],
                messages=messages,
                inferenceConfig={
                    'maxTokens': 1024,
                    'temperature': 0.2,
                },
                toolConfig={
                    'tools': self.tools,
                    'toolChoice': {'auto': {}},
                },
            )
        except (ClientError, BotoCoreError) as exc:
            raise BedrockPlannerError(f'Bedrock request failed: {exc}') from exc

    def _get_client(self):
        if self._client is None:
            if boto3 is None:
                raise BedrockPlannerError(
                    'boto3 is not installed. Install it with: python3 -m pip install boto3'
                )
            config = Config(retries={'max_attempts': 3, 'mode': 'standard'}, read_timeout=120)
            self._client = boto3.client(
                service_name='bedrock-runtime',
                region_name=self.region_name,
                config=config,
            )
        return self._client

    def _remember_clean_turn(self, user_task: str, assistant_text: str) -> None:
        # Persist only plain-text conversation history. Tool protocol messages
        # must not be replayed in later turns without their matching tool use.
        self.memory.append(self._user_text_message(user_task))
        if assistant_text:
            self.memory.append({'role': 'assistant', 'content': [{'text': assistant_text}]})

    @staticmethod
    def _user_text_message(text: str) -> dict[str, Any]:
        return {'role': 'user', 'content': [{'text': text}]}

    @staticmethod
    def _load_env_file(explicit_env_file: str | None) -> None:
        candidate_paths = []
        if explicit_env_file:
            candidate_paths.append(explicit_env_file)
        env_override = os.getenv('TB3_AGENT_ENV_FILE', '').strip()
        if env_override:
            candidate_paths.append(env_override)
        candidate_paths.append(os.path.join(os.getcwd(), '.env'))

        seen = set()
        for path in candidate_paths:
            normalized = os.path.abspath(os.path.expanduser(path))
            if normalized in seen or not os.path.isfile(normalized):
                continue
            seen.add(normalized)
            with open(normalized, 'r', encoding='utf-8') as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key and key not in os.environ:
                        os.environ[key] = value
            break
