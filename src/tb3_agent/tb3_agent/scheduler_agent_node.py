import json
import os
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from ament_index_python.packages import get_package_share_directory
from zoneinfo import ZoneInfo

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # pragma: no cover - handled at runtime
    boto3 = None
    Config = None
    BotoCoreError = Exception
    ClientError = Exception
import rclpy
from rclpy.node import Node
from std_msgs.msg import String
import yaml

from tb3_agent.bedrock_planner import BedrockPlanner
from tb3_agent.open_meteo_client import OpenMeteoClient, OpenMeteoError


AUTO_RESUME_PAUSE_REASONS = {
    'before_sunrise',
    'after_sunset',
    'current_rain_or_snow',
    'forecast_rain_or_snow',
    'weather_api_unavailable',
}
MANUAL_PAUSE_REASON = 'manual_stop'
STOP_CONTROL_COMMANDS = {
    'stop',
    'stop all tasks',
    'stop all tasks and return to spawn',
    'stop current task and return to spawn',
    'return to spawn',
    'go to spawn',
    'cancel all tasks',
}
URGENT_PRIORITY = 'urgent'
NORMAL_PRIORITY = 'normal'


class SchedulerAgentNode(Node):
    def __init__(self) -> None:
        super().__init__('scheduler_agent_node')

        package_share = get_package_share_directory('tb3_agent')
        workspace_root = Path(package_share).resolve().parents[3]
        config_dir = Path(package_share) / 'config'
        env_file = workspace_root / '.env'

        BedrockPlanner._load_env_file(str(env_file))

        self.scheduler_config = self._load_yaml(config_dir / 'scheduler.yaml')
        self.state_path = self._resolve_path(self.scheduler_config['state_file'], workspace_root)
        self.scheduler_prompt = (config_dir / 'scheduler_system_prompt.txt').read_text(encoding='utf-8')
        self.weather_client = OpenMeteoClient()
        self.region_name = os.getenv('AWS_DEFAULT_REGION', self.scheduler_config.get('region', 'eu-west-2'))
        self.model_id = os.getenv('BEDROCK_MODEL_ID', '').strip()
        self.bedrock_client = None
        self.robot_configs = {robot['name']: robot for robot in self.scheduler_config['robots']}
        self.robot_states = {
            robot_name: {
                'agent_status': 'unknown',
                'active_mission_id': None,
                'active_subtask_id': None,
                'control_in_progress': False,
            }
            for robot_name in self.robot_configs
        }
        self.mission_state: dict[str, Any] = self._load_state()
        self.weather_snapshot: dict[str, Any] | None = self.mission_state.get('weather') or None
        self.weather_block_reason = ''
        self.daylight_block_reason = ''

        self.status_publisher = self.create_publisher(String, '/scheduler/status', 10)
        self.feedback_publisher = self.create_publisher(String, '/scheduler/feedback', 10)
        self.plan_publisher = self.create_publisher(String, '/scheduler/mission_plan', 10)
        self.assignments_publisher = self.create_publisher(String, '/scheduler/active_assignments', 10)
        self.mission_request_subscription = self.create_subscription(
            String, '/scheduler/mission_requests', self._mission_request_callback, 10
        )

        self.robot_task_publishers = {}
        for robot_name in self.robot_configs:
            self.robot_task_publishers[robot_name] = self.create_publisher(
                String, f'/{robot_name}/scheduled_task_request', 10
            )
            self.create_subscription(
                String, f'/{robot_name}/scheduled_task_result', self._make_result_callback(robot_name), 10
            )
            self.create_subscription(
                String, f'/{robot_name}/agent_status', self._make_status_callback(robot_name), 10
            )

        self.create_timer(float(self.scheduler_config['weather_poll_seconds']), self._refresh_weather)
        self.create_timer(float(self.scheduler_config['schedule_poll_seconds']), self._scheduler_tick)
        self._publish_feedback('Scheduler agent started.')
        self._refresh_weather()
        if not self.scheduler_config.get('resume_unfinished_missions_on_startup', False):
            self._pause_restored_missions()
        self._publish_configuration_warning()

    def _load_yaml(self, path: Path) -> dict:
        with open(path, 'r', encoding='utf-8') as handle:
            return yaml.safe_load(handle) or {}

    def _resolve_path(self, path_value: str, workspace_root: Path) -> Path:
        candidate = Path(path_value)
        if candidate.is_absolute():
            return candidate
        return workspace_root / candidate

    def _load_state(self) -> dict[str, Any]:
        if self.state_path.exists():
            with open(self.state_path, 'r', encoding='utf-8') as handle:
                return json.load(handle)
        return {
            'missions': {},
            'mission_order': [],
            'scheduler_state': 'idle',
            'pause_reason': '',
            'weather': {},
        }

    def _pause_restored_missions(self) -> None:
        restored_any = False
        for mission_id in self.mission_state['mission_order']:
            mission = self.mission_state['missions'][mission_id]
            if mission.get('status') == 'completed':
                continue
            restored_any = True
            mission['status'] = 'paused'
            mission['pause_reason'] = 'awaiting_manual_restart'
            for subtask in mission.get('subtasks', []):
                if subtask.get('status') in {'dispatched', 'paused'}:
                    subtask['status'] = 'paused'
                elif subtask.get('status') == 'completed':
                    continue
                else:
                    subtask['status'] = 'pending'

        for robot_state in self.robot_states.values():
            robot_state['active_mission_id'] = None
            robot_state['active_subtask_id'] = None
            robot_state['control_in_progress'] = False

        if restored_any:
            self._save_state()
            self._publish_feedback(
                'Restored missions were paused at startup. Send new work explicitly or resume them manually later.'
            )

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_path, 'w', encoding='utf-8') as handle:
            json.dump(self.mission_state, handle, indent=2)

    def _mission_request_callback(self, msg: String) -> None:
        request_text = msg.data.strip()
        if not request_text:
            return
        if self._handle_control_text(request_text):
            return
        parsed = self._parse_mission_request(request_text)
        self._enqueue_mission(
            parsed['request_text'],
            None,
            parsed.get('preferred_robot'),
            priority=parsed.get('priority', NORMAL_PRIORITY),
        )

    def _enqueue_mission(
        self,
        request_text: str,
        mission_id: str | None,
        preferred_robot: str | None,
        *,
        priority: str = NORMAL_PRIORITY,
    ) -> None:
        mission_id = mission_id or f'mission_{datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")}'
        if mission_id in self.mission_state['missions']:
            return
        assigned_robot = self._select_robot_for_new_mission(preferred_robot)
        self.mission_state['missions'][mission_id] = {
            'mission_id': mission_id,
            'original_request': request_text,
            'assigned_robot': assigned_robot,
            'priority': priority,
            'status': 'pending_plan',
            'pause_reason': '',
            'subtasks': [],
        }
        self.mission_state['mission_order'].append(mission_id)
        self._save_state()
        self._publish_feedback(f'Queued {priority} mission {mission_id} for {assigned_robot}: {request_text}')
        if priority == URGENT_PRIORITY:
            self._preempt_robot_for_urgent_mission(assigned_robot, mission_id)

    def _handle_control_text(self, text: str) -> bool:
        normalized = ' '.join(text.lower().strip().split())
        if normalized in STOP_CONTROL_COMMANDS:
            self._stop_all_tasks_and_return_home()
            return True
        if all(token in normalized for token in ('stop', 'return', 'spawn')):
            self._stop_all_tasks_and_return_home()
            return True
        return False

    def _stop_all_tasks_and_return_home(self) -> None:
        affected = 0
        for mission_id in self.mission_state['mission_order']:
            mission = self.mission_state['missions'][mission_id]
            if mission.get('status') == 'completed':
                continue
            mission['status'] = 'paused'
            mission['pause_reason'] = MANUAL_PAUSE_REASON
            for subtask in mission.get('subtasks', []):
                if subtask.get('status') == 'dispatched':
                    subtask['status'] = 'paused'
                elif subtask.get('status') not in {'completed', 'failed', 'needs_clarification'}:
                    subtask['status'] = 'pending'
            affected += 1

        for robot_name, robot_state in self.robot_states.items():
            home_location = self.robot_configs[robot_name].get('home_location', 'spawn')
            mission_id = robot_state['active_mission_id'] or 'manual_control'
            subtask_id = robot_state['active_subtask_id'] or 'return_home'
            control_envelope = {
                'mission_id': mission_id,
                'subtask_id': f'{subtask_id}_return_home',
                'robot_id': robot_name,
                'instruction_text': f'Stop current work and return to {home_location} immediately.',
                'resume_context': 'Manual stop requested by the operator.',
                'priority': 'high',
                'dispatch_reason': 'stop_work',
            }
            self.robot_task_publishers[robot_name].publish(String(data=json.dumps(control_envelope)))
            robot_state['control_in_progress'] = True
            robot_state['active_mission_id'] = mission_id
            robot_state['active_subtask_id'] = f'{subtask_id}_return_home'

        self._save_state()
        self._publish_feedback(
            f'Manual stop requested. Paused {affected} mission(s) and instructed all managed robots to return home.'
        )

    def _parse_mission_request(self, request_text: str) -> dict[str, Any]:
        try:
            parsed = json.loads(request_text)
        except json.JSONDecodeError:
            return {'request_text': request_text, 'priority': NORMAL_PRIORITY}

        if not isinstance(parsed, dict):
            return {'request_text': request_text, 'priority': NORMAL_PRIORITY}

        mission_text = str(parsed.get('request_text', '')).strip()
        if not mission_text:
            mission_text = request_text
        priority = str(parsed.get('priority', NORMAL_PRIORITY)).strip().lower() or NORMAL_PRIORITY
        if priority not in {NORMAL_PRIORITY, URGENT_PRIORITY}:
            priority = NORMAL_PRIORITY
        preferred_robot = parsed.get('preferred_robot')
        if preferred_robot is not None:
            preferred_robot = str(preferred_robot).strip() or None
        return {
            'request_text': mission_text,
            'priority': priority,
            'preferred_robot': preferred_robot,
        }

    def _preempt_robot_for_urgent_mission(self, robot_name: str, urgent_mission_id: str) -> None:
        robot_state = self.robot_states[robot_name]
        mission_id = robot_state['active_mission_id']
        subtask_id = robot_state['active_subtask_id']
        if mission_id is None or subtask_id is None:
            return

        current_mission = self.mission_state['missions'].get(mission_id)
        if current_mission is None or mission_id == urgent_mission_id:
            return

        for subtask in current_mission.get('subtasks', []):
            if subtask.get('subtask_id') == subtask_id:
                subtask['status'] = 'paused'
                subtask['last_message'] = f'Preempted by urgent mission {urgent_mission_id}.'
                break
        current_mission['status'] = 'paused'
        current_mission['pause_reason'] = f'preempted_by_{urgent_mission_id}'

        control_envelope = {
            'mission_id': mission_id,
            'subtask_id': f'{subtask_id}_preempt',
            'robot_id': robot_name,
            'instruction_text': 'Stop current work immediately and wait for the next urgent assignment.',
            'resume_context': f'Preempted by urgent mission {urgent_mission_id}.',
            'priority': 'high',
            'dispatch_reason': 'stop_work',
        }
        self.robot_task_publishers[robot_name].publish(String(data=json.dumps(control_envelope)))
        robot_state['control_in_progress'] = True
        self._save_state()
        self._publish_feedback(
            f'Preempting {mission_id}/{subtask_id} on {robot_name} for urgent mission {urgent_mission_id}.'
        )

    def _select_robot_for_new_mission(self, preferred_robot: str | None) -> str:
        if preferred_robot and preferred_robot in self.robot_configs:
            return preferred_robot

        ranked_robots = []
        for robot_name in self.robot_configs:
            open_mission_count = 0
            for mission_id in self.mission_state['mission_order']:
                mission = self.mission_state['missions'][mission_id]
                if mission['assigned_robot'] != robot_name:
                    continue
                if mission['status'] != 'completed':
                    open_mission_count += 1
            robot_busy = self.robot_states[robot_name]['active_subtask_id'] is not None
            ranked_robots.append((robot_busy, open_mission_count, robot_name))

        ranked_robots.sort()
        return ranked_robots[0][2]

    def _make_status_callback(self, robot_name: str):
        def _callback(msg: String) -> None:
            self.robot_states[robot_name]['agent_status'] = msg.data.strip() or 'unknown'
        return _callback

    def _make_result_callback(self, robot_name: str):
        def _callback(msg: String) -> None:
            try:
                result = json.loads(msg.data)
            except json.JSONDecodeError:
                self._publish_feedback(f'Invalid scheduled task result from {robot_name}: {msg.data}')
                return
            self._handle_task_result(robot_name, result)
        return _callback

    def _handle_task_result(self, robot_name: str, result: dict) -> None:
        mission_id = result.get('mission_id')
        subtask_id = result.get('subtask_id')
        mission = self.mission_state['missions'].get(mission_id)
        if mission is None:
            return
        matched_subtask = False
        for subtask in mission['subtasks']:
            if subtask['subtask_id'] != subtask_id:
                continue
            matched_subtask = True
            subtask['last_message'] = result.get('message', '')
            status = result.get('status', 'failed')
            if status == 'completed':
                subtask['status'] = 'completed'
            elif status == 'cancelled':
                subtask['status'] = 'paused'
            elif status == 'needs_clarification':
                subtask['status'] = 'needs_clarification'
                mission['status'] = 'paused'
                mission['pause_reason'] = 'needs_clarification'
            else:
                subtask['status'] = 'failed'
                mission['status'] = 'paused'
                mission['pause_reason'] = status

        if not matched_subtask:
            self._publish_feedback(
                f"{robot_name} completed control task {mission_id}/{subtask_id}: {result.get('message', '')}"
            )

        self.robot_states[robot_name]['active_mission_id'] = None
        self.robot_states[robot_name]['active_subtask_id'] = None
        self.robot_states[robot_name]['control_in_progress'] = False

        if mission['subtasks'] and all(subtask['status'] == 'completed' for subtask in mission['subtasks']):
            mission['status'] = 'completed'
            mission['pause_reason'] = ''
        elif matched_subtask and mission['status'] not in {'paused', 'completed'}:
            mission['status'] = 'planned'
        self._save_state()
        self._publish_feedback(
            f"{robot_name} reported {result.get('status')} for {mission_id}/{subtask_id}: {result.get('message', '')}"
        )

    def _scheduler_tick(self) -> None:
        self._publish_status()
        allowed, reason = self._work_allowed()
        if not allowed:
            self._pause_active_work(reason)
            return

        for mission_id in self.mission_state['mission_order']:
            mission = self.mission_state['missions'][mission_id]
            if mission['status'] == 'pending_plan':
                self._plan_mission(mission)
            if mission['status'] in {'planned', 'executing', 'paused'}:
                self._dispatch_next_subtask(mission)

        self._resume_preempted_missions()

        self._publish_assignments()
        self._save_state()

    def _plan_mission(self, mission: dict) -> None:
        try:
            plan = self._request_mission_plan(mission['original_request'], mission['assigned_robot'])
        except Exception as exc:
            mission['status'] = 'paused'
            mission['pause_reason'] = f'planning_failed: {exc}'
            self._publish_feedback(f"Failed to plan mission {mission['mission_id']}: {exc}")
            return

        subtasks = []
        for index, subtask in enumerate(plan.get('subtasks', []), start=1):
            subtasks.append(
                {
                    'subtask_id': subtask.get('subtask_id') or f"{mission['mission_id']}_subtask_{index:03d}",
                    'instruction_text': subtask['instruction_text'],
                    'resume_context': subtask.get('resume_context', ''),
                    'status': 'pending',
                    'last_message': '',
                }
            )

        if not subtasks:
            mission['status'] = 'paused'
            mission['pause_reason'] = 'planner_returned_no_subtasks'
            self._publish_feedback(f"Planner returned no subtasks for mission {mission['mission_id']}.")
            return

        mission['subtasks'] = subtasks
        mission['status'] = 'planned'
        mission['pause_reason'] = ''
        self.plan_publisher.publish(String(data=json.dumps({'mission_id': mission['mission_id'], 'plan': subtasks})))
        self._publish_feedback(f"Planned mission {mission['mission_id']} into {len(subtasks)} subtasks.")

    def _dispatch_next_subtask(self, mission: dict) -> None:
        robot_name = mission['assigned_robot']
        robot_state = self.robot_states[robot_name]
        if robot_state['active_subtask_id'] is not None:
            return
        if mission['status'] == 'paused' and mission.get('pause_reason') not in AUTO_RESUME_PAUSE_REASONS:
            return

        for subtask in mission['subtasks']:
            if subtask['status'] in {'pending', 'paused'}:
                dispatch_reason = 'resume' if subtask['status'] == 'paused' else 'new_work'
                envelope = {
                    'mission_id': mission['mission_id'],
                    'subtask_id': subtask['subtask_id'],
                    'robot_id': robot_name,
                    'instruction_text': subtask['instruction_text'],
                    'resume_context': subtask.get('resume_context', ''),
                    'priority': 'normal',
                    'dispatch_reason': dispatch_reason,
                }
                self.robot_task_publishers[robot_name].publish(String(data=json.dumps(envelope)))
                subtask['status'] = 'dispatched'
                mission['status'] = 'executing'
                robot_state['active_mission_id'] = mission['mission_id']
                robot_state['active_subtask_id'] = subtask['subtask_id']
                self._publish_feedback(
                    f"Dispatched {mission['mission_id']}/{subtask['subtask_id']} to {robot_name}: {subtask['instruction_text']}"
                )
                return

    def _resume_preempted_missions(self) -> None:
        for robot_name, robot_state in self.robot_states.items():
            if robot_state['active_subtask_id'] is not None or robot_state['control_in_progress']:
                continue
            if self._robot_has_incomplete_urgent_mission(robot_name):
                continue
            for mission_id in self.mission_state['mission_order']:
                mission = self.mission_state['missions'][mission_id]
                if mission.get('assigned_robot') != robot_name:
                    continue
                if not str(mission.get('pause_reason', '')).startswith('preempted_by_'):
                    continue
                if mission.get('status') != 'paused':
                    continue
                mission['status'] = 'planned'
                mission['pause_reason'] = ''
                self._publish_feedback(f'Resuming paused mission {mission_id} on {robot_name}.')
                break

    def _robot_has_incomplete_urgent_mission(self, robot_name: str) -> bool:
        for mission_id in self.mission_state['mission_order']:
            mission = self.mission_state['missions'][mission_id]
            if mission.get('assigned_robot') != robot_name:
                continue
            if mission.get('priority', NORMAL_PRIORITY) != URGENT_PRIORITY:
                continue
            if mission.get('status') != 'completed':
                return True
        return False

    def _pause_active_work(self, reason: str) -> None:
        for robot_name, robot_state in self.robot_states.items():
            mission_id = robot_state['active_mission_id']
            subtask_id = robot_state['active_subtask_id']
            if mission_id is None or subtask_id is None or robot_state['control_in_progress']:
                continue

            mission = self.mission_state['missions'].get(mission_id)
            if mission is None:
                continue
            for subtask in mission['subtasks']:
                if subtask['subtask_id'] == subtask_id:
                    subtask['status'] = 'paused'
            mission['status'] = 'paused'
            mission['pause_reason'] = reason
            home_location = self.robot_configs[robot_name].get('home_location', 'spawn')
            control_envelope = {
                'mission_id': mission_id,
                'subtask_id': f'{subtask_id}_return_home',
                'robot_id': robot_name,
                'instruction_text': f'Stop current work and return to {home_location} immediately.',
                'resume_context': f'Paused because {reason}.',
                'priority': 'high',
                'dispatch_reason': 'return_home',
            }
            self.robot_task_publishers[robot_name].publish(String(data=json.dumps(control_envelope)))
            robot_state['control_in_progress'] = True
            self._publish_feedback(f"Paused {mission_id} on {robot_name}: {reason}")
        self._save_state()

    def _request_mission_plan(self, mission_request: str, assigned_robot: str) -> dict:
        if not self.model_id:
            self.model_id = os.getenv('BEDROCK_MODEL_ID', '').strip()
        if not self.model_id:
            raise RuntimeError('BEDROCK_MODEL_ID is not set.')
        if not os.getenv('AWS_BEARER_TOKEN_BEDROCK', '').strip():
            raise RuntimeError('AWS_BEARER_TOKEN_BEDROCK is not set.')

        weather_context = self.weather_snapshot or {}
        assignments = {
            robot_name: {
                'active_mission_id': state['active_mission_id'],
                'active_subtask_id': state['active_subtask_id'],
                'agent_status': state['agent_status'],
            }
            for robot_name, state in self.robot_states.items()
        }
        prompt = (
            f"{self.scheduler_prompt}\n\n"
            f"Managed robots: {', '.join(sorted(self.robot_configs.keys()))}\n"
            f"Assigned robot: {assigned_robot}\n"
            f"Current fleet assignments: {json.dumps(assignments)}\n"
            f"Current weather/daylight snapshot: {json.dumps(weather_context)}\n"
            f"Available locations:\n{self._locations_text()}\n\n"
            f"Mission request:\n{mission_request}\n\n"
            "Return only JSON with this shape:\n"
            '{'
            '"subtasks":[{"subtask_id":"string","instruction_text":"string","resume_context":"string"}]'
            '}\n'
            "Do not use markdown fences. Do not include comments. "
            "Keep the plan compact. For repeated patrols or long-duration work, "
            "return one safe cycle of subtasks instead of expanding every repetition."
        )

        text = self._converse_text([{'role': 'user', 'content': [{'text': prompt}]}])
        try:
            return self._parse_json_response(text)
        except json.JSONDecodeError as exc:
            repaired_text = self._repair_plan_json(text, exc)
            return self._parse_json_response(repaired_text)

    def _get_bedrock_client(self):
        if self.bedrock_client is None:
            if boto3 is None:
                raise RuntimeError('boto3 is not installed. Install it with: python3 -m pip install boto3')
            config = Config(retries={'max_attempts': 3, 'mode': 'standard'}, read_timeout=120)
            self.bedrock_client = boto3.client(
                service_name='bedrock-runtime',
                region_name=self.region_name,
                config=config,
            )
        return self.bedrock_client

    def _converse_text(self, messages: list[dict[str, Any]]) -> str:
        response = self._get_bedrock_client().converse(
            modelId=self.model_id,
            messages=messages,
            inferenceConfig={'maxTokens': 1024, 'temperature': 0.2},
        )
        content = response['output']['message']['content']
        return '\n'.join(block['text'] for block in content if 'text' in block).strip()

    def _repair_plan_json(self, raw_text: str, parse_error: json.JSONDecodeError) -> str:
        repair_prompt = (
            "Convert the following scheduler plan into valid JSON only.\n"
            "Preserve meaning, but fix syntax.\n"
            "Return exactly this shape:\n"
            '{\"subtasks\":[{\"subtask_id\":\"string\",\"instruction_text\":\"string\",\"resume_context\":\"string\"}]}\n\n'
            f"Parser error:\n{parse_error}\n\n"
            f"Invalid content:\n{raw_text}"
        )
        repaired = self._converse_text([{'role': 'user', 'content': [{'text': repair_prompt}]}])
        if repaired:
            self._publish_feedback('Scheduler planner returned invalid JSON once; using repair pass.')
        return repaired

    @staticmethod
    def _parse_json_response(text: str) -> dict[str, Any]:
        cleaned = text.strip()
        if cleaned.startswith('```'):
            cleaned = cleaned.strip('`')
            if cleaned.startswith('json'):
                cleaned = cleaned[4:].strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            start = cleaned.find('{')
            end = cleaned.rfind('}')
            if start == -1 or end == -1 or end <= start:
                raise
            return json.loads(cleaned[start : end + 1])

    def _publish_configuration_warning(self) -> None:
        issues = []
        if boto3 is None:
            issues.append('boto3 is not installed. Install it with: python3 -m pip install boto3')
        if not os.getenv('AWS_BEARER_TOKEN_BEDROCK', '').strip():
            issues.append('AWS_BEARER_TOKEN_BEDROCK is not set.')
        if not self.model_id:
            issues.append('BEDROCK_MODEL_ID is not set.')
        if issues:
            self._publish_feedback(' '.join(issues))

    def _work_allowed(self) -> tuple[bool, str]:
        if self.weather_snapshot is None:
            return False, 'weather_api_unavailable'

        now = datetime.now(ZoneInfo(self.scheduler_config['site']['timezone']))
        sunrise = datetime.fromisoformat(self.weather_snapshot['sunrise']).astimezone(
            ZoneInfo(self.scheduler_config['site']['timezone'])
        )
        sunset = datetime.fromisoformat(self.weather_snapshot['sunset']).astimezone(
            ZoneInfo(self.scheduler_config['site']['timezone'])
        )
        if now < sunrise:
            return False, 'before_sunrise'
        if now >= sunset:
            return False, 'after_sunset'
        if self.weather_snapshot['blocking_reason']:
            return False, self.weather_snapshot['blocking_reason']
        return True, ''

    def _refresh_weather(self) -> None:
        site_config = self.scheduler_config['site']
        try:
            payload = self.weather_client.fetch_conditions(
                latitude=float(site_config['latitude']),
                longitude=float(site_config['longitude']),
                timezone=site_config['timezone'],
                forecast_hours=int(self.scheduler_config['forecast_lookahead_hours']),
            )
        except OpenMeteoError as exc:
            self.weather_snapshot = None
            self._publish_feedback(str(exc))
            return

        current_code = payload['current'].get('weather_code')
        hourly_codes = payload['hourly'].get('weather_code', [])
        blocking_reason = ''
        if self.weather_client.is_blocking_weather(current_code):
            blocking_reason = 'current_rain_or_snow'
        else:
            for code in hourly_codes:
                if self.weather_client.is_blocking_weather(code):
                    blocking_reason = 'forecast_rain_or_snow'
                    break

        self.weather_snapshot = {
            'current_weather_code': current_code,
            'forecast_weather_codes': hourly_codes,
            'sunrise': payload['daily']['sunrise'][0],
            'sunset': payload['daily']['sunset'][0],
            'blocking_reason': blocking_reason,
        }
        self.mission_state['weather'] = deepcopy(self.weather_snapshot)
        self._save_state()

    def _locations_text(self) -> str:
        package_share = get_package_share_directory('tb3_agent')
        locations = self._load_yaml(Path(package_share) / 'config' / 'locations.yaml')
        lines = []
        for name, location in sorted(locations.items()):
            lines.append(f'- {name}: x={location["x"]}, y={location["y"]}, yaw={location.get("yaw", 0.0)}')
        return '\n'.join(lines)

    def _publish_status(self) -> None:
        snapshot = {
            'scheduler_state': self._derive_scheduler_state(),
            'missions': {
                mission_id: {
                    'status': self.mission_state['missions'][mission_id]['status'],
                    'assigned_robot': self.mission_state['missions'][mission_id]['assigned_robot'],
                    'priority': self.mission_state['missions'][mission_id].get('priority', NORMAL_PRIORITY),
                    'pause_reason': self.mission_state['missions'][mission_id]['pause_reason'],
                }
                for mission_id in self.mission_state['mission_order']
            },
            'robots': {
                robot_name: {
                    'active_mission_id': state['active_mission_id'],
                    'active_subtask_id': state['active_subtask_id'],
                    'agent_status': state['agent_status'],
                }
                for robot_name, state in self.robot_states.items()
            },
            'weather_block_reason': self.weather_snapshot['blocking_reason'] if self.weather_snapshot else 'unknown',
        }
        self.status_publisher.publish(String(data=json.dumps(snapshot)))

    def _publish_assignments(self) -> None:
        assignments = {
            robot_name: {
                'active_mission_id': state['active_mission_id'],
                'active_subtask_id': state['active_subtask_id'],
            }
            for robot_name, state in self.robot_states.items()
        }
        self.assignments_publisher.publish(String(data=json.dumps(assignments)))

    def _publish_feedback(self, message: str) -> None:
        self.feedback_publisher.publish(String(data=message))
        self.get_logger().info(message)

    def _derive_scheduler_state(self) -> str:
        allowed, reason = self._work_allowed()
        if not allowed:
            return reason
        if any(state['active_subtask_id'] is not None for state in self.robot_states.values()):
            return 'executing'
        if any(
            self.mission_state['missions'][mission_id]['status'] != 'completed'
            for mission_id in self.mission_state['mission_order']
        ):
            return 'ready'
        return 'idle'


def main() -> None:
    rclpy.init()
    node = SchedulerAgentNode()
    rclpy.spin(node)
    node.destroy_node()
    rclpy.shutdown()
