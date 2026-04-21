import json
import os
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ament_index_python.packages import get_package_share_directory

try:
    import boto3
    from botocore.config import Config
except ImportError:  # pragma: no cover - handled at runtime
    boto3 = None
    Config = None
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
URGENT_KEYWORDS = {
    'urgent',
    'urgently',
    'immediately',
    'emergency',
    'high priority',
    'asap',
}


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
        self.last_scheduler_block_reason = ''

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
        self._drop_unrequested_return_home_subtasks_from_state()
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

    def _drop_unrequested_return_home_subtasks_from_state(self) -> None:
        changed = False
        for mission_id in self.mission_state.get('mission_order', []):
            mission = self.mission_state['missions'][mission_id]
            retained_subtasks = []
            for subtask in mission.get('subtasks', []):
                if self._should_drop_unrequested_return_home_subtask(mission, subtask):
                    changed = True
                    self._publish_feedback(
                        f"Removed stale unrequested return-to-spawn subtask from urgent mission {mission_id}."
                    )
                    continue
                retained_subtasks.append(subtask)
            mission['subtasks'] = retained_subtasks
        if changed:
            self._save_state()

    def _mission_request_callback(self, msg: String) -> None:
        request_text = msg.data.strip()
        if not request_text:
            return
        parsed = self._parse_mission_request(request_text)
        if self._handle_control_text(parsed['request_text']):
            return
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
            return {'request_text': request_text, 'priority': self._infer_priority(request_text)}

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

    @staticmethod
    def _infer_priority(text: str) -> str:
        normalized = text.lower()
        for keyword in URGENT_KEYWORDS:
            if keyword in normalized:
                return URGENT_PRIORITY
        return NORMAL_PRIORITY

    def _preempt_robot_for_urgent_mission(self, robot_name: str, urgent_mission_id: str) -> None:
        robot_state = self.robot_states[robot_name]
        mission_id = robot_state['active_mission_id']
        subtask_id = robot_state['active_subtask_id']
        if mission_id is None or subtask_id is None:
            return

        current_mission = self.mission_state['missions'].get(mission_id)
        if current_mission is None or mission_id == urgent_mission_id:
            return
        if (
            current_mission.get('priority', NORMAL_PRIORITY) == URGENT_PRIORITY
            and self._mission_order_index(current_mission['mission_id']) > self._mission_order_index(urgent_mission_id)
        ):
            return

        matched_active_subtask = False
        for subtask in current_mission.get('subtasks', []):
            if subtask.get('subtask_id') == subtask_id:
                subtask['status'] = 'paused'
                subtask['last_message'] = f'Preempted by urgent mission {urgent_mission_id}.'
                matched_active_subtask = True
                break
        if not matched_active_subtask:
            for subtask in current_mission.get('subtasks', []):
                if subtask.get('status') == 'dispatched':
                    subtask['status'] = 'paused'
                    subtask['last_message'] = f'Preempted by urgent mission {urgent_mission_id}.'
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
        robot_state['active_mission_id'] = mission_id
        robot_state['active_subtask_id'] = control_envelope['subtask_id']
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
        mission_id = str(result.get('mission_id', ''))
        subtask_id = str(result.get('subtask_id', ''))
        robot_state = self.robot_states[robot_name]
        active_matches = (
            robot_state.get('active_mission_id') == mission_id
            and robot_state.get('active_subtask_id') == subtask_id
        )
        mission = self.mission_state['missions'].get(mission_id)
        if mission is None:
            if active_matches:
                self._clear_robot_assignment(robot_name)
            self._publish_feedback(
                f"{robot_name} reported result for non-mission task {mission_id}/{subtask_id}: "
                f"{result.get('status', 'unknown')} - {result.get('message', '')}"
            )
            return

        matched_subtask = False
        for subtask in mission['subtasks']:
            if subtask['subtask_id'] != subtask_id:
                continue
            matched_subtask = True
            if not active_matches and subtask.get('status') != 'dispatched':
                subtask['last_message'] = f"Stale result ignored: {result.get('message', '')}"
                self._publish_feedback(
                    f"Ignored stale result for {mission_id}/{subtask_id} on {robot_name}: "
                    f"{result.get('status', 'unknown')} - {result.get('message', '')}"
                )
                self._save_state()
                return
            subtask['last_message'] = result.get('message', '')
            status = result.get('status', 'failed')
            if status == 'completed':
                subtask['status'] = 'completed'
                subtask['completed_outcome_tags'] = self._normalize_tags(
                    result.get('completed_outcome_tags', [])
                )
                subtask['completion_summary'] = result.get('completion_summary', result.get('message', ''))
            elif status == 'cancelled':
                subtask['status'] = 'paused'
                if not str(mission.get('pause_reason', '')).startswith('preempted_by_'):
                    mission['status'] = 'paused'
                    mission['pause_reason'] = 'cancelled'
            elif status == 'needs_clarification':
                subtask['status'] = 'needs_clarification'
                mission['status'] = 'paused'
                mission['pause_reason'] = 'needs_clarification'
            else:
                subtask['status'] = 'failed'
                mission['status'] = 'paused'
                mission['pause_reason'] = status

        if matched_subtask:
            has_dispatched_subtask = any(
                subtask.get('status') == 'dispatched' for subtask in mission.get('subtasks', [])
            )
            if not has_dispatched_subtask and mission.get('status') == 'executing':
                mission['status'] = 'planned'

        if not matched_subtask:
            self._publish_feedback(
                f"{robot_name} completed control task {mission_id}/{subtask_id}: {result.get('message', '')}"
            )

        if active_matches:
            self._clear_robot_assignment(robot_name)
        else:
            self._publish_feedback(
                f"Received non-active result for {mission_id}/{subtask_id}; current active task remains unchanged."
            )

        if mission['subtasks'] and all(subtask['status'] == 'completed' for subtask in mission['subtasks']):
            mission['status'] = 'completed'
            mission['pause_reason'] = ''
            if mission.get('priority', NORMAL_PRIORITY) == URGENT_PRIORITY:
                self._apply_completed_outcomes_to_pending_work(mission)
        elif matched_subtask and mission['status'] not in {'paused', 'completed'}:
            mission['status'] = 'planned'
        self._save_state()
        self._publish_feedback(
            f"{robot_name} reported {result.get('status')} for {mission_id}/{subtask_id}: {result.get('message', '')}"
        )

    def _clear_robot_assignment(self, robot_name: str) -> None:
        self.robot_states[robot_name]['active_mission_id'] = None
        self.robot_states[robot_name]['active_subtask_id'] = None
        self.robot_states[robot_name]['control_in_progress'] = False

    def _scheduler_tick(self) -> None:
        self._publish_status()
        allowed, reason = self._work_allowed()
        if not allowed:
            self._publish_blocked_if_needed(reason)
            self._pause_active_work(reason)
            return
        self.last_scheduler_block_reason = ''

        self._preempt_for_waiting_urgent_missions()

        for mission_id in self._ordered_mission_ids():
            mission = self.mission_state['missions'][mission_id]
            if mission['status'] == 'pending_plan':
                self._plan_mission(mission)
            if mission['status'] in {'planned', 'executing', 'paused'}:
                self._dispatch_next_subtask(mission)

        self._resume_preempted_missions()

        self._publish_assignments()
        self._save_state()

    def _ordered_mission_ids(self) -> list[str]:
        def sort_key(mission_id: str) -> tuple[int, int]:
            mission = self.mission_state['missions'][mission_id]
            priority_rank = 0 if mission.get('priority', NORMAL_PRIORITY) == URGENT_PRIORITY else 1
            order_index = self.mission_state['mission_order'].index(mission_id)
            if priority_rank == 0:
                order_index = -order_index
            return priority_rank, order_index

        return sorted(self.mission_state['mission_order'], key=sort_key)

    def _preempt_for_waiting_urgent_missions(self) -> None:
        for robot_name in self.robot_configs:
            mission_id = self._latest_incomplete_urgent_mission_id(robot_name)
            if mission_id is None:
                continue
            active_mission_id = self.robot_states[robot_name]['active_mission_id']
            if self.robot_states[robot_name]['control_in_progress']:
                continue
            if active_mission_id is None or active_mission_id == mission_id:
                continue
            self._preempt_robot_for_urgent_mission(robot_name, mission_id)
            return

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
            if self._should_drop_unrequested_return_home_subtask(mission, subtask):
                self._publish_feedback(
                    f"Dropped unrequested return-to-spawn subtask from urgent mission {mission['mission_id']}."
                )
                continue
            subtasks.append(
                {
                    'subtask_id': subtask.get('subtask_id') or f"{mission['mission_id']}_subtask_{index:03d}",
                    'instruction_text': subtask['instruction_text'],
                    'resume_context': subtask.get('resume_context', ''),
                    'completion_criteria': self._normalize_text_list(subtask.get('completion_criteria', [])),
                    'outcome_tags': self._normalize_tags(subtask.get('outcome_tags', [])),
                    'status': 'pending',
                    'last_message': '',
                    'completed_outcome_tags': [],
                    'completion_summary': '',
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

    def _should_drop_unrequested_return_home_subtask(self, mission: dict, subtask: dict) -> bool:
        if mission.get('priority', NORMAL_PRIORITY) != URGENT_PRIORITY:
            return False
        if self._mission_explicitly_requests_return_home(mission.get('original_request', '')):
            return False
        text = ' '.join(
            [
                str(subtask.get('instruction_text', '')),
                str(subtask.get('resume_context', '')),
                ' '.join(self._normalize_text_list(subtask.get('completion_criteria', []))),
                ' '.join(self._normalize_tags(subtask.get('outcome_tags', []))),
            ]
        ).lower()
        return 'spawn' in text and any(token in text for token in ('return', 'home', 'returned'))

    @staticmethod
    def _mission_explicitly_requests_return_home(request_text: str) -> bool:
        normalized = request_text.lower()
        return (
            'return to spawn' in normalized
            or 'go to spawn' in normalized
            or 'return home' in normalized
            or 'go home' in normalized
        )

    def _dispatch_next_subtask(self, mission: dict) -> None:
        robot_name = mission['assigned_robot']
        robot_state = self.robot_states[robot_name]
        if robot_state['active_subtask_id'] is not None:
            return
        if (
            mission.get('priority', NORMAL_PRIORITY) == URGENT_PRIORITY
            and self._robot_has_newer_incomplete_urgent_mission(robot_name, mission['mission_id'])
        ):
            return
        if (
            mission.get('priority', NORMAL_PRIORITY) != URGENT_PRIORITY
            and self._robot_has_incomplete_urgent_mission(robot_name)
        ):
            return
        if (
            mission['status'] == 'paused'
            and mission.get('pause_reason') not in AUTO_RESUME_PAUSE_REASONS
            and not str(mission.get('pause_reason', '')).startswith('preempted_by_')
        ):
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
                    'completion_criteria': subtask.get('completion_criteria', []),
                    'outcome_tags': subtask.get('outcome_tags', []),
                    'priority': mission.get('priority', NORMAL_PRIORITY),
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
            for mission_id in self._ordered_mission_ids():
                mission = self.mission_state['missions'][mission_id]
                if mission.get('assigned_robot') != robot_name:
                    continue
                if not str(mission.get('pause_reason', '')).startswith('preempted_by_'):
                    continue
                if mission.get('status') != 'paused':
                    continue
                if (
                    mission.get('priority', NORMAL_PRIORITY) == URGENT_PRIORITY
                    and self._robot_has_newer_incomplete_urgent_mission(robot_name, mission_id)
                ):
                    continue
                if (
                    mission.get('priority', NORMAL_PRIORITY) != URGENT_PRIORITY
                    and self._robot_has_incomplete_urgent_mission(robot_name)
                ):
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

    def _latest_incomplete_urgent_mission_id(self, robot_name: str) -> str | None:
        latest_mission_id = None
        for mission_id in self.mission_state['mission_order']:
            mission = self.mission_state['missions'][mission_id]
            if mission.get('assigned_robot') != robot_name:
                continue
            if mission.get('priority', NORMAL_PRIORITY) != URGENT_PRIORITY:
                continue
            if mission.get('status') == 'completed':
                continue
            latest_mission_id = mission_id
        return latest_mission_id

    def _robot_has_newer_incomplete_urgent_mission(self, robot_name: str, mission_id: str) -> bool:
        mission_index = self._mission_order_index(mission_id)
        for candidate_id in self.mission_state['mission_order']:
            mission = self.mission_state['missions'][candidate_id]
            if mission.get('assigned_robot') != robot_name:
                continue
            if mission.get('priority', NORMAL_PRIORITY) != URGENT_PRIORITY:
                continue
            if mission.get('status') == 'completed':
                continue
            if self._mission_order_index(candidate_id) > mission_index:
                return True
        return False

    def _mission_order_index(self, mission_id: str) -> int:
        try:
            return self.mission_state['mission_order'].index(mission_id)
        except ValueError:
            return -1

    def _mission_started_before(self, candidate_id: str, reference_id: str) -> bool:
        candidate_index = self._mission_order_index(candidate_id)
        reference_index = self._mission_order_index(reference_id)
        return candidate_index >= 0 and reference_index >= 0 and candidate_index < reference_index

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
            robot_state['active_mission_id'] = mission_id
            robot_state['active_subtask_id'] = control_envelope['subtask_id']
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
            '"subtasks":[{"subtask_id":"string","instruction_text":"string","resume_context":"string",'
            '"completion_criteria":["string"],"outcome_tags":["string"]}]'
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
            '{\"subtasks\":[{\"subtask_id\":\"string\",\"instruction_text\":\"string\",\"resume_context\":\"string\",'
            '\"completion_criteria\":[\"string\"],\"outcome_tags\":[\"string\"]}]}\n\n'
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

        site_timezone = ZoneInfo(self.scheduler_config['site']['timezone'])
        now = datetime.now(site_timezone)
        sunrise = self._parse_site_datetime(self.weather_snapshot['sunrise'], site_timezone)
        sunset = self._parse_site_datetime(self.weather_snapshot['sunset'], site_timezone)
        if now < sunrise:
            return False, 'before_sunrise'
        if now >= sunset:
            return False, 'after_sunset'
        if self.weather_snapshot['blocking_reason']:
            return False, self.weather_snapshot['blocking_reason']
        return True, ''

    @staticmethod
    def _parse_site_datetime(value: str, site_timezone: ZoneInfo) -> datetime:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=site_timezone)
        return parsed.astimezone(site_timezone)

    def _publish_blocked_if_needed(self, reason: str) -> None:
        if reason == self.last_scheduler_block_reason:
            return
        has_open_missions = any(
            self.mission_state['missions'][mission_id].get('status') != 'completed'
            for mission_id in self.mission_state['mission_order']
        )
        if has_open_missions:
            self._publish_feedback(f'Scheduler is not planning because work is currently blocked: {reason}.')
        self.last_scheduler_block_reason = reason

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

    def _apply_completed_outcomes_to_pending_work(self, completed_mission: dict) -> None:
        completed_subtasks = [
            subtask
            for subtask in completed_mission.get('subtasks', [])
            if subtask.get('status') == 'completed' and subtask.get('completed_outcome_tags')
        ]
        if not completed_subtasks:
            return

        for completed_subtask in completed_subtasks:
            completed_tags = self._normalize_tags(completed_subtask.get('completed_outcome_tags', []))
            if not completed_tags:
                continue
            for mission_id in self.mission_state['mission_order']:
                mission = self.mission_state['missions'][mission_id]
                if not self._eligible_for_urgent_outcome_satisfaction(mission, completed_mission):
                    continue
                for pending_subtask in mission.get('subtasks', []):
                    if pending_subtask.get('status') not in {'pending', 'paused'}:
                        continue
                    if self._completed_outcome_satisfies_subtask(completed_subtask, pending_subtask):
                        pending_subtask['status'] = 'completed'
                        pending_subtask['completed_outcome_tags'] = completed_tags
                        pending_subtask['completion_summary'] = (
                            f"Satisfied by urgent mission {completed_mission['mission_id']} "
                            f"subtask {completed_subtask.get('subtask_id', '')}."
                        )
                        pending_subtask['last_message'] = pending_subtask['completion_summary']
                        self._publish_feedback(
                            f"Marked {mission_id}/{pending_subtask.get('subtask_id', '')} complete because "
                            f"urgent mission {completed_mission['mission_id']} satisfied the same outcome."
                        )

                if mission.get('subtasks') and all(
                    subtask.get('status') == 'completed' for subtask in mission.get('subtasks', [])
                ):
                    mission['status'] = 'completed'
                    mission['pause_reason'] = ''

    def _eligible_for_urgent_outcome_satisfaction(self, candidate_mission: dict, completed_mission: dict) -> bool:
        candidate_id = candidate_mission.get('mission_id', '')
        completed_id = completed_mission.get('mission_id', '')
        if candidate_id == completed_id:
            return False
        if completed_mission.get('priority', NORMAL_PRIORITY) != URGENT_PRIORITY:
            return False
        if candidate_mission.get('assigned_robot') != completed_mission.get('assigned_robot'):
            return False
        if candidate_mission.get('priority', NORMAL_PRIORITY) == URGENT_PRIORITY:
            return False
        if candidate_mission.get('status') == 'completed':
            return False
        return self._mission_started_before(candidate_id, completed_id)

    def _completed_outcome_satisfies_subtask(self, completed_subtask: dict, pending_subtask: dict) -> bool:
        completed_tags = set(self._normalize_tags(completed_subtask.get('completed_outcome_tags', [])))
        required_tags = set(self._normalize_tags(pending_subtask.get('outcome_tags', [])))
        if completed_tags and required_tags and required_tags.issubset(completed_tags):
            return True
        return self._verify_outcome_match_with_bedrock(completed_subtask, pending_subtask)

    def _verify_outcome_match_with_bedrock(self, completed_subtask: dict, pending_subtask: dict) -> bool:
        try:
            prompt = (
                "Decide whether a completed urgent robot task satisfies a pending scheduled subtask.\n"
                "Be conservative. Do not match based on location alone. Match only when the same concrete "
                "outcome/action/object/location is completed.\n"
                "For simple room patrol work, visiting/checking/inspecting the same area can satisfy another "
                "passive visit/check/inspect subtask. For manipulation, retrieval, delivery, or object tasks, "
                "match only if the same action, object, source, and destination are satisfied.\n"
                "Return JSON only with this exact shape: {\"satisfies\":true,\"reason\":\"string\"}\n\n"
                f"Completed urgent subtask:\n{json.dumps(completed_subtask)}\n\n"
                f"Pending subtask:\n{json.dumps(pending_subtask)}"
            )
            response = self._parse_json_response(
                self._converse_text([{'role': 'user', 'content': [{'text': prompt}]}])
            )
        except Exception as exc:
            self._publish_feedback(f'Outcome verifier could not compare subtasks safely: {exc}')
            return False

        satisfies = bool(response.get('satisfies', False))
        reason = str(response.get('reason', '')).strip()
        if satisfies:
            self._publish_feedback(f'Outcome verifier approved subtask skip: {reason}')
        return satisfies

    @staticmethod
    def _normalize_tags(tags: Any) -> list[str]:
        if not isinstance(tags, list):
            return []
        normalized = []
        for tag in tags:
            value = str(tag).strip().lower().replace(' ', '_')
            if value:
                normalized.append(value)
        return sorted(set(normalized))

    @staticmethod
    def _normalize_text_list(values: Any) -> list[str]:
        if not isinstance(values, list):
            return []
        return [str(value).strip() for value in values if str(value).strip()]

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
