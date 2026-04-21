import json
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


MAX_EVENTS = 80
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 8080


class SchedulerUiNode(Node):
    def __init__(self) -> None:
        super().__init__('scheduler_ui_node')

        self.declare_parameter('host', DEFAULT_HOST)
        self.declare_parameter('port', DEFAULT_PORT)
        self.host = self.get_parameter('host').value
        self.port = int(self.get_parameter('port').value)

        self._lock = threading.Lock()
        self._events: dict[str, deque[dict[str, Any]]] = {
            'interaction': deque(maxlen=MAX_EVENTS),
            'scheduler_feedback': deque(maxlen=MAX_EVENTS),
            'scheduler_status': deque(maxlen=MAX_EVENTS),
            'scheduler_mission_plan': deque(maxlen=MAX_EVENTS),
            'scheduler_active_assignments': deque(maxlen=MAX_EVENTS),
            'robot_agent_feedback': deque(maxlen=MAX_EVENTS),
            'robot_agent_status': deque(maxlen=MAX_EVENTS),
            'robot_scheduled_task_result': deque(maxlen=MAX_EVENTS),
            'robot_agent_command': deque(maxlen=MAX_EVENTS),
            'operator_requests': deque(maxlen=MAX_EVENTS),
        }
        self._latest: dict[str, Any] = {}

        self.scheduler_request_publisher = self.create_publisher(String, '/scheduler/mission_requests', 10)

        self.create_subscription(String, '/scheduler/feedback', self._make_callback('scheduler_feedback'), 10)
        self.create_subscription(String, '/scheduler/status', self._make_callback('scheduler_status'), 10)
        self.create_subscription(String, '/scheduler/mission_plan', self._make_callback('scheduler_mission_plan'), 10)
        self.create_subscription(
            String, '/scheduler/active_assignments', self._make_callback('scheduler_active_assignments'), 10
        )
        self.create_subscription(String, '/robot_1/agent_feedback', self._make_callback('robot_agent_feedback'), 10)
        self.create_subscription(String, '/robot_1/agent_status', self._make_callback('robot_agent_status'), 10)
        self.create_subscription(
            String, '/robot_1/scheduled_task_result', self._make_callback('robot_scheduled_task_result'), 10
        )
        self.create_subscription(String, '/robot_1/agent_command', self._make_callback('robot_agent_command'), 10)

        handler = self._build_handler()
        self._http_server = ThreadingHTTPServer((self.host, self.port), handler)
        self._server_thread = threading.Thread(target=self._http_server.serve_forever, daemon=True)
        self._server_thread.start()

        self.get_logger().info(f'Scheduler UI available at http://{self.host}:{self.port}')

    def destroy_node(self) -> bool:
        self._http_server.shutdown()
        self._http_server.server_close()
        return super().destroy_node()

    def publish_scheduler_request(self, text: str, priority: str) -> None:
        text = text.strip()
        if not text:
            raise ValueError('request text is empty')

        if priority == 'urgent':
            payload = json.dumps({'request_text': text, 'priority': 'urgent'})
        else:
            payload = text

        self.scheduler_request_publisher.publish(String(data=payload))
        self._record_event(
            'operator_requests',
            {
                'raw': text,
                'parsed': {'text': text, 'priority': priority},
                'timestamp': time.time(),
            },
        )
        self._record_event(
            'interaction',
            {
                'raw': text,
                'parsed': {'speaker': 'You', 'text': text, 'priority': priority},
                'timestamp': time.time(),
            },
        )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                'latest': dict(self._latest),
                'events': {name: list(events) for name, events in self._events.items()},
                'alerts': self._build_alerts_locked(),
            }

    def _make_callback(self, stream_name: str):
        def _callback(msg: String) -> None:
            timestamp = time.time()
            parsed = self._try_parse_json(msg.data)
            self._record_event(
                stream_name,
                {
                    'raw': msg.data,
                    'parsed': parsed,
                    'timestamp': timestamp,
                },
            )
            if stream_name == 'scheduler_feedback':
                self._record_event(
                    'interaction',
                    {
                        'raw': msg.data,
                        'parsed': {'speaker': 'Scheduler', 'text': msg.data},
                        'timestamp': timestamp,
                    },
                )

        return _callback

    def _record_event(self, stream_name: str, event: dict[str, Any]) -> None:
        with self._lock:
            self._events[stream_name].append(event)
            self._latest[stream_name] = event

    def _build_alerts_locked(self) -> list[str]:
        alerts = []
        scheduler_status = self._latest.get('scheduler_status', {}).get('parsed')
        if isinstance(scheduler_status, dict):
            for mission_id, mission in scheduler_status.get('missions', {}).items():
                if mission.get('pause_reason') == 'needs_clarification':
                    alerts.append(f'Scheduler needs clarification for {mission_id}.')
                elif str(mission.get('pause_reason', '')).startswith('planning_failed'):
                    alerts.append(f'Scheduler planning failed for {mission_id}.')

        latest_feedback = self._latest.get('robot_agent_feedback', {}).get('raw', '')
        if 'clarification' in latest_feedback.lower():
            alerts.append('Robot task agent is asking for clarification.')
        latest_scheduler_feedback = self._latest.get('scheduler_feedback', {}).get('raw', '')
        if 'clarification' in latest_scheduler_feedback.lower():
            alerts.append('Scheduler is asking for clarification.')
        latest_task_result = self._latest.get('robot_scheduled_task_result', {}).get('parsed')
        if isinstance(latest_task_result, dict) and latest_task_result.get('status') == 'needs_clarification':
            alerts.append('A scheduled robot task needs clarification.')
        return alerts

    @staticmethod
    def _try_parse_json(value: str) -> Any:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None

    def _build_handler(self):
        node = self

        class SchedulerUiHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                route = urlparse(self.path).path
                if route == '/':
                    self._send_text(HTML, 'text/html; charset=utf-8')
                    return
                if route == '/api/snapshot':
                    self._send_json(node.snapshot())
                    return
                if route == '/events':
                    self._send_events()
                    return
                self.send_error(404, 'Not found')

            def do_POST(self) -> None:
                route = urlparse(self.path).path
                if route != '/api/scheduler/request':
                    self.send_error(404, 'Not found')
                    return

                try:
                    content_length = int(self.headers.get('Content-Length', '0'))
                    body = self.rfile.read(content_length).decode('utf-8')
                    payload = json.loads(body)
                    text = str(payload.get('text', '')).strip()
                    priority = str(payload.get('priority', 'normal')).strip().lower()
                    if priority not in {'normal', 'urgent'}:
                        priority = 'normal'
                    node.publish_scheduler_request(text, priority)
                except Exception as exc:
                    self._send_json({'ok': False, 'error': str(exc)}, status=400)
                    return

                self._send_json({'ok': True})

            def log_message(self, format: str, *args) -> None:  # noqa: A002 - inherited API name
                node.get_logger().debug(format % args)

            def _send_events(self) -> None:
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Cache-Control', 'no-cache')
                self.send_header('Connection', 'keep-alive')
                self.end_headers()

                while True:
                    try:
                        data = json.dumps(node.snapshot())
                        self.wfile.write(f'data: {data}\n\n'.encode('utf-8'))
                        self.wfile.flush()
                        time.sleep(1.0)
                    except (BrokenPipeError, ConnectionResetError):
                        return

            def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
                self._send_text(json.dumps(payload), 'application/json; charset=utf-8', status=status)

            def _send_text(self, payload: str, content_type: str, status: int = 200) -> None:
                encoded = payload.encode('utf-8')
                self.send_response(status)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        return SchedulerUiHandler


HTML = r'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TB3 Scheduler Console</title>
  <style>
    :root {
      --ink: #17211f;
      --muted: #65736f;
      --paper: #f6efe3;
      --panel: rgba(255, 252, 245, 0.92);
      --line: #d5c7ae;
      --accent: #0f766e;
      --accent-strong: #0b4f49;
      --urgent: #b73525;
      --soft: #e9dcc6;
      --code: #26342f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at 12% 10%, rgba(15,118,110,0.18), transparent 28rem),
        radial-gradient(circle at 90% 5%, rgba(183,53,37,0.16), transparent 24rem),
        linear-gradient(135deg, #efe1c8 0%, #f8f2e8 50%, #dfebdf 100%);
      min-height: 100vh;
    }
    header {
      padding: 28px clamp(18px, 4vw, 52px) 12px;
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 20px;
    }
    h1 {
      font-size: clamp(30px, 5vw, 58px);
      line-height: 0.95;
      margin: 0;
      letter-spacing: -0.05em;
    }
    .subtitle { color: var(--muted); max-width: 720px; margin-top: 12px; font-size: 16px; }
    .status-pill {
      border: 1px solid var(--line);
      background: var(--panel);
      padding: 10px 14px;
      border-radius: 999px;
      font-size: 14px;
      white-space: nowrap;
      box-shadow: 0 12px 30px rgba(50, 38, 21, 0.08);
    }
    main {
      padding: 16px clamp(18px, 4vw, 52px) 46px;
      display: grid;
      grid-template-columns: minmax(320px, 0.9fr) minmax(360px, 1.3fr);
      gap: 22px;
    }
    section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 28px;
      box-shadow: 0 22px 70px rgba(54, 42, 25, 0.12);
      overflow: hidden;
    }
    .composer { padding: 24px; }
    label { display: block; font-size: 13px; text-transform: uppercase; letter-spacing: .12em; color: var(--muted); margin-bottom: 8px; }
    textarea {
      width: 100%;
      min-height: 180px;
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 16px;
      resize: vertical;
      background: #fffaf0;
      color: var(--ink);
      font: 18px/1.45 Georgia, "Times New Roman", serif;
      outline: none;
    }
    textarea:focus { border-color: var(--accent); box-shadow: 0 0 0 4px rgba(15,118,110,.12); }
    .controls { display: flex; gap: 12px; margin-top: 14px; flex-wrap: wrap; }
    select, button {
      border: 0;
      border-radius: 999px;
      padding: 12px 16px;
      font: 700 14px/1 system-ui, sans-serif;
    }
    select { background: var(--soft); color: var(--ink); border: 1px solid var(--line); }
    button { background: var(--accent); color: white; cursor: pointer; }
    button:hover { background: var(--accent-strong); }
    .hint { color: var(--muted); font-size: 14px; line-height: 1.4; margin-top: 14px; }
    .alerts { padding: 0 24px 24px; display: grid; gap: 10px; }
    .alert {
      padding: 12px 14px;
      border-radius: 16px;
      background: #fff1dd;
      border: 1px solid #ecc58b;
      color: #70410d;
      font-size: 14px;
    }
    .tabs { display: flex; gap: 8px; padding: 16px 16px 0; flex-wrap: wrap; }
    .tab {
      background: transparent;
      color: var(--muted);
      border: 1px solid var(--line);
      padding: 10px 12px;
    }
    .tab.active { background: var(--ink); color: white; border-color: var(--ink); }
    .panel { padding: 18px; display: none; }
    .panel.active { display: block; }
    .event-list { display: grid; gap: 12px; max-height: 68vh; overflow: auto; padding-right: 6px; }
    .event {
      border: 1px solid var(--line);
      background: #fffaf2;
      border-radius: 18px;
      padding: 14px;
    }
    .event time { color: var(--muted); font-size: 12px; display: block; margin-bottom: 8px; font-family: system-ui, sans-serif; }
    .event strong { display: block; margin-bottom: 8px; font: 800 13px/1 system-ui, sans-serif; color: var(--accent-strong); }
    pre {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      color: var(--code);
    }
    .empty { color: var(--muted); padding: 20px; border: 1px dashed var(--line); border-radius: 18px; }
    @media (max-width: 900px) {
      header { align-items: start; flex-direction: column; }
      main { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Scheduler Console</h1>
      <div class="subtitle">Talk to the fleet scheduler in plain language, send urgent work, and monitor scheduler/robot state without juggling terminal tabs.</div>
    </div>
    <div class="status-pill" id="connection">Connecting...</div>
  </header>
  <main>
    <section>
      <div class="composer">
        <label for="request">Message to scheduler</label>
        <textarea id="request" placeholder="Example: Urgently inspect the south east room."></textarea>
        <div class="controls">
          <select id="priority">
            <option value="normal">Normal mission</option>
            <option value="urgent">Urgent mission</option>
          </select>
          <button id="send">Send to scheduler</button>
        </div>
        <p class="hint">Control phrases like “stop all tasks and return to spawn” still go through this same scheduler channel.</p>
      </div>
      <div class="alerts" id="alerts"></div>
    </section>
    <section>
      <div class="tabs" id="tabs"></div>
      <div id="panels"></div>
    </section>
  </main>
  <script>
    const streams = [
      ['interaction', 'Conversation'],
      ['scheduler_feedback', 'Scheduler Feedback'],
      ['scheduler_status', 'Scheduler Status'],
      ['scheduler_mission_plan', 'Mission Plan'],
      ['scheduler_active_assignments', 'Assignments'],
      ['robot_agent_feedback', 'Agent Feedback'],
      ['robot_agent_status', 'Agent Status'],
      ['robot_scheduled_task_result', 'Task Results'],
      ['robot_agent_command', 'Agent Commands'],
      ['operator_requests', 'You Sent']
    ];
    const tabs = document.getElementById('tabs');
    const panels = document.getElementById('panels');
    let active = 'interaction';
    let snapshot = {events: {}, latest: {}, alerts: []};

    for (const [key, label] of streams) {
      const tab = document.createElement('button');
      tab.className = 'tab';
      tab.textContent = label;
      tab.onclick = () => { active = key; render(); };
      tab.dataset.key = key;
      tabs.appendChild(tab);

      const panel = document.createElement('div');
      panel.className = 'panel';
      panel.id = `panel-${key}`;
      panels.appendChild(panel);
    }

    document.getElementById('send').onclick = async () => {
      const text = document.getElementById('request').value.trim();
      const priority = document.getElementById('priority').value;
      if (!text) return;
      const response = await fetch('/api/scheduler/request', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({text, priority})
      });
      if (response.ok) document.getElementById('request').value = '';
    };

    const events = new EventSource('/events');
    events.onopen = () => document.getElementById('connection').textContent = 'Live';
    events.onerror = () => document.getElementById('connection').textContent = 'Reconnecting...';
    events.onmessage = (event) => {
      snapshot = JSON.parse(event.data);
      render();
    };

    function render() {
      for (const tab of tabs.children) tab.classList.toggle('active', tab.dataset.key === active);
      for (const panel of panels.children) panel.classList.toggle('active', panel.id === `panel-${active}`);

      const alerts = document.getElementById('alerts');
      alerts.innerHTML = '';
      for (const text of snapshot.alerts || []) {
        const div = document.createElement('div');
        div.className = 'alert';
        div.textContent = text;
        alerts.appendChild(div);
      }

      for (const [key] of streams) {
        const panel = document.getElementById(`panel-${key}`);
        const items = (snapshot.events && snapshot.events[key] || []).slice().reverse();
        if (!items.length) {
          panel.innerHTML = '<div class="empty">No messages yet.</div>';
          continue;
        }
        panel.innerHTML = `<div class="event-list">${items.map(renderEvent).join('')}</div>`;
      }
    }

    function renderEvent(item) {
      const date = new Date((item.timestamp || 0) * 1000).toLocaleTimeString();
      const speaker = item.parsed && item.parsed.speaker ? `<strong>${escapeHtml(item.parsed.speaker)}</strong>` : '';
      const body = item.parsed && item.parsed.speaker ? item.parsed.text : (item.parsed ? JSON.stringify(item.parsed, null, 2) : item.raw);
      return `<div class="event"><time>${escapeHtml(date)}</time>${speaker}<pre>${escapeHtml(body || '')}</pre></div>`;
    }

    function escapeHtml(value) {
      return String(value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }
  </script>
</body>
</html>
'''


def main() -> None:
    rclpy.init()
    node = SchedulerUiNode()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.shutdown()
