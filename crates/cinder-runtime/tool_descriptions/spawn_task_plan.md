Create a TaskPlan and pause the current agent run until that plan completes.

Use this tool when the current user request or current TaskPlan task needs auditable sub-work from one or more specialist agents before it can produce its final result. A normal main agent can call this tool to dynamically delegate to subagents. A TaskPlan worker agent can also call it recursively when its task needs specialist sub-work and its configured tools allow that.

The plan should be append-only work: do not use it to rewrite hidden state or to hide decisions that should be visible in the plan history.

The `task_plan` argument must contain a valid TaskPlan with `id`, `tasks`, and optional `dependencies`. Every child task's `agent_id` must refer to an agent that exists in the runtime. Use `cinder.list_agents` first if you need to inspect available agents.

Each item in `tasks` is a TaskSpec:

- `id`: unique task id inside this child plan.
- `agent_id`: configured agent id that will execute the task.
- `prompt`: task-specific instructions for that agent.
- `input`: optional initial JSON input for the task.
- `output_schema`: JSON Schema that the task result must satisfy.

`dependencies` describes execution order and data flow. A dependency edge means `to_task` waits until `from_task` succeeds. When an upstream task succeeds, all top-level fields from its output object are merged into the downstream task input object.

Design upstream task output schemas so fields have the exact names downstream tasks should consume. If several upstream tasks feed the same downstream task, prefer distinct field names such as `research_markdown`, `formal_markdown`, and `draft_markdown` to avoid collisions.

Tasks without incoming dependencies may run first. A downstream task becomes ready only after all of its upstream dependencies have succeeded.

The plan result is inferred from terminal tasks, which are tasks with no outgoing dependencies. The result is always an object keyed by terminal task id, with each value set to that task's output. For example, a single terminal task named `review` returns `{ "review": <review output> }`.

After this tool succeeds, stop. The runtime will run the plan, then resume this agent by adding the plan result as the tool result for this call. If you are inside a TaskPlan task, do not call `cinder.submit_task` in the same turn; wait for the tool result first.
