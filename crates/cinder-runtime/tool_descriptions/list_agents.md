List the agents available in this Cinder runtime.

Use this tool when you need to decide which specialist agent should receive a TaskPlan task, or when you need to inspect the current runtime's agent directory before creating a child plan.

The result includes each agent's id, description, provider, model, configured tools, and configured skills. The `id` field is the value that TaskPlan tasks must use as `agent_id`.

Call this before `cinder.spawn_task_plan` if you are unsure which agents exist or what each agent is responsible for. Do not invent agent ids that are not returned by this tool.
