Create a child TaskPlan from the current TaskPlan task and pause the current task until that child plan completes.

Use this tool when the current task needs auditable sub-work from one or more specialist agents before it can produce its final result. The child plan should be append-only work: do not use it to rewrite the parent plan or to hide decisions that should be visible in the plan history.

The `task_plan` argument must contain a valid TaskPlan with `id`, `root_task_id`, `tasks`, and optional `dependencies`. Every child task's `agent_id` must refer to an agent that exists in the runtime. Use `cinder.list_agents` first if you need to inspect available agents.

After this tool succeeds, stop. Do not call `cinder.submit_task` in the same turn. The runtime will run the child plan, then resume this task by adding the child plan result as the tool result for this call.
