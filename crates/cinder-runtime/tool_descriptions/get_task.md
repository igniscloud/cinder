Return the current Cinder TaskPlan task assigned to this agent run.

Use this tool when you are a TaskPlan worker agent and you do not yet know the task you should execute. If you have no task details in the conversation, call `cinder.get_task` before doing any substantive work.

The result includes the plan id, plan run id, task id, task prompt, merged input JSON, output schema, and current task state. Treat the returned `prompt` as the authoritative task instructions and the returned `input` as the data you should work from.

If the result has `has_task: false`, this agent run is not currently executing a TaskPlan task. In that case, continue from the normal conversation instead of calling `cinder.submit_task`.

After completing a TaskPlan task, submit the final structured result with `cinder.submit_task`. The result must satisfy the returned `output_schema`.
