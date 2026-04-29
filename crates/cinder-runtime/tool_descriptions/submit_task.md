Submit the final structured result for the current TaskPlan task.

Use this tool only when the current task is complete and no further model turn is needed. For a successful task, call it with `status = "succeeded"` and a `result` object that matches the task's `output_schema`.

For a failed task, call it with `status = "failed"` and an `error` string that clearly explains why the task cannot produce a valid result. A failed task will fail the containing plan.

Do not use this tool to request a child plan. If specialist sub-work is needed first, call `cinder.spawn_task_plan`, stop, and wait for the runtime to resume you with the child result. Do not wrap the result in Markdown unless the task output schema explicitly asks for a Markdown field.
