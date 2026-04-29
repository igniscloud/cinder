use crate::CinderCoreError;
use chrono::{DateTime, Utc};
use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskPlan {
    pub id: String,
    pub root_task_id: String,
    #[serde(default)]
    pub tasks: Vec<TaskSpec>,
    #[serde(default)]
    pub dependencies: Vec<TaskDependency>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskSpec {
    pub id: String,
    pub agent_id: String,
    pub prompt: String,
    #[serde(default)]
    pub input: Value,
    pub output_schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskDependency {
    pub from_task: String,
    pub to_task: String,
    #[serde(default)]
    pub bindings: Vec<OutputBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputBinding {
    pub from_pointer: String,
    pub to_pointer: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Running,
    WaitingChildPlan,
    Succeeded,
    Failed,
    Cancelled,
}

impl TaskState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            TaskState::Queued => "queued",
            TaskState::Running => "running",
            TaskState::WaitingChildPlan => "waiting_child_plan",
            TaskState::Succeeded => "succeeded",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
        };
        f.write_str(value)
    }
}

impl std::str::FromStr for TaskState {
    type Err = CinderCoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(TaskState::Queued),
            "running" => Ok(TaskState::Running),
            "waiting_child_plan" => Ok(TaskState::WaitingChildPlan),
            "succeeded" => Ok(TaskState::Succeeded),
            "failed" => Ok(TaskState::Failed),
            "cancelled" => Ok(TaskState::Cancelled),
            other => Err(CinderCoreError::InvalidEnum {
                ty: "TaskState",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanRunStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl PlanRunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for PlanRunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            PlanRunStatus::Running => "running",
            PlanRunStatus::Completed => "completed",
            PlanRunStatus::Failed => "failed",
            PlanRunStatus::Cancelled => "cancelled",
        };
        f.write_str(value)
    }
}

impl std::str::FromStr for PlanRunStatus {
    type Err = CinderCoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "running" => Ok(PlanRunStatus::Running),
            "completed" => Ok(PlanRunStatus::Completed),
            "failed" => Ok(PlanRunStatus::Failed),
            "cancelled" => Ok(PlanRunStatus::Cancelled),
            other => Err(CinderCoreError::InvalidEnum {
                ty: "PlanRunStatus",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChildPlanLink {
    pub parent_plan_run_id: Uuid,
    pub parent_task_id: String,
    pub child_plan_run_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskPlanRun {
    pub id: Uuid,
    pub plan: TaskPlan,
    pub status: PlanRunStatus,
    pub result: Option<Value>,
    pub last_error: Option<String>,
    pub parent_plan_run_id: Option<Uuid>,
    pub parent_task_id: Option<String>,
    pub user_id: Option<String>,
    pub target_id: Option<String>,
    pub locked_at: Option<DateTime<Utc>>,
    pub locked_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskRun {
    pub plan_run_id: Uuid,
    pub task_id: String,
    pub agent_run_id: Option<Uuid>,
    pub child_plan_run_id: Option<Uuid>,
    pub child_tool_call_id: Option<String>,
    pub state: TaskState,
    pub input: Value,
    pub output: Option<Value>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub fn validate_plan(plan: &TaskPlan) -> Result<(), CinderCoreError> {
    validate_non_empty(&plan.id, "plan.id")?;
    validate_non_empty(&plan.root_task_id, "plan.root_task_id")?;
    if plan.tasks.is_empty() {
        return taskplan_err("task plan must contain at least one task");
    }

    let mut task_ids = BTreeSet::new();
    for task in &plan.tasks {
        validate_non_empty(&task.id, "task.id")?;
        validate_non_empty(&task.agent_id, "task.agent_id")?;
        validate_non_empty(&task.prompt, "task.prompt")?;
        if !task_ids.insert(task.id.clone()) {
            return taskplan_err(format!("duplicate task id `{}`", task.id));
        }
        validate_json_schema(&task.output_schema).map_err(|error| {
            CinderCoreError::TaskPlan(format!(
                "task `{}` output_schema is invalid: {error}",
                task.id
            ))
        })?;
    }

    if !task_ids.contains(&plan.root_task_id) {
        return taskplan_err(format!(
            "root_task_id `{}` does not reference a task",
            plan.root_task_id
        ));
    }

    for dependency in &plan.dependencies {
        validate_non_empty(&dependency.from_task, "dependency.from_task")?;
        validate_non_empty(&dependency.to_task, "dependency.to_task")?;
        if dependency.from_task == dependency.to_task {
            return taskplan_err(format!(
                "task `{}` cannot depend on itself",
                dependency.from_task
            ));
        }
        if !task_ids.contains(&dependency.from_task) {
            return taskplan_err(format!(
                "dependency from_task `{}` does not reference a task",
                dependency.from_task
            ));
        }
        if !task_ids.contains(&dependency.to_task) {
            return taskplan_err(format!(
                "dependency to_task `{}` does not reference a task",
                dependency.to_task
            ));
        }
        for binding in &dependency.bindings {
            validate_json_pointer(&binding.from_pointer, "binding.from_pointer")?;
            validate_json_pointer(&binding.to_pointer, "binding.to_pointer")?;
        }
    }

    validate_acyclic(plan, &task_ids)
}

pub fn ready_task_ids(
    plan: &TaskPlan,
    states: &BTreeMap<String, TaskState>,
) -> Result<Vec<String>, CinderCoreError> {
    validate_plan(plan)?;
    let mut ready = Vec::new();
    for task in &plan.tasks {
        let state = states.get(&task.id).copied().unwrap_or(TaskState::Queued);
        if state != TaskState::Queued {
            continue;
        }
        let dependencies_succeeded = plan
            .dependencies
            .iter()
            .filter(|dependency| dependency.to_task == task.id)
            .all(|dependency| {
                states.get(&dependency.from_task).copied() == Some(TaskState::Succeeded)
            });
        if dependencies_succeeded {
            ready.push(task.id.clone());
        }
    }
    Ok(ready)
}

pub fn apply_output_bindings(
    plan: &TaskPlan,
    task_id: &str,
    base_input: &Value,
    outputs: &BTreeMap<String, Value>,
) -> Result<Value, CinderCoreError> {
    validate_plan(plan)?;
    if !plan.tasks.iter().any(|task| task.id == task_id) {
        return taskplan_err(format!(
            "task `{task_id}` does not exist in plan `{}`",
            plan.id
        ));
    }

    let mut input = base_input.clone();
    for dependency in plan
        .dependencies
        .iter()
        .filter(|dependency| dependency.to_task == task_id)
    {
        let output = outputs.get(&dependency.from_task).ok_or_else(|| {
            CinderCoreError::TaskPlan(format!(
                "missing output for dependency `{}`",
                dependency.from_task
            ))
        })?;
        for binding in &dependency.bindings {
            let value = output.pointer(&binding.from_pointer).ok_or_else(|| {
                CinderCoreError::TaskPlan(format!(
                    "output pointer `{}` not found in task `{}` output",
                    binding.from_pointer, dependency.from_task
                ))
            })?;
            set_json_pointer(&mut input, &binding.to_pointer, value.clone())?;
        }
    }
    Ok(input)
}

pub fn validate_task_output(task: &TaskSpec, output: &Value) -> Result<(), CinderCoreError> {
    let compiled = JSONSchema::compile(&task.output_schema)
        .map_err(|error| CinderCoreError::TaskPlan(error.to_string()))?;
    let validation = match compiled.validate(output) {
        Ok(()) => Ok(()),
        Err(errors) => {
            let messages = errors
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            taskplan_err(messages)
        }
    };
    validation
}

fn validate_acyclic(plan: &TaskPlan, task_ids: &BTreeSet<String>) -> Result<(), CinderCoreError> {
    let mut outgoing = task_ids
        .iter()
        .map(|id| (id.clone(), Vec::<String>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut indegree = task_ids
        .iter()
        .map(|id| (id.clone(), 0usize))
        .collect::<BTreeMap<_, _>>();

    for dependency in &plan.dependencies {
        outgoing
            .get_mut(&dependency.from_task)
            .ok_or_else(|| {
                CinderCoreError::TaskPlan("dependency references missing task".to_owned())
            })?
            .push(dependency.to_task.clone());
        *indegree.get_mut(&dependency.to_task).ok_or_else(|| {
            CinderCoreError::TaskPlan("dependency references missing task".to_owned())
        })? += 1;
    }

    let mut queue = indegree
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(id.clone()))
        .collect::<VecDeque<_>>();
    let mut visited = 0usize;

    while let Some(task_id) = queue.pop_front() {
        visited += 1;
        for to_task in outgoing.get(&task_id).into_iter().flatten() {
            let count = indegree.get_mut(to_task).ok_or_else(|| {
                CinderCoreError::TaskPlan("dependency references missing task".to_owned())
            })?;
            *count -= 1;
            if *count == 0 {
                queue.push_back(to_task.clone());
            }
        }
    }

    if visited == task_ids.len() {
        Ok(())
    } else {
        taskplan_err("task plan contains a dependency cycle")
    }
}

fn validate_non_empty(value: &str, field: &str) -> Result<(), CinderCoreError> {
    if value.trim().is_empty() {
        taskplan_err(format!("{field} cannot be empty"))
    } else {
        Ok(())
    }
}

fn validate_json_schema(schema: &Value) -> Result<(), CinderCoreError> {
    JSONSchema::compile(schema)
        .map(|_| ())
        .map_err(|error| CinderCoreError::TaskPlan(error.to_string()))
}

fn validate_json_pointer(pointer: &str, field: &str) -> Result<(), CinderCoreError> {
    if pointer.is_empty() || pointer.starts_with('/') {
        Ok(())
    } else {
        taskplan_err(format!("{field} must be an RFC 6901 JSON pointer"))
    }
}

fn set_json_pointer(
    target: &mut Value,
    pointer: &str,
    value: Value,
) -> Result<(), CinderCoreError> {
    validate_json_pointer(pointer, "to_pointer")?;
    if pointer.is_empty() {
        *target = value;
        return Ok(());
    }

    let tokens = pointer
        .split('/')
        .skip(1)
        .map(unescape_pointer_token)
        .collect::<Result<Vec<_>, _>>()?;
    if tokens.is_empty() {
        *target = value;
        return Ok(());
    }

    let mut current = target;
    for token in &tokens[..tokens.len() - 1] {
        if !current.is_object() {
            *current = Value::Object(Map::new());
        }
        let object = current.as_object_mut().ok_or_else(|| {
            CinderCoreError::TaskPlan("to_pointer can only create object paths".to_owned())
        })?;
        current = object
            .entry(token.clone())
            .or_insert_with(|| Value::Object(Map::new()));
    }

    if !current.is_object() {
        *current = Value::Object(Map::new());
    }
    let object = current.as_object_mut().ok_or_else(|| {
        CinderCoreError::TaskPlan("to_pointer can only set object fields".to_owned())
    })?;
    let last = tokens
        .last()
        .ok_or_else(|| CinderCoreError::TaskPlan("to_pointer cannot be empty here".to_owned()))?;
    object.insert(last.clone(), value);
    Ok(())
}

fn unescape_pointer_token(token: &str) -> Result<String, CinderCoreError> {
    let mut output = String::new();
    let mut chars = token.chars();
    while let Some(ch) = chars.next() {
        if ch != '~' {
            output.push(ch);
            continue;
        }
        match chars.next() {
            Some('0') => output.push('~'),
            Some('1') => output.push('/'),
            Some(other) => return taskplan_err(format!("invalid JSON pointer escape `~{other}`")),
            None => return taskplan_err("invalid trailing JSON pointer escape"),
        }
    }
    Ok(output)
}

fn taskplan_err<T>(message: impl Into<String>) -> Result<T, CinderCoreError> {
    Err(CinderCoreError::TaskPlan(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_plan() -> TaskPlan {
        TaskPlan {
            id: "plan_1".to_owned(),
            root_task_id: "synthesis".to_owned(),
            tasks: vec![
                TaskSpec {
                    id: "research".to_owned(),
                    agent_id: "research_agent".to_owned(),
                    prompt: "Research competitors".to_owned(),
                    input: json!({ "topic": "devtools" }),
                    output_schema: json!({
                        "type": "object",
                        "required": ["findings"],
                        "properties": {
                            "findings": { "type": "array" }
                        }
                    }),
                },
                TaskSpec {
                    id: "synthesis".to_owned(),
                    agent_id: "strategy_agent".to_owned(),
                    prompt: "Synthesize findings".to_owned(),
                    input: json!({}),
                    output_schema: json!({ "type": "object" }),
                },
            ],
            dependencies: vec![TaskDependency {
                from_task: "research".to_owned(),
                to_task: "synthesis".to_owned(),
                bindings: vec![OutputBinding {
                    from_pointer: "/findings".to_owned(),
                    to_pointer: "/research_findings".to_owned(),
                }],
            }],
        }
    }

    #[test]
    fn validates_sample_plan() {
        validate_plan(&sample_plan()).unwrap();
    }

    #[test]
    fn rejects_dependency_cycle() {
        let mut plan = sample_plan();
        plan.dependencies.push(TaskDependency {
            from_task: "synthesis".to_owned(),
            to_task: "research".to_owned(),
            bindings: Vec::new(),
        });

        let error = validate_plan(&plan).unwrap_err().to_string();
        assert!(error.contains("cycle"));
    }

    #[test]
    fn computes_ready_tasks_from_succeeded_dependencies() {
        let plan = sample_plan();
        let mut states = BTreeMap::new();
        assert_eq!(ready_task_ids(&plan, &states).unwrap(), vec!["research"]);

        states.insert("research".to_owned(), TaskState::Succeeded);
        assert_eq!(ready_task_ids(&plan, &states).unwrap(), vec!["synthesis"]);
    }

    #[test]
    fn applies_output_bindings() {
        let plan = sample_plan();
        let mut outputs = BTreeMap::new();
        outputs.insert(
            "research".to_owned(),
            json!({ "findings": [{ "name": "A" }] }),
        );

        let input = apply_output_bindings(&plan, "synthesis", &json!({}), &outputs).unwrap();
        assert_eq!(input, json!({ "research_findings": [{ "name": "A" }] }));
    }

    #[test]
    fn validates_task_output_against_schema() {
        let plan = sample_plan();
        let task = &plan.tasks[0];
        validate_task_output(task, &json!({ "findings": [] })).unwrap();
        assert!(validate_task_output(task, &json!({ "summary": "missing" })).is_err());
    }

    #[test]
    fn unescapes_json_pointer_tokens() {
        let mut output = json!({});
        set_json_pointer(&mut output, "/a~1b/c~0d", json!(42)).unwrap();
        assert_eq!(output, json!({ "a/b": { "c~d": 42 } }));
    }
}
