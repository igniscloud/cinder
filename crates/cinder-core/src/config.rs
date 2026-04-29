use crate::{AgentSpec, CinderCoreError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CinderConfig {
    pub store: StoreConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
    #[serde(default)]
    pub models: BTreeMap<String, ModelConfig>,
    #[serde(default)]
    pub agents: BTreeMap<String, AgentConfig>,
}

impl CinderConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, CinderCoreError> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .map_err(|error| CinderCoreError::Config(format!("read config: {error}")))?;
        let config = Self::from_toml_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    pub fn from_toml_str(content: &str) -> Result<Self, CinderCoreError> {
        toml::from_str(content)
            .map_err(|error| CinderCoreError::Config(format!("parse config: {error}")))
    }

    pub fn validate(&self) -> Result<(), CinderCoreError> {
        self.store.validate()?;
        for (provider_id, provider) in &self.providers {
            validate_key(provider_id, "provider id")?;
            provider.validate(provider_id)?;
        }

        let mut aliases = BTreeSet::new();
        for (model_id, model) in &self.models {
            validate_key(model_id, "model id")?;
            validate_non_empty(&model.alias, "model.alias")?;
            if !aliases.insert(model.alias.clone()) {
                return config_err(format!("duplicate model alias `{}`", model.alias));
            }
            validate_non_empty(&model.provider, "model.provider")?;
            if !self.providers.contains_key(&model.provider) {
                return config_err(format!(
                    "model `{model_id}` references unknown provider `{}`",
                    model.provider
                ));
            }
            validate_non_empty(&model.model, "model.model")?;
        }

        for (agent_id, agent) in &self.agents {
            validate_key(agent_id, "agent id")?;
            validate_non_empty(&agent.description, "agent.description")?;
            validate_non_empty(&agent.model, "agent.model")?;
            if self.model_by_alias(&agent.model).is_none() {
                return config_err(format!(
                    "agent `{agent_id}` references unknown model alias `{}`",
                    agent.model
                ));
            }
            match (&agent.system_prompt, &agent.system_prompt_file) {
                (Some(_), Some(_)) => {
                    return config_err(format!(
                        "agent `{agent_id}` must use either system_prompt or system_prompt_file, not both"
                    ));
                }
                (None, None) => {
                    return config_err(format!(
                        "agent `{agent_id}` must define system_prompt or system_prompt_file"
                    ));
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub fn model_by_alias(&self, alias: &str) -> Option<&ModelConfig> {
        self.models.values().find(|model| model.alias == alias)
    }

    pub fn agent_specs(
        &self,
        base_dir: impl AsRef<Path>,
    ) -> Result<Vec<AgentSpec>, CinderCoreError> {
        self.validate()?;
        let base_dir = base_dir.as_ref();
        self.agents
            .iter()
            .map(|(agent_id, agent)| {
                let model = self.model_by_alias(&agent.model).ok_or_else(|| {
                    CinderCoreError::Config(format!(
                        "agent `{agent_id}` references unknown model alias `{}`",
                        agent.model
                    ))
                })?;
                Ok(AgentSpec {
                    id: agent_id.clone(),
                    provider: model.provider.clone(),
                    model: model.model.clone(),
                    description: agent.description.clone(),
                    system_prompt: agent.resolve_system_prompt(base_dir)?,
                    tools: agent.tools.clone(),
                    skills: agent.skills.clone(),
                })
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoreConfig {
    pub kind: StoreKind,
    #[serde(default)]
    pub sqlite: Option<SqliteStoreConfig>,
    #[serde(default)]
    pub postgres: Option<PostgresStoreConfig>,
}

impl StoreConfig {
    pub fn validate(&self) -> Result<(), CinderCoreError> {
        match self.kind {
            StoreKind::Sqlite => {
                let sqlite = self.sqlite.as_ref().ok_or_else(|| {
                    CinderCoreError::Config("store.sqlite is required".to_owned())
                })?;
                validate_non_empty(&sqlite.path, "store.sqlite.path")?;
            }
            StoreKind::Postgres => {
                let postgres = self.postgres.as_ref().ok_or_else(|| {
                    CinderCoreError::Config("store.postgres is required".to_owned())
                })?;
                validate_non_empty(&postgres.database_url, "store.postgres.database_url")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StoreKind {
    Sqlite,
    Postgres,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SqliteStoreConfig {
    pub path: String,
    #[serde(default = "default_true")]
    pub create_parent_dirs: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresStoreConfig {
    pub database_url: String,
    #[serde(default = "default_postgres_max_connections")]
    pub max_connections: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeConfig {
    #[serde(default = "default_run_lease_seconds")]
    pub run_lease_seconds: i64,
    #[serde(default = "default_plan_lease_seconds")]
    pub plan_lease_seconds: i64,
    #[serde(default = "default_max_internal_turns")]
    pub max_internal_turns: usize,
    #[serde(default = "default_max_plan_depth")]
    pub max_plan_depth: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            run_lease_seconds: default_run_lease_seconds(),
            plan_lease_seconds: default_plan_lease_seconds(),
            max_internal_turns: default_max_internal_turns(),
            max_plan_depth: default_max_plan_depth(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderConfig {
    OpenAiCompatible(OpenAiCompatibleProviderConfig),
}

impl ProviderConfig {
    pub fn validate(&self, provider_id: &str) -> Result<(), CinderCoreError> {
        match self {
            ProviderConfig::OpenAiCompatible(config) => {
                validate_non_empty(&config.base_url, "provider.base_url")?;
                validate_non_empty(&config.api_key, "provider.api_key")?;
                if config.base_url.trim_end_matches('/').is_empty() {
                    return config_err(format!("provider `{provider_id}` has empty base_url"));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    OpenAiCompatible,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpenAiCompatibleProviderConfig {
    pub base_url: String,
    pub api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelConfig {
    pub alias: String,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentConfig {
    pub model: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt_file: Option<PathBuf>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub skills: Vec<String>,
}

impl AgentConfig {
    pub fn resolve_system_prompt(&self, base_dir: &Path) -> Result<String, CinderCoreError> {
        if let Some(prompt) = &self.system_prompt {
            return Ok(prompt.clone());
        }
        let file = self.system_prompt_file.as_ref().ok_or_else(|| {
            CinderCoreError::Config("agent missing system_prompt_file".to_owned())
        })?;
        let path = if file.is_absolute() {
            file.clone()
        } else {
            base_dir.join(file)
        };
        fs::read_to_string(&path).map_err(|error| {
            CinderCoreError::Config(format!("read system prompt `{}`: {error}", path.display()))
        })
    }
}

fn validate_key(value: &str, field: &str) -> Result<(), CinderCoreError> {
    validate_non_empty(value, field)?;
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.')
    {
        Ok(())
    } else {
        config_err(format!(
            "{field} `{value}` may only contain ASCII letters, numbers, `_`, `-`, or `.`"
        ))
    }
}

fn validate_non_empty(value: &str, field: &str) -> Result<(), CinderCoreError> {
    if value.trim().is_empty() {
        config_err(format!("{field} cannot be empty"))
    } else {
        Ok(())
    }
}

fn config_err<T>(message: impl Into<String>) -> Result<T, CinderCoreError> {
    Err(CinderCoreError::Config(message.into()))
}

fn default_true() -> bool {
    true
}

fn default_postgres_max_connections() -> u32 {
    8
}

fn default_run_lease_seconds() -> i64 {
    300
}

fn default_plan_lease_seconds() -> i64 {
    300
}

fn default_max_internal_turns() -> usize {
    12
}

fn default_max_plan_depth() -> usize {
    8
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> CinderConfig {
        CinderConfig::from_toml_str(
            r#"
            [store]
            kind = "sqlite"

            [store.sqlite]
            path = "./cinder.db"

            [providers.hpcai]
            kind = "open_ai_compatible"
            base_url = "https://api.hpc-ai.com/inference/v1"
            api_key = "test"

            [models.kimi25]
            alias = "kimi25"
            provider = "hpcai"
            model = "moonshotai/kimi-k2.5"

            [agents.orchestrator]
            model = "kimi25"
            description = "Coordinates work."
            system_prompt = "You coordinate work."
            "#,
        )
        .unwrap()
    }

    #[test]
    fn validates_config() {
        valid_config().validate().unwrap();
    }

    #[test]
    fn rejects_duplicate_model_aliases() {
        let mut config = valid_config();
        config.models.insert(
            "other".to_owned(),
            ModelConfig {
                alias: "kimi25".to_owned(),
                provider: "hpcai".to_owned(),
                model: "other".to_owned(),
                temperature: None,
                max_tokens: None,
            },
        );

        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("duplicate model alias"));
    }

    #[test]
    fn builds_agent_specs_from_aliases() {
        let specs = valid_config().agent_specs(".").unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].id, "orchestrator");
        assert_eq!(specs[0].provider, "hpcai");
        assert_eq!(specs[0].model, "moonshotai/kimi-k2.5");
        assert_eq!(specs[0].description, "Coordinates work.");
    }
}
