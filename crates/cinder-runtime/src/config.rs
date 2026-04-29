use async_trait::async_trait;
use cinder_core::{
    CinderConfig, CinderCoreError, Message, MessageRole, ModelConfig,
    OpenAiCompatibleProviderConfig, Provider, ProviderRequest, ProviderResponse, ToolCall,
    ToolSpec,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone)]
pub struct OpenAiCompatibleProvider {
    id: String,
    client: Client,
    base_url: String,
    api_key: String,
    model_options: HashMap<String, ModelOptions>,
}

#[derive(Debug, Clone, Copy)]
struct ModelOptions {
    temperature: Option<f32>,
    max_output_tokens: Option<u32>,
}

impl OpenAiCompatibleProvider {
    pub fn from_config(
        id: String,
        provider: OpenAiCompatibleProviderConfig,
        config: &CinderConfig,
    ) -> Self {
        let model_options = config
            .models
            .values()
            .filter(|model| model.provider == id)
            .map(|model| (model.model.clone(), model_options(model)))
            .collect();
        Self {
            id,
            client: Client::new(),
            base_url: provider.base_url.trim_end_matches('/').to_owned(),
            api_key: provider.api_key,
            model_options,
        }
    }
}

#[async_trait]
impl Provider for OpenAiCompatibleProvider {
    fn name(&self) -> &str {
        &self.id
    }

    async fn chat(&self, request: ProviderRequest) -> Result<ProviderResponse, CinderCoreError> {
        let options = self.model_options.get(&request.model).copied();
        let tool_name_map = request
            .tools
            .iter()
            .map(|tool| (provider_tool_name(&tool.name), tool.name.clone()))
            .collect::<HashMap<_, _>>();
        let body = OpenAiChatRequest {
            model: request.model,
            messages: request
                .messages
                .iter()
                .map(|message| openai_message(message, &tool_name_map))
                .collect(),
            tools: if request.tools.is_empty() {
                None
            } else {
                Some(request.tools.iter().map(openai_tool_spec).collect())
            },
            temperature: options.and_then(|options| options.temperature),
            max_tokens: options.and_then(|options| options.max_output_tokens),
        };

        let response = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|error| CinderCoreError::Provider(error.to_string()))?;

        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|error| CinderCoreError::Provider(error.to_string()))?;
        if !status.is_success() {
            return Err(CinderCoreError::Provider(format!(
                "provider `{}` returned {status}: {text}",
                self.id
            )));
        }

        let decoded: OpenAiChatResponse = serde_json::from_str(&text)
            .map_err(|error| CinderCoreError::Provider(error.to_string()))?;
        let choice = decoded.choices.into_iter().next().ok_or_else(|| {
            CinderCoreError::Provider(format!("provider `{}` returned no choices", self.id))
        })?;
        let provider_metadata = match choice.message.reasoning_content {
            Some(reasoning_content) if !reasoning_content.is_empty() => {
                serde_json::json!({ "reasoning_content": reasoning_content })
            }
            _ => Value::Null,
        };
        Ok(ProviderResponse {
            message: Message::assistant_with_metadata(
                choice.message.content.unwrap_or_default(),
                choice
                    .message
                    .tool_calls
                    .unwrap_or_default()
                    .into_iter()
                    .map(|tool_call| tool_call_from_openai(tool_call, &tool_name_map))
                    .collect::<Result<Vec<_>, _>>()?,
                provider_metadata,
            ),
        })
    }
}

fn model_options(model: &ModelConfig) -> ModelOptions {
    ModelOptions {
        temperature: model.temperature,
        max_output_tokens: model.max_output_tokens.or(model.max_tokens),
    }
}

fn openai_message(message: &Message, tool_name_map: &HashMap<String, String>) -> OpenAiMessage {
    OpenAiMessage {
        role: match message.role {
            MessageRole::System => "system",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
        }
        .to_owned(),
        content: if message.content.is_empty() {
            None
        } else {
            Some(message.content.clone())
        },
        reasoning_content: message
            .provider_metadata
            .get("reasoning_content")
            .and_then(Value::as_str)
            .map(str::to_owned),
        tool_call_id: message.tool_call_id.clone(),
        tool_calls: if message.tool_calls.is_empty() {
            None
        } else {
            Some(
                message
                    .tool_calls
                    .iter()
                    .map(|tool_call| openai_tool_call(tool_call, tool_name_map))
                    .collect(),
            )
        },
    }
}

fn openai_tool_spec(tool: &ToolSpec) -> OpenAiToolSpec {
    OpenAiToolSpec {
        ty: "function".to_owned(),
        function: OpenAiFunctionSpec {
            name: provider_tool_name(&tool.name),
            description: tool.description.clone(),
            parameters: tool.input_schema.clone(),
        },
    }
}

fn openai_tool_call(
    tool_call: &ToolCall,
    tool_name_map: &HashMap<String, String>,
) -> OpenAiToolCall {
    let name = tool_name_map
        .iter()
        .find_map(|(provider_name, internal_name)| {
            (internal_name == &tool_call.name).then_some(provider_name.clone())
        })
        .unwrap_or_else(|| provider_tool_name(&tool_call.name));
    OpenAiToolCall {
        id: tool_call.id.clone(),
        ty: "function".to_owned(),
        function: OpenAiFunctionCall {
            name,
            arguments: tool_call.arguments.to_string(),
        },
    }
}

fn tool_call_from_openai(
    tool_call: OpenAiToolCall,
    tool_name_map: &HashMap<String, String>,
) -> Result<ToolCall, CinderCoreError> {
    let arguments = if tool_call.function.arguments.trim().is_empty() {
        Value::Object(Default::default())
    } else {
        serde_json::from_str(&tool_call.function.arguments)
            .map_err(|error| CinderCoreError::Provider(error.to_string()))?
    };
    let name = tool_name_map
        .get(&tool_call.function.name)
        .cloned()
        .unwrap_or(tool_call.function.name);
    Ok(ToolCall {
        id: tool_call.id,
        name,
        arguments,
    })
}

fn provider_tool_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[derive(Debug, Serialize)]
struct OpenAiChatRequest {
    model: String,
    messages: Vec<OpenAiMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<OpenAiToolSpec>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OpenAiMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OpenAiToolCall>>,
}

#[derive(Debug, Serialize)]
struct OpenAiToolSpec {
    #[serde(rename = "type")]
    ty: String,
    function: OpenAiFunctionSpec,
}

#[derive(Debug, Serialize)]
struct OpenAiFunctionSpec {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct OpenAiToolCall {
    id: String,
    #[serde(rename = "type")]
    ty: String,
    function: OpenAiFunctionCall,
}

#[derive(Debug, Serialize, Deserialize)]
struct OpenAiFunctionCall {
    name: String,
    arguments: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiChatResponse {
    choices: Vec<OpenAiChoice>,
}

#[derive(Debug, Deserialize)]
struct OpenAiChoice {
    message: OpenAiResponseMessage,
}

#[derive(Debug, Deserialize)]
struct OpenAiResponseMessage {
    content: Option<String>,
    #[serde(default)]
    reasoning_content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<OpenAiToolCall>>,
}
