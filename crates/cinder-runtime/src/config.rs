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
    max_tokens: Option<u32>,
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
        let body = OpenAiChatRequest {
            model: request.model,
            messages: request.messages.iter().map(openai_message).collect(),
            tools: if request.tools.is_empty() {
                None
            } else {
                Some(request.tools.iter().map(openai_tool_spec).collect())
            },
            temperature: options.and_then(|options| options.temperature),
            max_tokens: options.and_then(|options| options.max_tokens),
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
        Ok(ProviderResponse {
            message: Message::assistant(
                choice.message.content.unwrap_or_default(),
                choice
                    .message
                    .tool_calls
                    .unwrap_or_default()
                    .into_iter()
                    .map(tool_call_from_openai)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        })
    }
}

fn model_options(model: &ModelConfig) -> ModelOptions {
    ModelOptions {
        temperature: model.temperature,
        max_tokens: model.max_tokens,
    }
}

fn openai_message(message: &Message) -> OpenAiMessage {
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
        tool_call_id: message.tool_call_id.clone(),
        tool_calls: if message.tool_calls.is_empty() {
            None
        } else {
            Some(message.tool_calls.iter().map(openai_tool_call).collect())
        },
    }
}

fn openai_tool_spec(tool: &ToolSpec) -> OpenAiToolSpec {
    OpenAiToolSpec {
        ty: "function".to_owned(),
        function: OpenAiFunctionSpec {
            name: tool.name.clone(),
            description: tool.description.clone(),
            parameters: tool.input_schema.clone(),
        },
    }
}

fn openai_tool_call(tool_call: &ToolCall) -> OpenAiToolCall {
    OpenAiToolCall {
        id: tool_call.id.clone(),
        ty: "function".to_owned(),
        function: OpenAiFunctionCall {
            name: tool_call.name.clone(),
            arguments: tool_call.arguments.to_string(),
        },
    }
}

fn tool_call_from_openai(tool_call: OpenAiToolCall) -> Result<ToolCall, CinderCoreError> {
    let arguments = if tool_call.function.arguments.trim().is_empty() {
        Value::Object(Default::default())
    } else {
        serde_json::from_str(&tool_call.function.arguments)
            .map_err(|error| CinderCoreError::Provider(error.to_string()))?
    };
    Ok(ToolCall {
        id: tool_call.id,
        name: tool_call.function.name,
        arguments,
    })
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
    tool_calls: Option<Vec<OpenAiToolCall>>,
}
