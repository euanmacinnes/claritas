use anyhow::Context as _;
use claritasai_core::{Plan, PlanStep, PlanVerdict};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::debug;

#[derive(Clone)]
pub struct OllamaClient {
    http: reqwest::Client,
    pub base_url: String,
}

impl OllamaClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self { http: reqwest::Client::new(), base_url: base_url.into() }
    }

    pub async fn chat_json<T: for<'de> Deserialize<'de> + Send + 'static>(
        &self,
        model: &str,
        system: &str,
        user: &str,
    ) -> anyhow::Result<T> {
        #[derive(Serialize)]
        struct Msg<'a> { role: &'a str, content: &'a str }
        #[derive(Serialize)]
        struct Body<'a> {
            model: &'a str,
            messages: Vec<Msg<'a>>,
            format: &'static str,
            stream: bool,
        }
        #[derive(Deserialize)]
        struct Resp { message: RespMsg }
        #[derive(Deserialize)]
        struct RespMsg { content: String }

        let url = format!("{}/api/chat", self.base_url.trim_end_matches('/'));
        let body = Body { model, messages: vec![ Msg{role:"system", content: system}, Msg{role:"user", content: user} ], format: "json", stream: false };
        let resp = self.http.post(&url).json(&body).send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(anyhow::anyhow!("ollama error {}: {}", status, text));
        }
        let parsed: Resp = serde_json::from_str(&text)
            .with_context(|| format!("failed to parse ollama response as JSON: {}", text))?;
        let out: T = serde_json::from_str(&parsed.message.content)
            .with_context(|| format!("failed to decode assistant JSON: {}", parsed.message.content))?;
        Ok(out)
    }
}

#[derive(Clone)]
pub struct AgentsHarness {
    client: OllamaClient,
    model: String,
}

impl AgentsHarness {
    pub fn new(base_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self { client: OllamaClient::new(base_url), model: model.into() }
    }

    pub async fn draft_plan(&self, objective: &str) -> anyhow::Result<Plan> {
        #[derive(Deserialize)]
        struct PlanOut { objective: String, steps: Vec<StepOut> }
        #[derive(Deserialize)]
        struct StepOut { tool_ref: String, input: serde_json::Value }

        let system = "You are the DevAgent. Produce a concrete JSON execution plan that uses available tools. Respond ONLY with JSON matching {objective, steps:[{tool_ref,input}]}";
        let user = &format!(
            "Objective: {}\n\nReturn JSON only.",
            objective
        );
        let resp: PlanOut = self.client.chat_json(&self.model, system, user).await?;
        let steps = resp
            .steps
            .into_iter()
            .map(|s| PlanStep { tool_ref: s.tool_ref, input: s.input })
            .collect();
        Ok(Plan { objective: resp.objective, steps })
    }

    pub async fn review_plan(&self, plan: &Plan) -> anyhow::Result<PlanVerdict> {
        #[derive(Deserialize)]
        struct VerdictOut { status: String, rationale: String }
        let system = "You are the QAAgent. Review a plan against architectural sanity and safety. Reply strictly as JSON {status: 'Approved'|'NeedsChanges'|'Rejected', rationale: string}.";
        let user = &format!("Plan: {}", serde_json::to_string(plan)?);
        let out: VerdictOut = self.client.chat_json(&self.model, system, user).await?;
        Ok(PlanVerdict { status: out.status, rationale: out.rationale })
    }

    pub async fn manager_gate(&self, plan: &Plan, qa: &PlanVerdict) -> anyhow::Result<PlanVerdict> {
        #[derive(Deserialize)]
        struct VerdictOut { status: String, rationale: String }
        let system = "You are the ManagerAgent. Considering scope, risk, and value, decide if the plan should proceed. Reply JSON {status, rationale}.";
        let user = &format!("Plan: {}\nQA: {}", serde_json::to_string(plan)?, serde_json::to_string(qa)?);
        let out: VerdictOut = self.client.chat_json(&self.model, system, user).await?;
        Ok(PlanVerdict { status: out.status, rationale: out.rationale })
    }
}
