// Minimal notifications API (sync stubs for now). Network senders can be added later.

#[derive(Debug, Clone, Default)]
pub struct NotifyMessage {
    pub title: String,
    pub body: String,
    pub link: Option<String>,
    pub run_id: Option<i64>,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TelegramConfig {
    pub bot_token: String,
    pub chat_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct EmailConfig {
    pub smtp_url: String,
    pub from: String,
    pub to: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct WhatsAppConfig {
    pub api_url: String,
    pub token: String,
    pub to: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum Provider {
    Telegram(TelegramConfig),
    Email(EmailConfig),
    WhatsApp(WhatsAppConfig),
}

impl Provider {
    pub fn name(&self) -> &'static str {
        match self {
            Provider::Telegram(_) => "telegram",
            Provider::Email(_) => "email",
            Provider::WhatsApp(_) => "whatsapp",
        }
    }

    pub fn send(&self, msg: &NotifyMessage) -> Result<(), String> {
        // For now, these are stubs that simply format and log the message.
        let formatted = format_message(msg);
        match self {
            Provider::Telegram(cfg) => {
                // TODO: real HTTP send to Telegram Bot API
                println!("[notify:telegram] chat_id={} title={} body={} link={:?}", cfg.chat_id, msg.title, msg.body, msg.link);
                Ok(())
            }
            Provider::Email(cfg) => {
                // TODO: real SMTP or API send
                println!("[notify:email] to={:?} from={} title={} body={} link={:?}", cfg.to, cfg.from, msg.title, msg.body, msg.link);
                // Simulate content usage
                if formatted.is_empty() { return Err("empty message".into()); }
                Ok(())
            }
            Provider::WhatsApp(cfg) => {
                // TODO: real WhatsApp provider API send
                println!("[notify:whatsapp] to={:?} title={} body={} link={:?} api_url={}", cfg.to, msg.title, msg.body, msg.link, cfg.api_url);
                Ok(())
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct NotifierHub {
    pub providers: Vec<Provider>,
    pub max_retries: usize,
}

impl NotifierHub {
    pub fn new() -> Self { Self { providers: Vec::new(), max_retries: 3 } }

    pub fn with_provider(mut self, p: Provider) -> Self { self.providers.push(p); self }

    pub fn send_all(&self, msg: &NotifyMessage) {
        for p in &self.providers {
            let mut attempts = 0;
            loop {
                attempts += 1;
                match p.send(msg) {
                    Ok(()) => break,
                    Err(e) => {
                        if attempts >= self.max_retries { println!("[notify:{}] failed after {} attempts: {}", p.name(), attempts, e); break; }
                        // simple linear backoff placeholder
                        println!("[notify:{}] retrying (attempt {}): {}", p.name(), attempts, e);
                    }
                }
            }
        }
    }
}

fn format_message(msg: &NotifyMessage) -> String {
    let mut s = String::new();
    s.push_str(&msg.title);
    s.push_str("\n\n");
    s.push_str(&msg.body);
    if let Some(link) = &msg.link { s.push_str("\n\n"); s.push_str(link); }
    if let Some(rid) = msg.run_id { s.push_str(&format!("\n(run #{})", rid)); }
    if !msg.tags.is_empty() { s.push_str("\n"); s.push_str(&msg.tags.iter().map(|t| format!("#{}", t)).collect::<Vec<_>>().join(" ")); }
    s
}

// Backwards-compatible no-op stub
pub fn notify_stub(_msg: &str) {}
