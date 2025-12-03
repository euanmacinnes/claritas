#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Dev,
    QA,
    Manager,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Drafting,
    Reviewing,
    Refining,
    Approved,
    Executing,
    Completed,
    Blocked,
}
