#![forbid(unsafe_code)]

use aurum_types::CommandKind;

#[derive(Debug, Clone)]
pub struct AdapterFrame<'a> {
    pub bytes: &'a [u8],
}

#[derive(Debug, Clone, Default)]
pub struct CommandBatch {
    pub kinds: Vec<CommandKind>,
}

pub trait ProtocolAdapter {
    fn name(&self) -> &'static str;
    fn translate(&mut self, frame: AdapterFrame<'_>, out: &mut CommandBatch) -> Result<(), AdapterError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterError {
    MalformedFrame,
    UnsupportedOperation,
}
