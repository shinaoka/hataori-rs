use crate::error::{ActionError, RuntimeError};
use hataori_runtime_foundation::protocol::ActionId;
use std::{
    collections::BTreeMap,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

pub type Segments = Vec<Vec<u8>>;

pub trait WireValue: Sized + Send + 'static {
    const SCHEMA_ID: u64;

    fn encode(self) -> Result<Segments, ActionError>;
    fn decode(segments: Segments) -> Result<Self, ActionError>;
}

pub trait Action: WireValue {
    const ID: u128;
    type Output: WireValue;
}

type Handler = dyn Fn(Segments) -> Result<Segments, ActionError> + Send + Sync;

#[derive(Clone)]
pub(crate) struct RegisteredAction {
    handler: Arc<Handler>,
}

impl std::fmt::Debug for RegisteredAction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RegisteredAction { .. }")
    }
}

impl RegisteredAction {
    pub(crate) fn new(
        handler: impl Fn(Segments) -> Result<Segments, ActionError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }

    pub(crate) fn execute(&self, input: Segments) -> Result<Segments, ActionError> {
        match catch_unwind(AssertUnwindSafe(|| (self.handler)(input))) {
            Ok(result) => result,
            Err(_) => Err(ActionError::Panic),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ActionRegistry {
    entries: BTreeMap<ActionId, RegisteredAction>,
    schemas: BTreeMap<ActionId, (u64, u64)>,
}

impl ActionRegistry {
    pub(crate) fn register<A, F>(&mut self, handler: F) -> Result<(), RuntimeError>
    where
        A: Action,
        F: Fn(A) -> Result<A::Output, ActionError> + Send + Sync + 'static,
    {
        let id = ActionId::new(A::ID).map_err(|_| RuntimeError::InvalidActionId)?;
        if A::SCHEMA_ID == 0 || A::Output::SCHEMA_ID == 0 {
            return Err(RuntimeError::InvalidSchema);
        }
        if self.entries.contains_key(&id) {
            return Err(RuntimeError::DuplicateAction(id));
        }
        self.entries.insert(
            id,
            RegisteredAction {
                handler: Arc::new(move |segments| {
                    let input = A::decode(segments)?;
                    handler(input)?.encode()
                }),
            },
        );
        self.schemas
            .insert(id, (A::SCHEMA_ID, A::Output::SCHEMA_ID));
        Ok(())
    }

    pub(crate) fn insert_erased(
        &mut self,
        id: ActionId,
        input_schema: u64,
        output_schema: u64,
        handler: RegisteredAction,
    ) -> Result<(), RuntimeError> {
        if input_schema == 0 || output_schema == 0 {
            return Err(RuntimeError::InvalidSchema);
        }
        if self.entries.insert(id, handler).is_some() {
            return Err(RuntimeError::DuplicateAction(id));
        }
        self.schemas.insert(id, (input_schema, output_schema));
        Ok(())
    }

    pub(crate) fn get(&self, id: ActionId) -> Option<&RegisteredAction> {
        self.entries.get(&id)
    }

    pub(crate) fn fingerprint(&self) -> [u8; 32] {
        const SEEDS: [u64; 4] = [
            0xcbf2_9ce4_8422_2325,
            0x8422_2325_cbf2_9ce4,
            0x9e37_79b9_7f4a_7c15,
            0xd6e8_feb8_6659_fd93,
        ];
        let mut hashes = SEEDS;
        for (id, (input, output)) in &self.schemas {
            for byte in id
                .get()
                .to_le_bytes()
                .into_iter()
                .chain(input.to_le_bytes())
                .chain(output.to_le_bytes())
            {
                for (index, hash) in hashes.iter_mut().enumerate() {
                    *hash ^= u64::from(byte).wrapping_add(index as u64);
                    *hash = hash.wrapping_mul(0x100_0000_01b3);
                }
            }
        }
        let mut result = [0_u8; 32];
        for (index, hash) in hashes.into_iter().enumerate() {
            result[index * 8..index * 8 + 8].copy_from_slice(&hash.to_le_bytes());
        }
        result
    }
}

impl WireValue for () {
    const SCHEMA_ID: u64 = 1;

    fn encode(self) -> Result<Segments, ActionError> {
        Ok(Vec::new())
    }

    fn decode(segments: Segments) -> Result<Self, ActionError> {
        if segments.is_empty() {
            Ok(())
        } else {
            Err(ActionError::codec("unit value has payload"))
        }
    }
}

impl WireValue for u64 {
    const SCHEMA_ID: u64 = 2;

    fn encode(self) -> Result<Segments, ActionError> {
        Ok(vec![self.to_le_bytes().to_vec()])
    }

    fn decode(segments: Segments) -> Result<Self, ActionError> {
        let bytes: [u8; 8] = one_segment(segments, "u64")?
            .try_into()
            .map_err(|_| ActionError::codec("u64 payload must contain 8 bytes"))?;
        Ok(Self::from_le_bytes(bytes))
    }
}

impl WireValue for Vec<u8> {
    const SCHEMA_ID: u64 = 3;

    fn encode(self) -> Result<Segments, ActionError> {
        Ok(vec![self])
    }

    fn decode(segments: Segments) -> Result<Self, ActionError> {
        one_segment(segments, "byte vector")
    }
}

impl WireValue for String {
    const SCHEMA_ID: u64 = 4;

    fn encode(self) -> Result<Segments, ActionError> {
        Ok(vec![self.into_bytes()])
    }

    fn decode(segments: Segments) -> Result<Self, ActionError> {
        String::from_utf8(one_segment(segments, "string")?)
            .map_err(|_| ActionError::codec("string payload is not UTF-8"))
    }
}

fn one_segment(mut segments: Segments, name: &'static str) -> Result<Vec<u8>, ActionError> {
    if segments.len() != 1 {
        return Err(ActionError::codec(format!(
            "{name} requires exactly one segment"
        )));
    }
    Ok(segments.pop().unwrap())
}

#[cfg(test)]
mod tests;
