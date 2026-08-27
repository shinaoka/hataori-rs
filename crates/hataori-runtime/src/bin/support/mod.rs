use hataori_runtime::{Action, ActionError, RuntimeBuilder, Segments, WireValue};

pub struct Increment(pub u64);

impl WireValue for Increment {
    const SCHEMA_ID: u64 = 1000;

    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }

    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(segments)?))
    }
}

impl Action for Increment {
    const ID: u128 = 1000;
    type Output = u64;
}

pub fn register(builder: &mut RuntimeBuilder) -> Result<(), Box<dyn std::error::Error>> {
    builder.register::<Increment, _>(|value| Ok(value.0 + 1))?;
    Ok(())
}
