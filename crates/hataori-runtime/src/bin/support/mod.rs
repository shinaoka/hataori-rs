use hataori_runtime::{
    Action, ActionError, DistributedObject, ObjectWriteAction, RuntimeBuilder, Segments, WireValue,
};

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

pub struct Counter(pub u64);

impl WireValue for Counter {
    const SCHEMA_ID: u64 = 1001;
    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(segments)?))
    }
}

impl DistributedObject for Counter {
    const TYPE_ID: u128 = 1001;
}

pub struct CounterAdd(pub u64);

impl WireValue for CounterAdd {
    const SCHEMA_ID: u64 = 1002;
    fn encode(self) -> Result<Segments, ActionError> {
        self.0.encode()
    }
    fn decode(segments: Segments) -> Result<Self, ActionError> {
        Ok(Self(u64::decode(segments)?))
    }
}

impl ObjectWriteAction<Counter> for CounterAdd {
    const ID: u128 = 1002;
    type Output = u64;
    fn execute(self, state: &mut Counter) -> Result<u64, ActionError> {
        state.0 += self.0;
        Ok(state.0)
    }
}

pub fn register(builder: &mut RuntimeBuilder) -> Result<(), Box<dyn std::error::Error>> {
    builder.register::<Increment, _>(|value| Ok(value.0 + 1))?;
    builder.register_object_exclusive::<Counter>()?;
    builder.register_object_write::<Counter, CounterAdd>()?;
    Ok(())
}
